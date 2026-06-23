import ast
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Optional


MAX_CHANGED_FILES = 5
MAX_DEP_FILES = 12
MAX_FULL_SOURCE_CHARS = 120000
MAX_DEP_SOURCE_CHARS = 20000
FORCED_DEPENDENCIES = [
    "silver/notebooks/common/common_functions",
]
EXTERNAL_IMPORT_PREFIXES = (
    "pyspark", "delta", "databricks", "requests", "pandas", "numpy",
    "json", "os", "sys", "re", "typing", "datetime", "pathlib",
)

base_sha = os.environ.get("BASE_SHA", "")
head_sha = os.environ.get("HEAD_SHA", "")


def _git_changed() -> list[str]:
    try:
        return subprocess.check_output(
            ["git", "diff", "--name-only", "--diff-filter=ACMRT", base_sha, head_sha],
            text=True,
            stderr=subprocess.DEVNULL,
        ).splitlines()
    except Exception:
        return []


def _is_source_file(path: str) -> bool:
    if path.startswith("backend/") or path.startswith(".github/"):
        return False
    if path.endswith(".ipynb"):
        return False
    if path.endswith(".py"):
        return True
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            return fh.readline().startswith("# Databricks notebook source")
    except Exception:
        return False


def _read_text(path: str, limit: Optional[int] = None) -> str:
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        text = fh.read()
    return text[:limit] if limit else text


def _ipynb_code(path: str) -> str:
    with open(path, "r", encoding="utf-8") as fh:
        nb = json.load(fh)
    chunks: list[str] = []
    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        src = cell.get("source", [])
        if isinstance(src, list):
            src = "".join(src)
        src = src.strip()
        if src:
            chunks.append(src)
    return "\n\n# COMMAND ----------\n".join(chunks)


def _numbered_file_block(path: str, source: str, dependency_of: str = "") -> str:
    lines = [f"# FILE: {path}"]
    if dependency_of:
        lines.append(f"# DEPENDENCY_OF: {dependency_of}")
    for i, code_line in enumerate(source.splitlines(), 1):
        lines.append(f"{i}: {code_line}")
    return "\n".join(lines) + "\n\n"


def _python_imports(source: str) -> list[str]:
    cleaned: list[str] = []
    for line in source.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("%") or stripped.startswith("# MAGIC"):
            cleaned.append("")
        else:
            cleaned.append(line)
    try:
        tree = ast.parse("\n".join(cleaned))
    except SyntaxError:
        return []

    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.append("." * node.level + node.module)
    return imports


def _extract_dependencies(source: str) -> list[dict[str, str]]:
    deps: list[dict[str, str]] = []
    # Match bare `%run` and Databricks magic-cell `# MAGIC %run`
    for m in re.finditer(
        r"^\s*(?:#\s*MAGIC\s+)?%run\s+['\"]?([^\s'\"]+)['\"]?",
        source, re.I | re.M,
    ):
        deps.append({"type": "run", "target": m.group(1).strip()})
    for m in re.finditer(r"dbutils\.notebook\.run\(['\"]([^'\"]+)['\"]", source, re.I):
        deps.append({"type": "notebook_run", "target": m.group(1).strip()})
    for mod in _python_imports(source):
        root = mod.lstrip(".").split(".", 1)[0]
        if root in EXTERNAL_IMPORT_PREFIXES:
            continue
        if not mod.startswith((".", "src", "common", "utils", "lib", "notebooks")) and "." not in mod:
            continue
        deps.append({"type": "import", "target": mod})
    return deps


def _candidate_paths_for_run(seed: str, target: str) -> list[str]:
    seed_dir = Path(seed).parent
    target_path = Path(target)
    bases = []
    if target_path.is_absolute():
        bases.append(Path(str(target_path).lstrip("/")))
    else:
        try:
            bases.append((seed_dir / target_path).resolve().relative_to(Path.cwd()))
        except ValueError:
            bases.append(seed_dir / target_path)
        bases.append(target_path)
    out: list[str] = []
    for base in bases:
        s = str(base)
        out.extend([s, f"{s}.py", f"{s}.ipynb", f"{s}/__init__.py"])
    return out


def _candidate_paths_for_import(seed: str, target: str) -> list[str]:
    seed_dir = Path(seed).parent
    dots = len(target) - len(target.lstrip("."))
    mod = target.lstrip(".")
    mod_path = mod.replace(".", "/")
    search_dirs = [Path("."), seed_dir]
    cur = seed_dir
    for _ in range(max(dots, 1)):
        search_dirs.append(cur)
        cur = cur.parent
    out: list[str] = []
    for root in search_dirs:
        if not mod_path:
            continue
        out.append(str(root / f"{mod_path}.py"))
        out.append(str(root / mod_path / "__init__.py"))
    return out


def _resolve_dep(seed: str, dep: dict[str, str], repo_files: set[str]) -> str:
    candidates = (
        _candidate_paths_for_import(seed, dep["target"])
        if dep["type"] == "import"
        else _candidate_paths_for_run(seed, dep["target"])
    )
    for cand in candidates:
        norm = os.path.normpath(cand).replace("\\", "/")
        if norm in repo_files and _is_source_file(norm):
            return norm
    suffix = dep["target"].strip("./").replace(".", "/")
    suffixes = [f"{suffix}.py", f"{suffix}/__init__.py", f"{suffix}.ipynb"]
    for repo_file in repo_files:
        if any(repo_file.endswith(s) for s in suffixes) and _is_source_file(repo_file):
            return repo_file
    return ""


changed = _git_changed()
repo_files = {
    p for p in subprocess.check_output(["git", "ls-files"], text=True).splitlines()
    if p and not p.startswith(".git/")
}

notebooks = [f for f in changed if f.endswith(".ipynb")]
src_files = [f for f in changed if _is_source_file(f)]
seed_files = (notebooks + src_files)[:MAX_CHANGED_FILES]

full_source = ""
seed_sources: dict[str, str] = {}

for path in seed_files:
    if not os.path.exists(path):
        continue
    try:
        source = _ipynb_code(path) if path.endswith(".ipynb") else _read_text(path)
    except Exception:
        continue
    seed_sources[path] = source
    full_source += _numbered_file_block(path, source)

dependency_sources: dict[str, dict[str, object]] = {}
seen_deps: set[str] = set()

for seed, source in seed_sources.items():
    for dep in _extract_dependencies(source):
        if len(dependency_sources) >= MAX_DEP_FILES:
            break
        resolved = _resolve_dep(seed, dep, repo_files)
        if not resolved or resolved in seen_deps or resolved in seed_sources:
            continue
        try:
            dep_source = _ipynb_code(resolved) if resolved.endswith(".ipynb") else _read_text(resolved)
        except Exception:
            continue
        seen_deps.add(resolved)
        dependency_sources[resolved] = {
            "source": dep_source[:MAX_DEP_SOURCE_CHARS],
            "imported_by": [seed],
            "import_type": dep["type"],
            "import_target": dep["target"],
        }
        full_source += _numbered_file_block(resolved, dep_source, dependency_of=seed)

for forced_path in FORCED_DEPENDENCIES:
    if forced_path in dependency_sources or forced_path in seed_sources:
        continue
    if not os.path.exists(forced_path) or not _is_source_file(forced_path):
        continue
    try:
        forced_source = _read_text(forced_path)
    except Exception:
        continue
    dependency_sources[forced_path] = {
        "source": forced_source[:MAX_DEP_SOURCE_CHARS],
        "imported_by": seed_files or ["PR source"],
        "import_type": "forced_common",
        "import_target": forced_path,
    }
    full_source += _numbered_file_block(
        forced_path,
        forced_source,
        dependency_of=",".join(seed_files or ["PR source"]),
    )

with open("/tmp/full_source.txt", "w", encoding="utf-8") as fh:
    fh.write(full_source[:MAX_FULL_SOURCE_CHARS])

with open("/tmp/dependency_sources.json", "w", encoding="utf-8") as fh:
    json.dump(dependency_sources, fh)
