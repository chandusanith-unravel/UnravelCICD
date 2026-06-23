import json, sys, subprocess, os

base_sha = sys.argv[1]
head_sha = sys.argv[2]

try:
    changed = subprocess.check_output(
        ["git", "diff", "--name-only", "--diff-filter=ACMRT", base_sha, head_sha],
        text=True, stderr=subprocess.DEVNULL
    ).splitlines()
except Exception:
    changed = []

notebooks = [f for f in changed if f.endswith(".ipynb")]
result = {}

for nb_path in notebooks[:10]:
    if not os.path.exists(nb_path):
        continue
    try:
        with open(nb_path, "r", encoding="utf-8") as fh:
            nb = json.load(fh)
    except Exception:
        continue

    cells_out = []
    for idx, cell in enumerate(nb.get("cells", [])):
        ctype = cell.get("cell_type", "code")
        if ctype not in ("code", "markdown"):
            continue
        src = cell.get("source", [])
        if isinstance(src, list):
            src = "".join(src)
        src = src.strip()
        if not src:
            continue
        cells_out.append({
            "cell_index": idx + 1,
            "cell_type": ctype,
            "source": src[:1500]
        })

    if cells_out:
        result[nb_path] = cells_out

with open("/tmp/notebook_cells.json", "w") as fh:
    json.dump(result, fh)
