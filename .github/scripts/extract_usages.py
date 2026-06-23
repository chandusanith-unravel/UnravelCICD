import sys, re, subprocess

with open("/tmp/diff.txt", "r") as f:
    diff = f.read()

symbols = set()
for line in diff.splitlines():
    if line.startswith("+") or line.startswith("-") or line.startswith(" ") or line.startswith("@@"):
        m = re.search(r'\b(?:def|class)\s+([a-zA-Z0-9_]+)', line)
        if m:
            symbols.add(m.group(1))

ignore = {"main", "init", "__init__", "get", "set", "run"}
symbols = {s for s in symbols if len(s) > 3 and s not in ignore}
snippets = ""

for sym in sorted(symbols):
    try:
        out = subprocess.check_output(
            ["git", "grep", "-n", "-B", "5", "-A", "5", r"\b" + sym + r"\b"],
            text=True, stderr=subprocess.DEVNULL
        )
        if out.strip():
            snippets += f"=== Usages of `{sym}` ===\n{out[:4000]}\n\n"
    except Exception:
        pass

with open("/tmp/usage_snippets.txt", "w") as f:
    f.write(snippets[:8000])
