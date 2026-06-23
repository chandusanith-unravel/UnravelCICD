import json, os

commit_id = os.getenv("HEAD_SHA", "")

with open("/tmp/response.json") as fh:
    resp = json.load(fh)

issues = resp.get("issues", [])
perfs  = resp.get("performance_insights", [])


def make_body(item):
    prerendered = item.get("comment_body", "").strip()
    if prerendered:
        body = prerendered
    else:
        title = item.get("title", "")
        desc  = item.get("description", "")
        sugg  = item.get("suggestion", "")
        code  = item.get("suggested_code") or ""
        line  = item.get("line")

        body = (
            "### Suggested rewrite"
            + (f": {title}" if title else "")
            + f"\n\n**Line {line}**"
            + (f" — {desc}" if desc else "")
            + (f"\n\n**Why:** {sugg}" if sugg else "")
            + "\n\n```suggestion\n"
            + code.strip()
            + "\n```"
        )

    return body


inline_out = []
file_out   = []

for item in issues + perfs:
    path = item.get("file")
    line = item.get("line")
    code = (item.get("suggested_code") or "").strip()
    if path is None or line is None or not code:
        continue
    body = make_body(item)
    inline_out.append({
        "path": path,
        "line": int(line),
        "side": "RIGHT",
        "commit_id": commit_id,
        "body": body,
    })

with open("/tmp/review_inline.json", "w") as fh:
    json.dump({"comments": inline_out}, fh)

with open("/tmp/review_file_level.json", "w") as fh:
    json.dump({"comments": file_out}, fh)

total = len(issues) + len(perfs)
print(f"{total} findings -> {len(inline_out)} inline + {len(file_out)} file-level comment(s).")
