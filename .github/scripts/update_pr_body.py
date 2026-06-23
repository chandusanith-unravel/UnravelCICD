import json
import sys


START = "<!-- unravel-code-review:start -->"
END = "<!-- unravel-code-review:end -->"


def main():
    body_path, response_path = sys.argv[1], sys.argv[2]
    with open(body_path, encoding="utf-8") as fh:
        body = fh.read()
    with open(response_path, encoding="utf-8") as fh:
        response = json.load(fh)

    report_url = response.get("report_url") or ""
    risk = response.get("risk_level") or "UNKNOWN"
    blocked = bool(response.get("pr_blocked") or response.get("has_critical"))
    status = "BLOCKED" if blocked else "PASS"

    block = "\n".join([
        START,
        "### Unravel Code Review",
        "",
        f"**Status:** `{status}`  ",
        f"**Risk:** `{risk}`  ",
        f"**Full HTML Report:** [Open ACM report]({report_url})",
        END,
    ])

    if START in body and END in body:
        before = body.split(START, 1)[0].rstrip()
        after = body.split(END, 1)[1].lstrip()
        new_body = f"{before}\n\n{block}\n\n{after}".strip()
    else:
        new_body = f"{body.rstrip()}\n\n{block}".strip()

    print(json.dumps({"body": new_body}))


if __name__ == "__main__":
    main()
