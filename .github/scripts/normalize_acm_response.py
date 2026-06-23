"""
normalize_acm_response.py
Converts the ACM pr_review_agent JSON response into the rich PR comment
format used in the Omega pipeline (matching PR #108 style).
"""
import json, os, statistics

# ── helpers ──────────────────────────────────────────────────────────────────

def _fmt_dur(secs):
    if secs is None:
        return "—"
    s = int(float(secs))
    return f"{s // 60}m {s % 60}s"

def _fmt_bytes(b):
    b = float(b or 0)
    if b >= 1024**3: return f"{b/1024**3:.1f} GB"
    if b >= 1024**2: return f"{b/1024**2:.1f} MB"
    if b >= 1024:    return f"{b/1024:.1f} KB"
    return f"{b:.0f} B"

def _sparkline(vals):
    bars = "▁▂▃▄▅▆▇█"
    if len(vals) < 2:
        return ""
    lo, hi = min(vals), max(vals)
    span = hi - lo or 1
    return "".join(bars[round((v - lo) / span * 7)] for v in reversed(vals))

def _trend_pct(latest, prior_median):
    if not (latest and prior_median):
        return 0
    return (latest - prior_median) / prior_median * 100

# ── load response ─────────────────────────────────────────────────────────────

with open("/tmp/acm_response.json") as fh:
    acm = json.load(fh)

raw_result = acm.get("result", "{}")
try:
    result = json.loads(raw_result) if isinstance(raw_result, str) else raw_result
except Exception:
    result = {}

risk        = result.get("risk_level", "UNKNOWN")
blocked     = bool(result.get("pr_blocked", False))
findings    = result.get("findings", [])
report_path = result.get("report_url", "")
report_html = result.get("report_html", "")
run_id      = result.get("run_id", "")

# Strip "Author: ..." lines LLM injects into summary
raw_summary = result.get("summary", "")
summary = "\n".join(
    line for line in raw_summary.splitlines()
    if not line.strip().lower().startswith("author")
).strip()

# Absolute report URL
acm_base    = os.environ.get("ACM_BASE", "").rstrip("/")
if str(report_path).startswith(("http://", "https://")):
    report_url = report_path
elif report_path and acm_base:
    report_url = acm_base + report_path
else:
    report_url = report_path
if "/artifacts//" in str(report_url):
    report_url = ""
if not report_url and run_id and acm_base:
    report_url = f"{acm_base}/artifacts/{run_id}/pr_review_report.html"

if report_html:
    with open("/tmp/pr_review_report.html", "w") as _fh:
        _fh.write(report_html)

# ── rich telemetry ─────────────────────────────────────────────────────────────

mr = result.get("metrics_rich") or {}

# prod_runs / stage_runs may be:
#   • a list of run-dicts (legacy LLM output / full metrics_rich)
#   • an int count (simplified engine stats — use prod_run_list / stage_run_list)
_pr_raw = mr.get("prod_runs", [])
_sr_raw = mr.get("stage_runs", [])
if isinstance(_pr_raw, list):
    prod_runs = _pr_raw
else:
    prod_runs = mr.get("prod_run_list", [])
if isinstance(_sr_raw, list):
    stage_runs = _sr_raw
else:
    stage_runs = mr.get("stage_run_list", [])

prod_stats   = mr.get("prod_stats",   {})
stage_stats  = mr.get("stage_stats",  {})
job_identity = mr.get("job_identity", {})
target_tables= mr.get("target_tables",[])
blast_items  = mr.get("blast_radius", [])

# Back-fill prod_stats from top-level mr fields when not populated
if not prod_stats and mr.get("avg_runtime_s") is not None:
    prod_stats = {
        "avg_duration_s":   mr.get("avg_runtime_s"),
        "avg_dbu":          mr.get("avg_dbu", 0),
        "total_spill_bytes": mr.get("q6_spill_tb", 0) * 1024**4,
    }
if not prod_stats and prod_runs:
    durs = [r.get("duration_s") for r in prod_runs if r.get("duration_s")]
    dbus = [r.get("dbu", 0) for r in prod_runs]
    prod_stats = {
        "avg_duration_s":   sum(durs) / len(durs) if durs else None,
        "avg_dbu":          sum(dbus) / len(dbus) if dbus else 0,
        "total_spill_bytes": sum(r.get("spill_bytes", 0) for r in prod_runs),
    }
if not stage_stats and stage_runs:
    durs = [r.get("duration_s") for r in stage_runs if r.get("duration_s")]
    dbus = [r.get("dbu", 0) for r in stage_runs]
    stage_stats = {
        "avg_duration_s": sum(durs) / len(durs) if durs else None,
        "avg_dbu":        sum(dbus) / len(dbus) if dbus else 0,
    }

# ── issues for inline comments ─────────────────────────────────────────────────

issues = []
for f in findings:
    sev = f.get("severity", "INFO")
    issues.append({
        "severity":       "critical" if sev == "RED" else "major" if sev == "AMBER" else "minor",
        "category":       f.get("category", "CODE"),
        "title":          f.get("title", ""),
        "description":    f.get("detail", ""),
        "suggestion":     f.get("recommendation", ""),
        "file":           f.get("file"),
        "line":           f.get("line"),
        "suggested_code": f.get("suggested_code"),
        "blocking":       f.get("blocking", False),
    })

has_critical = blocked or any(i["severity"] == "critical" for i in issues)
red_count    = sum(1 for i in issues if i["severity"] == "critical")
amber_count  = sum(1 for i in issues if i["severity"] == "major")
green_count  = sum(1 for i in issues if i["severity"] == "minor")

# ── compute display stats ──────────────────────────────────────────────────────

p = prod_stats
avg_dur      = p.get("avg_duration_s")
latest_dur   = p.get("latest_duration_s")
runtime_pct  = p.get("runtime_pct", 0)
avg_dbu      = p.get("avg_dbu", 0)
latest_dbu   = p.get("latest_dbu", 0)
dbu_pct      = p.get("dbu_pct", 0)
avg_cost     = p.get("avg_cost_usd", 0)
latest_cost  = p.get("latest_cost_usd", 0)
avg_spill_bytes    = p.get("avg_spill_bytes", 0)
total_spill_bytes  = p.get("total_spill_bytes", 0)
latest_spill_bytes = p.get("latest_spill_bytes", 0)
prod_run_count     = len(prod_runs)
stage_run_count    = len(stage_runs)

avg_spill_gb   = avg_spill_bytes   / 1024**3
total_spill_tb = total_spill_bytes / 1024**4
latest_spill_gb= latest_spill_bytes/ 1024**3
stage_avg_dur  = stage_stats.get("avg_duration_s")
stage_avg_dbu  = stage_stats.get("avg_dbu", 0)

# sparkline from prod run durations
prod_durations = [r["duration_s"] for r in prod_runs if r.get("duration_s")]
prod_dbus      = [r["dbu"]        for r in prod_runs if r.get("dbu")]
prod_spills    = [r["spill_bytes"] for r in prod_runs]
runtime_spark  = _sparkline(prod_durations) if prod_durations else ""
spill_spark    = _sparkline(prod_spills)    if prod_spills    else ""

rt_sign     = "improving" if runtime_pct < 0 else "degrading"
rt_color    = "2ea44f"    if runtime_pct < 0 else "d73a49"
dbu_sign    = "improving" if dbu_pct < 0    else "degrading"
dbu_color   = "2ea44f"    if dbu_pct < 0    else "d73a49"

# job/notebook identity
job_name  = job_identity.get("name", "")
notebook  = (job_name.replace("[prod]-","").replace("[stage]-","")
             .split("[component:")[0].strip() if job_name else "")
nb_path   = f"silver/notebooks/{notebook}" if notebook else "(see changed files)"

prod_job_id  = str(job_identity.get("job_id_str") or job_identity.get("job_id",""))
target_prod  = target_tables[0] if target_tables else ""
target_stage = target_tables[1] if len(target_tables) > 1 else ""

# ── build comment markdown ─────────────────────────────────────────────────────

lines = []

# Header
lines += [
    '<table>',
    '<tr>',
    '<td width="110" align="center" valign="middle" bgcolor="#0D1117" style="border-radius: 6px;">',
    '<img src="https://cdn.prod.website-files.com/69c4c9c7f795b263782ef5be/69c628dd2000dd75fce9d09d_b88c9b1de08c1a38c2ab3fd25b02793e_un-logo-white.svg" width="84">',
    '</td>',
    '<td valign="middle">',
    '<h2>Unravel Code Review</h2>',
    '<sub><b>Telemetry-grounded analysis of this PR against real production behavior.</b> &nbsp;Advisory only — informs reviewers, does not gate deployment.</sub>',
    '</td>',
    '</tr>',
    '</table>',
    '',
]

# Badges
findings_badge = f"{red_count + amber_count + green_count}_·_{red_count}_critical".replace(" ","_")
runtime_badge  = f"Prod_runtime-{rt_sign}_{abs(runtime_pct):.0f}%25-{rt_color}" if prod_run_count else "Prod_runtime-no_data-888888"
cost_badge     = f"Prod_cost-avg_%24{avg_cost:.2f}_·_latest_%24{latest_cost:.2f}-2ea44f".replace(" ","_") if avg_cost else "Prod_cost-no_data-888888"
spill_badge    = f"Prod_spill-avg_~{avg_spill_gb:.0f}_GB_·_latest_{latest_spill_gb:.0f}GB-{'d73a49' if avg_spill_gb > 0 else '2ea44f'}".replace(" ","_")

lines.append(
    f'![Review](https://img.shields.io/badge/Review-advisory_·_non--blocking-4482E3?style=flat-square&labelColor=1a1a2e) '
    f'![Findings](https://img.shields.io/badge/Findings-{findings_badge}-d29922?style=flat-square&labelColor=1a1a2e) '
    f'![Runtime](https://img.shields.io/badge/{runtime_badge}?style=flat-square&labelColor=1a1a2e) '
    f'![Cost](https://img.shields.io/badge/{cost_badge}?style=flat-square&labelColor=1a1a2e) '
    f'![Spill](https://img.shields.io/badge/{spill_badge}?style=flat-square&labelColor=1a1a2e)'
)
lines.append('')

# Top-priority action callout
blocking_findings = [f for f in findings if f.get("blocking") or f.get("severity") == "RED"]
if blocking_findings or summary:
    lines.append('> [!IMPORTANT]')
    if blocking_findings:
        lines.append('> **Suggested fixes in _Files changed_:**')
        for i, f in enumerate(blocking_findings[:3], 1):
            title = f.get("title","")
            detail = f.get("detail","")[:120]
            lines.append(f'> {i}. 🧹 **{title}** — {detail}')
        lines.append('>')
    if avg_dur and avg_spill_gb > 1:
        lines.append(f'> **Why it matters:** this pipeline averages **{_fmt_dur(avg_dur)}** per run and spills **~{avg_spill_gb:.0f} GB to disk** on each run.')
    elif avg_dur:
        lines.append(f'> **Why it matters:** this pipeline averages **{_fmt_dur(avg_dur)}** per run.')
    lines.append('')

if report_url:
    lines.append(f'📊 <a href="{report_url}" target="_blank">Full HTML Report</a>')
    lines.append('')

lines += ['---', '']

# What this PR changes
lines += ['### 🧭 What this PR changes', '', '|  |  |', '|---|---|']
lines.append(f'| 📓 **Notebook** | `{nb_path}` |')
if job_name:
    lines.append(f'| 🧱 **Task / Job** | `{job_name}` |')
if target_prod:
    writes = f'`{target_prod}`'
    if target_stage:
        writes += f' &nbsp;·&nbsp; 🟡 `{target_stage}`'
    lines.append(f'| 🎯 **Writes to** | 🔵 {writes} |')
if prod_run_count or stage_run_count:
    lines.append(f'| 🌐 **Observed in** | 🔵 PROD · {prod_run_count} runs &nbsp;·&nbsp; 🟡 STAGE · {stage_run_count} runs |')
lines.append('')

# Blast radius mermaid
if prod_run_count:
    lines += ['```mermaid', 'flowchart LR']
    nb_label = notebook or "this notebook"
    lines.append(f'  NB["📓 Notebook (this PR)<br/>{nb_label}"]:::pr')
    if target_prod:
        lines.append(f'  TBL[("🎯 Target<br/>{target_prod}")]:::tbl')
    lines.append(f'  P["🔵 PROD · {prod_run_count} runs"]:::prod')
    lines.append(f'  S["🟡 STAGE · {stage_run_count} runs"]:::stage')
    lines.append('  NB --> TBL' if target_prod else '  NB --> P')
    if target_prod:
        lines += ['  TBL --> P', '  TBL --> S']
    lines += [
        '  classDef pr   fill:#e8f4fd,stroke:#4482E3,stroke-width:2px,color:#1a1a2e;',
        '  classDef tbl  fill:#f0fdfa,stroke:#0d9488,color:#134e4a;',
        '  classDef prod fill:#dbeafe,stroke:#2563eb,color:#1e40af;',
        '  classDef stage fill:#fefce8,stroke:#ca8a04,color:#854d0e;',
        '```', '',
    ]

lines += ['---', '']

# Production health table
if prod_run_count:
    lines += ['### 📊 Production health', '']
    lines.append(f'| Metric | 🔵 PROD ({prod_run_count} runs) | Trend | 🟡 Stage ({stage_run_count} runs) |')
    lines.append('|---|---|:--:|---|')

    rt_trend_str = f'`{runtime_spark}` {"✅" if runtime_pct < 0 else "⚠️"} {runtime_pct:+.0f}%' if runtime_spark else f'{"✅" if runtime_pct < 0 else "⚠️"} {runtime_pct:+.0f}%'
    lines.append(f'| ⏱️ **Runtime** | **{_fmt_dur(avg_dur)}** avg · last **{_fmt_dur(latest_dur)}** | {rt_trend_str} | {_fmt_dur(stage_avg_dur)} |')

    if avg_dbu:
        dbu_trend_str = f'{"✅" if dbu_pct < 0 else "⚠️"} {dbu_pct:+.0f}%'
        lines.append(f'| 💰 **Cost** | avg **{avg_dbu:.2f} DBU** (~${avg_cost:.2f}) · latest ~${latest_cost:.2f} | {dbu_trend_str} | {stage_avg_dbu:.2f} DBU |')

    if any(r.get("spill_bytes",0) > 0 for r in prod_runs):
        spill_trend_str = f'`{spill_spark}` {"+0%" if avg_spill_bytes == 0 else "persistent"}' if spill_spark else "persistent"
        lines.append(f'| 💾 **Spill** | avg **~{avg_spill_gb:.0f} GB**/run · {total_spill_tb:.2f} TB total | {spill_trend_str} | 0 |')

    lines += ['', '<sub>Trend % compares the latest run to the median of earlier runs · Cost estimated at $0.70/DBU · Stage environment runs on a much smaller dataset so memory issues don\'t show up there</sub>', '', '---', '']

# Findings table
risk_emoji = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(risk, "⚪")
lines += ['### ⚡ Findings', '', '| # | Type | What we found | Where the data comes from | What to do |', '|:--:|:--:|---|---|---|']
for i, f in enumerate(findings, 1):
    sev = f.get("severity","INFO")
    emoji = {"RED":"🔴","AMBER":"🟡","GREEN":"🟢","INFO":"ℹ️"}.get(sev,"⚪")
    ftype = "📊 Perf" if f.get("category","").upper() in ("PERFORMANCE","COST") else ("🟡 Code" if sev=="AMBER" else "🔵 Info")
    title = f.get("title","")
    detail = (f.get("detail","") or "")[:100]
    evidence = f.get("evidence","") or f.get("category","")
    rec = (f.get("recommendation","") or "")[:100]
    lines.append(f'| {i} | {ftype} | {emoji} **{title}** — {detail} | {evidence} | {rec} |')
lines += ['', '---', '']

# Evidence — run history tables
if prod_runs:
    lines += ['### 📁 Evidence & run detail', '']
    lines += ['<details>', f'<summary><b>🔵 PROD run history — {prod_run_count} runs</b></summary>', '']
    lines += ['| Date | Run ID | Duration | DBU | Spill | State |', '|---|---|--:|--:|--:|:--:|']
    for i, r in enumerate(prod_runs):
        star       = " ⭐" if i == 0 else ""
        date_lbl   = f'**{r["date"]}{star}**' if i == 0 else r["date"]
        dur        = _fmt_dur(r.get("duration_s"))
        dbu        = r.get("dbu",0)
        spill_b    = r.get("spill_bytes",0)
        spill_str  = f'{spill_b/1024**3:.1f} GB 🔴' if spill_b else "—"
        state_icon = "✅" if r.get("result_state","") in ("SUCCEEDED","SUCCESS") else "❌"
        run_id_str = r.get("run_id","—")
        lines.append(f'| {date_lbl} | `{run_id_str}` | {dur} | {dbu:.2f} | {spill_str} | {state_icon} |')
    lines += ['', '</details>', '']

if stage_runs:
    lines += ['<details>', f'<summary><b>🟡 Stage run history — {stage_run_count} runs</b> · no disk spill (runs on a much smaller dataset than production)</summary>', '']
    lines += ['| Date | Run ID | Duration | DBU | State |', '|---|---|--:|--:|:--:|']
    for i, r in enumerate(stage_runs):
        star       = " ⭐" if i == 0 else ""
        date_lbl   = f'**{r["date"]}{star}**' if i == 0 else r["date"]
        dur        = _fmt_dur(r.get("duration_s"))
        dbu        = r.get("dbu",0)
        state_icon = "✅" if r.get("result_state","") in ("SUCCEEDED","SUCCESS") else "❌"
        run_id_str = r.get("run_id","—")
        lines.append(f'| {date_lbl} | `{run_id_str}` | {dur} | {dbu:.2f} | {state_icon} |')
    lines += ['', '</details>', '']

# Full metric breakdown collapsible
if prod_runs and len(prod_durations) > 1:
    med_prior = statistics.median(prod_durations[1:])
    dur_pct   = _trend_pct(prod_durations[0], med_prior)
    med_dbu   = statistics.median(prod_dbus[1:]) if len(prod_dbus) > 1 else (prod_dbus[0] if prod_dbus else 0)
    lines += [
        '<details>',
        '<summary><b>🔬 Full metric breakdown</b> · latest vs baseline</summary>',
        '',
        '| Metric | Latest | Baseline (median prior) | Δ | Status |',
        '|---|--:|--:|--:|:--:|',
        f'| ⏱️ Runtime | {_fmt_dur(prod_durations[0])} | {_fmt_dur(med_prior)} | {dur_pct:+.0f}% | {"✅ improving" if dur_pct < 0 else "🟢 stable"} |',
    ]
    if prod_dbus:
        dbu_delta = _trend_pct(prod_dbus[0], med_dbu)
        lines.append(f'| 💰 DBU | {prod_dbus[0]:.2f} | {med_dbu:.2f} | {dbu_delta:+.0f}% | {"✅ improving" if dbu_delta < 0 else "🟢 stable"} |')
    if any(s > 0 for s in prod_spills):
        lines.append(f'| 💾 Spill (avg) | {avg_spill_gb:.1f} GB | — | — | 🔴 high, persistent |')
    lines += ['', '</details>', '']

lines += ['---', '', '<sub>🔎 Powered by **Unravel AI**</sub>']

comment_md = "\n".join(lines)

# Keep GitHub clean: full telemetry/code evidence lives in the ACM HTML report.
status_label = "BLOCKED" if has_critical else "PASS"
status_icon = "⛔" if has_critical else "✅"
report_line = (
    f"**Full HTML Report:** [Open ACM report]({report_url})"
    if report_url else
    "**Full HTML Report:** unavailable from ACM response"
)
rewrite_count = sum(1 for i in issues if i.get("suggested_code") and i.get("file") and i.get("line"))
comment_md = "\n".join([
    "## Unravel Code Review",
    "",
    f"{status_icon} **Status:** `{status_label}`  ",
    f"**Risk:** `{risk}`  ",
    f"**Findings:** `{len(findings)}` total · `{red_count}` critical · `{amber_count}` major  ",
    f"**Inline rewrite suggestions:** `{rewrite_count}`",
    "",
    report_line,
    "",
    "> Full Databricks run evidence, metric-to-code correlation, agent markdown, and agent HTML links are in the ACM report.",
])

# Only code rewrites should become GitHub inline comments.
inline_issues = [
    i for i in issues
    if i.get("suggested_code") and i.get("file") and i.get("line")
]

# ── output ─────────────────────────────────────────────────────────────────────

out = {
    "status":           "failure" if has_critical else "success",
    "has_critical":     has_critical,
    "pr_blocked":       blocked,
    "risk_level":       risk,
    "summary":          summary,
    "comment_markdown": comment_md,
    "issues":           inline_issues,
    "report_url":       report_url,
    "run_id":           run_id,
    "counts":           {"critical": red_count, "major": amber_count, "minor": green_count},
}

with open("/tmp/response.json", "w") as fh:
    json.dump(out, fh)

print(f"ACM review complete: {risk} risk | blocked={blocked} | {len(findings)} findings | prod_runs={prod_run_count} stage_runs={stage_run_count}")
