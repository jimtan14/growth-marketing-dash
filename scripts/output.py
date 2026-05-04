"""Refresh dashboard.html, write weekly_report.md, and post Slack summary."""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import OUTPUT, PROCESSED, env, get_logger, utc_now_iso

log = get_logger("output")

FUNNEL_PATH = PROCESSED / "full_funnel_with_deltas.csv"
ALERTS_PATH = PROCESSED / "alerts.json"
DASHBOARD_PATH = OUTPUT / "dashboard.html"
REPORT_PATH = OUTPUT / "weekly_report.md"

MONTHLY_TARGETS = {
    "leads": 4000, "mqls": 800, "s1": 400, "s2": 200, "cw": 40, "arr": 500_000,
}

DASHBOARD_DATA_RE = re.compile(
    r"/\* DASHBOARD_DATA_START \*/.*?/\* DASHBOARD_DATA_END \*/", re.DOTALL,
)

CONVERSION_DROP_METRICS = {"Lead_CVR", "MQL_S1_rate", "S1_S2_rate"}


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    out = df.where(pd.notna(df), None).to_dict(orient="records")
    cleaned = []
    for row in out:
        clean = {}
        for k, v in row.items():
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                clean[k] = None
            elif isinstance(v, (np.integer,)):
                clean[k] = int(v)
            elif isinstance(v, (np.floating,)):
                clean[k] = float(v)
            else:
                clean[k] = v
        cleaned.append(clean)
    return cleaned


def refresh_dashboard(df: pd.DataFrame, alerts: list[dict]):
    if not DASHBOARD_PATH.exists():
        log.warning(f"Dashboard template missing at {DASHBOARD_PATH}; skipping refresh")
        return

    html = DASHBOARD_PATH.read_text()
    payload = {
        "generated_at": utc_now_iso(),
        "rows": _df_to_records(df),
        "alerts": alerts,
    }
    block = "/* DASHBOARD_DATA_START */\n" + json.dumps(payload, indent=2, default=str) + "\n/* DASHBOARD_DATA_END */"

    if DASHBOARD_DATA_RE.search(html):
        html = DASHBOARD_DATA_RE.sub(lambda _m: block, html)
    else:
        log.warning("Dashboard markers not found; appending data script")
        html = html.replace(
            "</body>",
            f'<script id="dashboard-data" type="application/json">{block}</script></body>',
        )
    DASHBOARD_PATH.write_text(html)
    log.info(f"Refreshed {DASHBOARD_PATH}")


def _latest_week(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["week_dt"] = pd.to_datetime(df["week"], errors="coerce")
    max_w = df["week_dt"].max()
    return df[df["week_dt"] == max_w].drop(columns=["week_dt"])


def _date_range_label(df: pd.DataFrame) -> str:
    if df.empty:
        today = datetime.now(timezone.utc).date()
        return f"{(today - timedelta(days=7)).isoformat()} to {today.isoformat()}"
    weeks = pd.to_datetime(df["week"], errors="coerce").dropna()
    if weeks.empty:
        today = datetime.now(timezone.utc).date()
        return f"{(today - timedelta(days=7)).isoformat()} to {today.isoformat()}"
    end = weeks.max().date()
    start = end - timedelta(days=6)
    return f"{start.isoformat()} to {end.isoformat()}"


def _fmt_money(v): return "—" if v is None or pd.isna(v) else f"${v:,.0f}"
def _fmt_pct(v): return "—" if v is None or pd.isna(v) else f"{v*100:+.1f}%"
def _fmt_num(v): return "—" if v is None or pd.isna(v) else f"{int(v):,}"
def _fmt_x(v): return "—" if v is None or pd.isna(v) else f"{v:.2f}x"


def build_report(df: pd.DataFrame, alerts: list[dict]) -> str:
    label = _date_range_label(df)
    lines = [f"# Weekly Pipeline Report — {label}", ""]

    lines.append("## Alerts")
    flagged = [a for a in alerts if a["severity"] in ("red", "yellow")]
    lines.append("No red or yellow alerts this week." if not flagged
                 else "\n".join(f"- **[{a['severity'].upper()}]** {a['message']}" for a in flagged))
    lines.append("")

    lines.append("## Full Funnel Summary")
    latest = _latest_week(df)
    if latest.empty:
        lines.append("_No data available._")
    else:
        agg = latest.groupby("deal_channel", as_index=False).agg(
            spend=("spend", "sum"), leads=("leads", "sum"), mqls=("mqls", "sum"),
            s1=("s1", "sum"), s2=("s2", "sum"), cw=("cw", "sum"),
            arr=("arr", "sum"),
        )
        agg["CpCW"] = agg.apply(lambda r: r["spend"] / r["cw"] if r["cw"] else None, axis=1)
        agg["ROI"] = agg.apply(lambda r: r["arr"] / r["spend"] if r["spend"] else None, axis=1)
        lines.append("| deal_channel | spend | leads | mqls | s1 | s2 | cw | cpcw | roi |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
        for _, r in agg.sort_values("spend", ascending=False).iterrows():
            lines.append(
                f"| {r['deal_channel']} | {_fmt_money(r['spend'])} | "
                f"{_fmt_num(r['leads'])} | {_fmt_num(r['mqls'])} | "
                f"{_fmt_num(r['s1'])} | {_fmt_num(r['s2'])} | {_fmt_num(r['cw'])} | "
                f"{_fmt_money(r['CpCW'])} | {_fmt_x(r['ROI'])} |"
            )
    lines.append("")

    lines.append("## Pacing vs Target")
    lines.append("| stage | actual_mtd | target | pacing_pct | status |")
    lines.append("|---|---:|---:|---:|---|")
    if df.empty:
        for stage in ["leads", "mqls", "s1", "s2", "cw", "arr"]:
            target = MONTHLY_TARGETS.get(stage, 0)
            lines.append(f"| {stage} | 0 | {target:,} | — | n/a |")
    else:
        df2 = df.copy()
        df2["week_dt"] = pd.to_datetime(df2["week"], errors="coerce")
        current_month = df2["week_dt"].max().to_period("M")
        mtd = df2[df2["week_dt"].dt.to_period("M") == current_month]
        for stage in ["leads", "mqls", "s1", "s2", "cw", "arr"]:
            actual = float(mtd[stage].sum()) if stage in mtd.columns else 0.0
            target = MONTHLY_TARGETS.get(stage, 0)
            pct_val = actual / target if target else None
            status = "on track" if pct_val and pct_val >= 0.9 else "behind" if pct_val is not None else "n/a"
            actual_str = f"${actual:,.0f}" if stage == "arr" else f"{int(actual):,}"
            target_str = f"${target:,}" if stage == "arr" else f"{target:,}"
            pct_str = f"{pct_val*100:.0f}%" if pct_val is not None else "—"
            lines.append(f"| {stage} | {actual_str} | {target_str} | {pct_str} | {status} |")
    lines.append("")

    lines.append("## Recommended Actions")
    actions = []
    for a in alerts:
        ch = f"{a['channel']} / {a.get('sub_channel') or ''}".strip(" /")
        if a["severity"] == "red" and a["metric"] == "CpCW":
            actions.append(f"- Pause or rebudget {ch}: CpCW {_fmt_pct(a['delta'])} WoW.")
        if a["severity"] == "red" and a["metric"] in CONVERSION_DROP_METRICS:
            actions.append(f"- Investigate {a['metric']} on {ch} ({_fmt_pct(a['delta'])} WoW).")
        if a["severity"] == "green" and a["metric"] == "ROI":
            actions.append(f"- Increase budget on {ch} (ROI {a['current_value']:.1f}x).")
    if not actions:
        actions = ["- No urgent alerts — continue monitoring next week."]
    lines.extend(actions[:5])
    return "\n".join(lines)


def send_slack(df: pd.DataFrame, alerts: list[dict]):
    webhook = env("SLACK_WEBHOOK_URL")
    if not webhook:
        log.warning("SLACK_WEBHOOK_URL not set; skipping Slack notification")
        return False
    label = _date_range_label(df)
    red_alerts = [a for a in alerts if a["severity"] == "red"]
    text_lines = [f"*Weekly Pipeline Report — {label}*", f"Red alerts: {len(red_alerts)}"]
    for a in red_alerts[:5]:
        text_lines.append(f"  • {a['message']}")
    text_lines.append("Full report: output/weekly_report.md")
    try:
        r = requests.post(webhook, json={"text": "\n".join(text_lines)}, timeout=15)
        r.raise_for_status()
        log.info("Slack notification sent")
        return True
    except Exception as e:
        log.error(f"Slack post failed: {e}")
        return False


def run():
    log.info("Starting output stage")
    try:
        df = pd.read_csv(FUNNEL_PATH) if FUNNEL_PATH.exists() else pd.DataFrame()
    except pd.errors.EmptyDataError:
        df = pd.DataFrame()
    try:
        alerts = json.loads(ALERTS_PATH.read_text()) if ALERTS_PATH.exists() else []
    except Exception:
        alerts = []

    refresh_dashboard(df, alerts)
    REPORT_PATH.write_text(build_report(df, alerts))
    log.info(f"Wrote {REPORT_PATH}")
    send_slack(df, alerts)
    return True


if __name__ == "__main__":
    run()
