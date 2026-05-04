"""Apply red/yellow/green alert rules and emit alerts.json."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import PROCESSED, get_logger

log = get_logger("flag")

INPUT_PATH = PROCESSED / "full_funnel_with_deltas.csv"
OUTPUT_PATH = PROCESSED / "alerts.json"

CONVERSION_METRICS = ["Lead_CVR", "MQL_S1_rate", "S1_S2_rate"]


def _alert(severity, metric, channel, current, delta, message):
    return {
        "severity": severity,
        "metric": metric,
        "channel": channel,
        "current_value": None if current is None or (isinstance(current, float) and np.isnan(current)) else float(current),
        "delta": None if delta is None or (isinstance(delta, float) and np.isnan(delta)) else float(delta),
        "message": message,
    }


def _latest_per_channel(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["week_dt"] = pd.to_datetime(df["week"], errors="coerce")
    return df.sort_values("week_dt").groupby("channel", as_index=False).tail(1)


def evaluate(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []

    alerts = []
    latest = _latest_per_channel(df)

    for _, r in latest.iterrows():
        channel = r["channel"]

        cpcw = r.get("CpCW")
        cpcw_delta = r.get("CpCW_wow_delta")
        if pd.notna(cpcw_delta):
            if cpcw_delta > 0.30:
                alerts.append(_alert("red", "CpCW", channel, cpcw, cpcw_delta,
                                     f"{channel} CpCW is {cpcw_delta:+.0%} WoW (>30% above prior week)"))
            elif cpcw_delta > 0.15:
                alerts.append(_alert("yellow", "CpCW", channel, cpcw, cpcw_delta,
                                     f"{channel} CpCW is {cpcw_delta:+.0%} WoW (15-30% above prior week)"))
            elif cpcw_delta < -0.10:
                alerts.append(_alert("green", "CpCW", channel, cpcw, cpcw_delta,
                                     f"{channel} CpCW improved {cpcw_delta:+.0%} WoW"))

        for metric in CONVERSION_METRICS:
            val = r.get(metric)
            delta = r.get(f"{metric}_wow_delta")
            if pd.notna(delta):
                if delta < -0.20:
                    alerts.append(_alert("red", metric, channel, val, delta,
                                         f"{channel} {metric} dropped {delta:+.0%} WoW"))
                elif delta < -0.10:
                    alerts.append(_alert("yellow", metric, channel, val, delta,
                                         f"{channel} {metric} dropped {delta:+.0%} WoW"))
                elif delta > 0.15:
                    alerts.append(_alert("green", metric, channel, val, delta,
                                         f"{channel} {metric} improved {delta:+.0%} WoW"))

        spend_delta = r.get("spend_wow_delta")
        leads_delta = r.get("leads_wow_delta")
        if pd.notna(spend_delta) and spend_delta > 0.40:
            if pd.isna(leads_delta) or leads_delta < spend_delta:
                alerts.append(_alert("red", "spend", channel, r.get("spend"), spend_delta,
                                     f"{channel} spend up {spend_delta:+.0%} WoW without matching lead growth"))

        roi = r.get("ROI")
        if pd.notna(roi) and roi > 3:
            alerts.append(_alert("green", "ROI", channel, roi, None,
                                 f"{channel} ROI is {roi:.1f}x (>3x)"))

    total_s1_latest = latest["s1"].sum() if "s1" in latest.columns else 0
    weekly_target = 100  # placeholder weekly run-rate
    if total_s1_latest < 0.8 * weekly_target:
        alerts.append(_alert("yellow", "s1_pacing", "ALL", float(total_s1_latest), None,
                             f"Total S1 ({int(total_s1_latest)}) is below 80% of weekly run-rate target ({weekly_target})"))

    return alerts


def run():
    log.info("Starting flag evaluation")
    try:
        df = pd.read_csv(INPUT_PATH) if INPUT_PATH.exists() else pd.DataFrame()
    except pd.errors.EmptyDataError:
        df = pd.DataFrame()

    alerts = evaluate(df)
    OUTPUT_PATH.write_text(json.dumps(alerts, indent=2, default=str))
    counts = {"red": 0, "yellow": 0, "green": 0}
    for a in alerts:
        counts[a["severity"]] = counts.get(a["severity"], 0) + 1
    log.info(f"Wrote {len(alerts)} alerts ({counts}) to {OUTPUT_PATH}")
    return True


if __name__ == "__main__":
    run()
