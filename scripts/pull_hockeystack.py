"""Pull HockeyStack attribution and funnel data for the last 30 days."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import RAW, env, get_logger, utc_now_iso, date_range

log = get_logger("pull_hockeystack")

ATTR_PATH = RAW / "hockeystack_attribution.csv"
FUNNEL_PATH = RAW / "hockeystack_funnel.csv"
BASE = "https://api.hockeystack.com/v1"


def _get(path: str, params: dict):
    key = env("HOCKEYSTACK_API_KEY")
    if not key:
        raise RuntimeError("HOCKEYSTACK_API_KEY not set")
    headers = {"Authorization": f"Bearer {key}"}
    r = requests.get(f"{BASE}{path}", headers=headers, params=params, timeout=60)
    if r.status_code in (401, 404):
        log.warning(f"HockeyStack {path} returned {r.status_code}: {r.text[:200]}")
        return None
    r.raise_for_status()
    return r.json()


def fetch():
    start, end = date_range(30)
    params = {"start_date": start.isoformat(), "end_date": end.isoformat()}

    attr_payload = _get("/attribution", params)
    funnel_payload = _get("/funnel", params)

    attr_rows = []
    if attr_payload:
        for row in attr_payload.get("data", []):
            attr_rows.append({
                "channel": row.get("channel"),
                "touchpoints": int(row.get("touchpoints", 0) or 0),
                "influenced_deals": int(row.get("influenced_deals", 0) or 0),
                "influenced_revenue": round(float(row.get("influenced_revenue", 0) or 0), 2),
            })

    funnel_rows = []
    if funnel_payload:
        for row in funnel_payload.get("data", []):
            funnel_rows.append({
                "channel": row.get("channel"),
                "leads": int(row.get("leads", 0) or 0),
                "mqls": int(row.get("mqls", 0) or 0),
                "opportunities": int(row.get("opportunities", 0) or 0),
                "closed_won": int(row.get("closed_won", 0) or 0),
            })
    return attr_rows, funnel_rows


def run():
    log.info("Starting HockeyStack pull")
    try:
        attr, funnel = fetch()
    except Exception as e:
        log.error(f"HockeyStack pull failed: {e}")
        pd.DataFrame(columns=["channel", "touchpoints", "influenced_deals",
                              "influenced_revenue", "pulled_at"]).to_csv(ATTR_PATH, index=False)
        pd.DataFrame(columns=["channel", "leads", "mqls", "opportunities",
                              "closed_won", "pulled_at"]).to_csv(FUNNEL_PATH, index=False)
        return False

    adf = pd.DataFrame(attr)
    fdf = pd.DataFrame(funnel)
    adf["pulled_at"] = utc_now_iso()
    fdf["pulled_at"] = utc_now_iso()
    adf.to_csv(ATTR_PATH, index=False)
    fdf.to_csv(FUNNEL_PATH, index=False)
    log.info(f"Wrote {len(adf)} attribution rows and {len(fdf)} funnel rows")
    return True


if __name__ == "__main__":
    run()
