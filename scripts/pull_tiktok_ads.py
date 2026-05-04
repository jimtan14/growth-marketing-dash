"""Pull TikTok Ads campaign performance for the last 30 days, broken down by week.

Uses the TikTok Marketing API directly via requests for portability.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import RAW, env, get_logger, utc_now_iso, date_range, week_floor

log = get_logger("pull_tiktok_ads")

OUTPUT_PATH = RAW / "tiktok_ads.csv"
BASE = "https://business-api.tiktok.com/open_api/v1.3"


def fetch():
    token = env("TIKTOK_ACCESS_TOKEN")
    advertiser_id = env("TIKTOK_ADVERTISER_ID")
    if not token or not advertiser_id:
        raise RuntimeError("TIKTOK_ACCESS_TOKEN or TIKTOK_ADVERTISER_ID not set")

    headers = {"Access-Token": token, "Content-Type": "application/json"}
    start, end = date_range(30)

    url = f"{BASE}/report/integrated/get/"
    params = {
        "advertiser_id": advertiser_id,
        "report_type": "BASIC",
        "data_level": "AUCTION_CAMPAIGN",
        "dimensions": json.dumps(["campaign_id", "stat_time_day"]),
        "metrics": json.dumps(["campaign_name", "impressions", "clicks", "spend"]),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "page_size": 1000,
    }

    rows = []
    page = 1
    while True:
        params["page"] = page
        r = requests.get(url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        body = r.json()
        if body.get("code") != 0:
            raise RuntimeError(f"TikTok API error: {body.get('message')}")
        data = body.get("data", {})
        for item in data.get("list", []):
            dims = item.get("dimensions", {})
            metrics = item.get("metrics", {})
            rows.append({
                "campaign_id": dims.get("campaign_id"),
                "campaign_name": metrics.get("campaign_name"),
                "impressions": int(metrics.get("impressions", 0) or 0),
                "clicks": int(metrics.get("clicks", 0) or 0),
                "spend": round(float(metrics.get("spend", 0) or 0), 2),
                "date": dims.get("stat_time_day", "")[:10],
            })
        page_info = data.get("page_info", {})
        if page >= page_info.get("total_page", 1):
            break
        page += 1
    return rows


def run():
    log.info("Starting TikTok Ads pull")
    try:
        rows = fetch()
    except Exception as e:
        log.error(f"TikTok Ads pull failed: {e}")
        df = pd.DataFrame(columns=["campaign_id", "campaign_name", "impressions",
                                   "clicks", "spend", "date", "week", "pulled_at"])
        df.to_csv(OUTPUT_PATH, index=False)
        return False

    df = pd.DataFrame(rows)
    if not df.empty:
        df["week"] = df["date"].apply(week_floor)
    else:
        df["week"] = []
    df["pulled_at"] = utc_now_iso()
    df.to_csv(OUTPUT_PATH, index=False)
    log.info(f"Wrote {len(df)} rows to {OUTPUT_PATH}")
    return True


if __name__ == "__main__":
    run()
