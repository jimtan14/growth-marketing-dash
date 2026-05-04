"""Pull Meta Ads campaign performance for the last 30 days, broken down by week."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import RAW, env, get_logger, utc_now_iso, date_range, week_floor

log = get_logger("pull_meta_ads")

OUTPUT_PATH = RAW / "meta_ads.csv"


def fetch():
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adaccount import AdAccount

    token = env("META_ACCESS_TOKEN")
    account_id = env("META_AD_ACCOUNT_ID")
    if not token or not account_id:
        raise RuntimeError("META_ACCESS_TOKEN or META_AD_ACCOUNT_ID not set")

    FacebookAdsApi.init(access_token=token)
    account = AdAccount(f"act_{account_id}")
    start, end = date_range(30)

    params = {
        "time_range": {"since": start.isoformat(), "until": end.isoformat()},
        "level": "campaign",
        "fields": ["campaign_id", "campaign_name", "impressions", "clicks", "spend"],
        "time_increment": 1,
    }

    rows = []
    for r in account.get_insights(params=params):
        rows.append({
            "campaign_id": r.get("campaign_id"),
            "campaign_name": r.get("campaign_name"),
            "impressions": int(r.get("impressions", 0)),
            "clicks": int(r.get("clicks", 0)),
            "spend": round(float(r.get("spend", 0)), 2),
            "date": r.get("date_start"),
        })
    return rows


def run():
    log.info("Starting Meta Ads pull")
    try:
        rows = fetch()
    except Exception as e:
        log.error(f"Meta Ads pull failed: {e}")
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
