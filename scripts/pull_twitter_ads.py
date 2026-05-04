"""Pull Twitter Ads campaign performance for the last 30 days, broken down by week.

Note: the official `twitter-ads` SDK is no longer maintained on recent Python versions.
This implementation uses the X Ads REST API via the requests library and OAuth1.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import requests
from requests_oauthlib import OAuth1

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import RAW, env, get_logger, utc_now_iso, date_range, week_floor

log = get_logger("pull_twitter_ads")

OUTPUT_PATH = RAW / "twitter_ads.csv"
BASE = "https://ads-api.twitter.com/12"


def _auth():
    return OAuth1(
        env("TWITTER_API_KEY"),
        env("TWITTER_API_SECRET"),
        env("TWITTER_ACCESS_TOKEN"),
        env("TWITTER_ACCESS_SECRET"),
    )


def fetch():
    account_id = env("TWITTER_AD_ACCOUNT_ID")
    if not account_id:
        raise RuntimeError("TWITTER_AD_ACCOUNT_ID not set")

    auth = _auth()
    start, end = date_range(30)

    campaigns_url = f"{BASE}/accounts/{account_id}/campaigns"
    cr = requests.get(campaigns_url, auth=auth, timeout=30)
    cr.raise_for_status()
    campaigns = cr.json().get("data", [])
    name_by_id = {c["id"]: c.get("name", "") for c in campaigns}

    rows = []
    for cid in name_by_id:
        stats_url = f"{BASE}/stats/accounts/{account_id}"
        params = {
            "entity": "CAMPAIGN",
            "entity_ids": cid,
            "start_time": f"{start.isoformat()}T00:00:00Z",
            "end_time": f"{end.isoformat()}T00:00:00Z",
            "granularity": "DAY",
            "metric_groups": "ENGAGEMENT,BILLING",
            "placement": "ALL_ON_TWITTER",
        }
        sr = requests.get(stats_url, auth=auth, params=params, timeout=60)
        sr.raise_for_status()
        for entry in sr.json().get("data", []):
            metrics = entry.get("id_data", [{}])[0].get("metrics", {})
            impressions = metrics.get("impressions") or []
            clicks = metrics.get("clicks") or []
            spend = metrics.get("billed_charge_local_micro") or []
            for i, day_offset in enumerate(range((end - start).days)):
                d = (start.fromordinal(start.toordinal() + day_offset)).isoformat()
                rows.append({
                    "campaign_id": cid,
                    "campaign_name": name_by_id[cid],
                    "impressions": int(impressions[i]) if i < len(impressions) and impressions[i] else 0,
                    "clicks": int(clicks[i]) if i < len(clicks) and clicks[i] else 0,
                    "spend": round((spend[i] or 0) / 1_000_000, 2) if i < len(spend) else 0.0,
                    "date": d,
                })
    return rows


def run():
    log.info("Starting Twitter Ads pull")
    try:
        rows = fetch()
    except Exception as e:
        log.error(f"Twitter Ads pull failed: {e}")
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
