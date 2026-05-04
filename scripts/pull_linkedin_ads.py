"""Pull LinkedIn Ads campaign performance for the last 30 days, broken down by week."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import RAW, env, get_logger, utc_now_iso, date_range, week_floor

log = get_logger("pull_linkedin_ads")

OUTPUT_PATH = RAW / "linkedin_ads.csv"
BASE = "https://api.linkedin.com/rest"


def _headers():
    token = env("LINKEDIN_ACCESS_TOKEN")
    if not token:
        raise RuntimeError("LINKEDIN_ACCESS_TOKEN not set")
    return {
        "Authorization": f"Bearer {token}",
        "LinkedIn-Version": "202401",
        "X-Restli-Protocol-Version": "2.0.0",
    }


def _campaign_names(account_id: str, headers: dict) -> dict[str, str]:
    url = f"{BASE}/adAccounts/{account_id}/adCampaigns"
    r = requests.get(url, headers=headers, params={"q": "search"}, timeout=30)
    r.raise_for_status()
    body = r.json()
    return {str(c["id"]): c.get("name", "") for c in body.get("elements", [])}


def fetch():
    account_id = env("LINKEDIN_AD_ACCOUNT_ID")
    if not account_id:
        raise RuntimeError("LINKEDIN_AD_ACCOUNT_ID not set")
    headers = _headers()

    start, end = date_range(30)
    params = {
        "q": "analytics",
        "pivot": "CAMPAIGN",
        "timeGranularity": "DAILY",
        "dateRange.start.day": start.day,
        "dateRange.start.month": start.month,
        "dateRange.start.year": start.year,
        "dateRange.end.day": end.day,
        "dateRange.end.month": end.month,
        "dateRange.end.year": end.year,
        "accounts[0]": f"urn:li:sponsoredAccount:{account_id}",
        "fields": "pivotValue,impressions,clicks,costInLocalCurrency,dateRange",
    }
    url = f"{BASE}/adAnalytics"
    r = requests.get(url, headers=headers, params=params, timeout=60)
    r.raise_for_status()
    elements = r.json().get("elements", [])
    names = _campaign_names(account_id, headers)

    rows = []
    for e in elements:
        pivot = e.get("pivotValue", "")
        cid = pivot.split(":")[-1] if pivot else ""
        dr = e.get("dateRange", {}).get("start", {})
        date = f"{dr.get('year')}-{dr.get('month'):02d}-{dr.get('day'):02d}" if dr else ""
        rows.append({
            "campaign_id": cid,
            "campaign_name": names.get(cid, ""),
            "impressions": int(e.get("impressions", 0)),
            "clicks": int(e.get("clicks", 0)),
            "spend": round(float(e.get("costInLocalCurrency", 0)), 2),
            "date": date,
        })
    return rows


def run():
    log.info("Starting LinkedIn Ads pull")
    try:
        rows = fetch()
    except Exception as e:
        log.error(f"LinkedIn Ads pull failed: {e}")
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
