"""Pull Google Ads campaign performance for the last 30 days, broken down by week."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import RAW, env, get_logger, utc_now_iso, date_range, week_floor

log = get_logger("pull_google_ads")

OUTPUT_PATH = RAW / "google_ads.csv"


def _client():
    from google.ads.googleads.client import GoogleAdsClient

    cfg = {
        "developer_token": env("GOOGLE_ADS_DEVELOPER_TOKEN"),
        "client_id": env("GOOGLE_ADS_CLIENT_ID"),
        "client_secret": env("GOOGLE_ADS_CLIENT_SECRET"),
        "refresh_token": env("GOOGLE_ADS_REFRESH_TOKEN"),
        "use_proto_plus": True,
    }
    return GoogleAdsClient.load_from_dict(cfg)


def fetch():
    customer_id = env("GOOGLE_ADS_CUSTOMER_ID")
    if not customer_id:
        raise RuntimeError("GOOGLE_ADS_CUSTOMER_ID not set")

    client = _client()
    ga_service = client.get_service("GoogleAdsService")

    start, end = date_range(30)
    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            segments.date
        FROM campaign
        WHERE campaign.status = 'ENABLED'
          AND segments.date BETWEEN '{start.isoformat()}' AND '{end.isoformat()}'
    """

    rows = []
    response = ga_service.search_stream(customer_id=customer_id, query=query)
    for batch in response:
        for r in batch.results:
            rows.append({
                "campaign_id": r.campaign.id,
                "campaign_name": r.campaign.name,
                "impressions": int(r.metrics.impressions),
                "clicks": int(r.metrics.clicks),
                "spend": round(r.metrics.cost_micros / 1_000_000, 2),
                "date": r.segments.date,
            })
    return rows


def run():
    log.info("Starting Google Ads pull")
    try:
        rows = fetch()
    except Exception as e:
        log.error(f"Google Ads pull failed: {e}")
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
