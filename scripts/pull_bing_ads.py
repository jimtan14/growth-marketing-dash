"""Pull Bing (Microsoft) Ads campaign performance for the last 30 days, broken down by week."""
from __future__ import annotations

import sys
from io import StringIO
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import RAW, env, get_logger, utc_now_iso, date_range, week_floor

log = get_logger("pull_bing_ads")

OUTPUT_PATH = RAW / "bing_ads.csv"


def fetch():
    from bingads.authorization import OAuthWebAuthCodeGrant, AuthorizationData
    from bingads.service_client import ServiceClient
    from bingads.v13.reporting import ReportingDownloadParameters, ReportingServiceManager
    from bingads.v13.reporting.reporting_service_manager import ReportingServiceManager

    client_id = env("BING_ADS_CLIENT_ID")
    client_secret = env("BING_ADS_CLIENT_SECRET")
    refresh_token = env("BING_ADS_REFRESH_TOKEN")
    developer_token = env("BING_ADS_DEVELOPER_TOKEN")
    customer_id = env("BING_ADS_CUSTOMER_ID")
    account_id = env("BING_ADS_ACCOUNT_ID")

    missing = [k for k, v in {
        "BING_ADS_CLIENT_ID": client_id,
        "BING_ADS_REFRESH_TOKEN": refresh_token,
        "BING_ADS_DEVELOPER_TOKEN": developer_token,
        "BING_ADS_ACCOUNT_ID": account_id,
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Missing Bing Ads credentials: {missing}")

    oauth = OAuthWebAuthCodeGrant(
        client_id=client_id,
        client_secret=client_secret,
        redirection_uri="https://login.microsoftonline.com/common/oauth2/nativeclient",
    )
    oauth.request_oauth_tokens_by_refresh_token(refresh_token)
    auth_data = AuthorizationData(
        account_id=int(account_id),
        customer_id=int(customer_id) if customer_id else None,
        developer_token=developer_token,
        authentication=oauth,
    )

    reporting_service = ServiceClient(
        service="ReportingService",
        version=13,
        authorization_data=auth_data,
        environment="production",
    )

    start, end = date_range(30)
    report_request = reporting_service.factory.create("CampaignPerformanceReportRequest")
    report_request.Format = "Csv"
    report_request.ReportName = "CampaignPerformance"
    report_request.ReturnOnlyCompleteData = False
    report_request.Aggregation = "Daily"

    scope = reporting_service.factory.create("AccountThroughCampaignReportScope")
    scope.AccountIds = {"long": [int(account_id)]}
    report_request.Scope = scope

    columns = reporting_service.factory.create("ArrayOfCampaignPerformanceReportColumn")
    columns.CampaignPerformanceReportColumn.append(
        ["TimePeriod", "CampaignId", "CampaignName", "Impressions", "Clicks", "Spend"]
    )
    report_request.Columns = columns

    time_obj = reporting_service.factory.create("ReportTime")
    custom_date = reporting_service.factory.create("Date")
    time_obj.CustomDateRangeStart = custom_date
    time_obj.CustomDateRangeStart.Day = start.day
    time_obj.CustomDateRangeStart.Month = start.month
    time_obj.CustomDateRangeStart.Year = start.year
    custom_date_end = reporting_service.factory.create("Date")
    time_obj.CustomDateRangeEnd = custom_date_end
    time_obj.CustomDateRangeEnd.Day = end.day
    time_obj.CustomDateRangeEnd.Month = end.month
    time_obj.CustomDateRangeEnd.Year = end.year
    report_request.Time = time_obj

    download_params = ReportingDownloadParameters(
        report_request=report_request,
        result_file_directory=str(RAW),
        result_file_name="bing_raw.csv",
        overwrite_result_file=True,
        timeout_in_milliseconds=120000,
    )
    manager = ReportingServiceManager(authorization_data=auth_data, environment="production")
    file_path = manager.download_file(download_params)

    df = pd.read_csv(file_path, skiprows=10)
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "campaign_id": r.get("CampaignId"),
            "campaign_name": r.get("CampaignName"),
            "impressions": int(r.get("Impressions", 0) or 0),
            "clicks": int(r.get("Clicks", 0) or 0),
            "spend": round(float(r.get("Spend", 0) or 0), 2),
            "date": str(r.get("TimePeriod", "")),
        })
    return rows


def run():
    log.info("Starting Bing Ads pull")
    try:
        rows = fetch()
    except Exception as e:
        log.error(f"Bing Ads pull failed: {e}")
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
