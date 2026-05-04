"""Standardize ad CSVs, build pipeline df, join, and compute funnel metrics + deltas."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import RAW, PROCESSED, get_logger, utc_now_iso, week_floor

log = get_logger("transform")

SPEND_OUT = PROCESSED / "spend_data.csv"
PIPELINE_OUT = PROCESSED / "pipeline_by_channel.csv"
FULL_FUNNEL_OUT = PROCESSED / "full_funnel.csv"
FULL_FUNNEL_DELTAS_OUT = PROCESSED / "full_funnel_with_deltas.csv"

PLATFORM_TO_CHANNEL = {
    "google_ads": "Google Search",
    "linkedin_ads": "LinkedIn",
    "meta_ads": "Meta",
    "twitter_ads": "Twitter",
    "bing_ads": "Bing",
    "tiktok_ads": "TikTok",
}

MANUAL_TO_CHANNEL = {
    "vibe": "Vibe CTV",
    "g2": "G2",
    "community": "Community",
    "influencer": "Influencer",
    "chatgpt": "ChatGPT",
}

TRAFFIC_SOURCE_MAP = {
    "google": "Google Search", "google_ads": "Google Search", "cpc": "Google Search",
    "linkedin": "LinkedIn", "linkedin_ads": "LinkedIn",
    "meta": "Meta", "facebook": "Meta", "instagram": "Meta",
    "twitter": "Twitter", "x": "Twitter",
    "bing": "Bing", "bing_ads": "Bing",
    "tiktok": "TikTok", "tiktok_ads": "TikTok",
    "vibe": "Vibe CTV", "ctv": "Vibe CTV",
    "g2": "G2",
    "community": "Community",
    "influencer": "Influencer",
    "chatgpt": "ChatGPT",
    "organic": "Content/Organic", "seo": "Content/Organic",
    "direct": "Direct",
    "other": "Other",
}

S1_KEYWORDS = ["meeting booked", "discovery", "s1"]
S2_KEYWORDS = ["demo", "qualified", "s2"]
CW_KEYWORDS = ["closedwon"]


def _safe_div(num, denom):
    try:
        if denom is None or denom == 0 or pd.isna(denom):
            return None
        return float(num) / float(denom)
    except Exception:
        return None


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _stamp_channel(df: pd.DataFrame, channel: str) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["channel"] = channel
    return df


def build_spend() -> pd.DataFrame:
    parts = []
    for stem, channel in PLATFORM_TO_CHANNEL.items():
        df = _read_csv(RAW / f"{stem}.csv")
        if df.empty:
            continue
        df = _stamp_channel(df, channel)
        parts.append(df)

    manual = _read_csv(RAW / "manual_channels.csv")
    if not manual.empty and "source" in manual.columns:
        manual = manual.copy()
        manual["channel"] = manual["source"].map(MANUAL_TO_CHANNEL).fillna("Other")
        manual["campaign_id"] = None
        parts.append(manual)

    if not parts:
        return pd.DataFrame(columns=["channel", "campaign_name", "campaign_id",
                                     "impressions", "clicks", "spend", "date", "week"])

    cols = ["channel", "campaign_name", "campaign_id", "impressions", "clicks", "spend", "date", "week"]
    for p in parts:
        for c in cols:
            if c not in p.columns:
                p[c] = None
    spend = pd.concat([p[cols] for p in parts], ignore_index=True)
    spend["impressions"] = pd.to_numeric(spend["impressions"], errors="coerce").fillna(0).astype(int)
    spend["clicks"] = pd.to_numeric(spend["clicks"], errors="coerce").fillna(0).astype(int)
    spend["spend"] = pd.to_numeric(spend["spend"], errors="coerce").fillna(0).round(2)
    spend["pulled_at"] = utc_now_iso()
    spend.to_csv(SPEND_OUT, index=False)
    log.info(f"Wrote {len(spend)} spend rows to {SPEND_OUT}")
    return spend


def _normalize_traffic_source(value) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "Other"
    s = str(value).strip().lower()
    if not s:
        return "Other"
    return TRAFFIC_SOURCE_MAP.get(s, s)


def _assign_channel(row) -> str:
    for col in ["airops_original_traffic_source", "utm_source", "hs_analytics_source"]:
        v = row.get(col)
        if v is not None and not (isinstance(v, float) and np.isnan(v)) and str(v).strip():
            mapped = _normalize_traffic_source(v)
            if mapped:
                return mapped
    return "Other"


def _classify_deal_stage(stage: str) -> str | None:
    if not isinstance(stage, str):
        return None
    s = stage.lower().replace(" ", "")
    if any(k.replace(" ", "") in s for k in CW_KEYWORDS):
        return "cw"
    if any(k.replace(" ", "") in s for k in S2_KEYWORDS):
        return "s2"
    if any(k.replace(" ", "") in s for k in S1_KEYWORDS):
        return "s1"
    return None


def _classify_source(channel: str) -> str:
    inbound = {"Google Search", "LinkedIn", "Meta", "Twitter", "Bing", "TikTok",
               "Vibe CTV", "G2", "Community", "Influencer", "ChatGPT",
               "Content/Organic", "Direct"}
    return "inbound" if channel in inbound else "outbound"


def build_pipeline() -> pd.DataFrame:
    contacts = _read_csv(RAW / "hubspot_contacts.csv")
    deals = _read_csv(RAW / "hubspot_deals.csv")

    if contacts.empty and deals.empty:
        log.warning("No HubSpot data; pipeline df will be empty")
        empty = pd.DataFrame(columns=["channel", "week", "segment", "source",
                                      "leads", "mqls", "s1", "s2", "cw", "arr"])
        empty.to_csv(PIPELINE_OUT, index=False)
        return empty

    if not contacts.empty:
        contacts["channel"] = contacts.apply(_assign_channel, axis=1)
        contacts["createdate"] = pd.to_datetime(contacts["createdate"], errors="coerce", utc=True)
        contacts["week"] = contacts["createdate"].dt.tz_convert(None).apply(
            lambda d: week_floor(d) if pd.notna(d) else None
        )
        contacts["segment"] = "Non-ENT"
        contacts["source"] = contacts["channel"].apply(_classify_source)
        contacts["lifecyclestage"] = contacts.get("lifecyclestage", "").fillna("").astype(str).str.lower()
        contacts["is_lead"] = (contacts["lifecyclestage"] == "lead").astype(int)
        contacts["is_mql"] = (contacts["lifecyclestage"] == "marketingqualifiedlead").astype(int)

        contact_agg = contacts.groupby(["channel", "week", "segment", "source"], dropna=False).agg(
            leads=("is_lead", "sum"),
            mqls=("is_mql", "sum"),
        ).reset_index()
    else:
        contact_agg = pd.DataFrame(columns=["channel", "week", "segment", "source", "leads", "mqls"])

    if not deals.empty:
        deals["createdate"] = pd.to_datetime(deals["createdate"], errors="coerce", utc=True)
        deals["week"] = deals["createdate"].dt.tz_convert(None).apply(
            lambda d: week_floor(d) if pd.notna(d) else None
        )
        deals["amount"] = pd.to_numeric(deals.get("amount"), errors="coerce").fillna(0)
        deals["funnel_stage"] = deals["dealstage"].apply(_classify_deal_stage)
        deals["channel"] = deals.get("deal_channel", "").fillna("Other").apply(_normalize_traffic_source)
        deals["segment"] = deals.get("segment", "Non-ENT").fillna("Non-ENT")
        deals["source"] = deals["channel"].apply(_classify_source)

        deals["is_s1"] = (deals["funnel_stage"] == "s1").astype(int)
        deals["is_s2"] = (deals["funnel_stage"] == "s2").astype(int)
        deals["is_cw"] = (deals["funnel_stage"] == "cw").astype(int)
        deals["cw_amount"] = deals["is_cw"] * deals["amount"]

        deal_agg = deals.groupby(["channel", "week", "segment", "source"], dropna=False).agg(
            s1=("is_s1", "sum"),
            s2=("is_s2", "sum"),
            cw=("is_cw", "sum"),
            arr=("cw_amount", "sum"),
        ).reset_index()
    else:
        deal_agg = pd.DataFrame(columns=["channel", "week", "segment", "source", "s1", "s2", "cw", "arr"])

    pipeline = contact_agg.merge(deal_agg, on=["channel", "week", "segment", "source"], how="outer")
    for col in ["leads", "mqls", "s1", "s2", "cw"]:
        if col in pipeline.columns:
            pipeline[col] = pipeline[col].fillna(0).astype(int)
    if "arr" in pipeline.columns:
        pipeline["arr"] = pipeline["arr"].fillna(0).round(2)

    pipeline["pulled_at"] = utc_now_iso()
    pipeline.to_csv(PIPELINE_OUT, index=False)
    log.info(f"Wrote {len(pipeline)} pipeline rows to {PIPELINE_OUT}")
    return pipeline


def _agg_spend_by_channel_week(spend: pd.DataFrame) -> pd.DataFrame:
    if spend.empty:
        return pd.DataFrame(columns=["channel", "week", "impressions", "clicks", "spend"])
    return spend.groupby(["channel", "week"], dropna=False).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        spend=("spend", "sum"),
    ).reset_index()


def _agg_pipeline_by_channel_week(pipeline: pd.DataFrame) -> pd.DataFrame:
    if pipeline.empty:
        return pd.DataFrame(columns=["channel", "week", "leads", "mqls", "s1", "s2", "cw", "arr"])
    return pipeline.groupby(["channel", "week"], dropna=False).agg(
        leads=("leads", "sum"),
        mqls=("mqls", "sum"),
        s1=("s1", "sum"),
        s2=("s2", "sum"),
        cw=("cw", "sum"),
        arr=("arr", "sum"),
    ).reset_index()


def join_full_funnel(spend: pd.DataFrame, pipeline: pd.DataFrame) -> pd.DataFrame:
    spend_agg = _agg_spend_by_channel_week(spend)
    pipe_agg = _agg_pipeline_by_channel_week(pipeline)
    full = spend_agg.merge(pipe_agg, on=["channel", "week"], how="outer")

    for col in ["impressions", "clicks", "leads", "mqls", "s1", "s2", "cw"]:
        full[col] = pd.to_numeric(full.get(col), errors="coerce").fillna(0).astype(int)
    for col in ["spend", "arr"]:
        full[col] = pd.to_numeric(full.get(col), errors="coerce").fillna(0).round(2)

    full["CTR"] = full.apply(lambda r: _safe_div(r["clicks"], r["impressions"]), axis=1)
    full["Lead_CVR"] = full.apply(lambda r: _safe_div(r["leads"], r["clicks"]), axis=1)
    full["Lead_MQL_rate"] = full.apply(lambda r: _safe_div(r["mqls"], r["leads"]), axis=1)
    full["MQL_S1_rate"] = full.apply(lambda r: _safe_div(r["s1"], r["mqls"]), axis=1)
    full["S1_S2_rate"] = full.apply(lambda r: _safe_div(r["s2"], r["s1"]), axis=1)
    full["S2_CW_rate"] = full.apply(lambda r: _safe_div(r["cw"], r["s2"]), axis=1)
    full["CPC"] = full.apply(lambda r: _safe_div(r["spend"], r["clicks"]), axis=1)
    full["CPL"] = full.apply(lambda r: _safe_div(r["spend"], r["leads"]), axis=1)
    full["CpMQL"] = full.apply(lambda r: _safe_div(r["spend"], r["mqls"]), axis=1)
    full["CpS1"] = full.apply(lambda r: _safe_div(r["spend"], r["s1"]), axis=1)
    full["CpS2"] = full.apply(lambda r: _safe_div(r["spend"], r["s2"]), axis=1)
    full["CpCW"] = full.apply(lambda r: _safe_div(r["spend"], r["cw"]), axis=1)
    full["ROI"] = full.apply(lambda r: _safe_div(r["arr"], r["spend"]), axis=1)

    full["pulled_at"] = utc_now_iso()
    full.to_csv(FULL_FUNNEL_OUT, index=False)
    log.info(f"Wrote {len(full)} full-funnel rows to {FULL_FUNNEL_OUT}")
    return full


def add_deltas(full: pd.DataFrame) -> pd.DataFrame:
    if full.empty:
        full.to_csv(FULL_FUNNEL_DELTAS_OUT, index=False)
        return full

    full = full.copy()
    full["week_dt"] = pd.to_datetime(full["week"], errors="coerce")
    full["month"] = full["week_dt"].dt.to_period("M").astype(str)

    metrics = ["CTR", "Lead_CVR", "Lead_MQL_rate", "MQL_S1_rate", "S1_S2_rate",
               "S2_CW_rate", "CPC", "CPL", "CpMQL", "CpS1", "CpS2", "CpCW", "ROI",
               "spend", "leads", "mqls", "s1", "s2", "cw", "arr"]

    full = full.sort_values(["channel", "week_dt"])
    for m in metrics:
        if m in full.columns:
            prior = full.groupby("channel")[m].shift(1).astype(float)
            denom = prior.where(prior != 0, np.nan)
            full[f"{m}_wow_delta"] = (full[m].astype(float) - prior) / denom

    monthly = full.groupby(["channel", "month"], dropna=False)[metrics].sum(min_count=1).reset_index()
    monthly = monthly.sort_values(["channel", "month"])
    for m in metrics:
        prior_m = monthly.groupby("channel")[m].shift(1).astype(float)
        denom = prior_m.where(prior_m != 0, np.nan)
        monthly[f"{m}_mom_delta"] = (monthly[m].astype(float) - prior_m) / denom

    mom_cols = ["channel", "month"] + [f"{m}_mom_delta" for m in metrics]
    full = full.merge(monthly[mom_cols], on=["channel", "month"], how="left")

    full = full.drop(columns=["week_dt"])
    full.to_csv(FULL_FUNNEL_DELTAS_OUT, index=False)
    log.info(f"Wrote {len(full)} delta rows to {FULL_FUNNEL_DELTAS_OUT}")
    return full


def run():
    log.info("Starting transform")
    try:
        spend = build_spend()
        pipeline = build_pipeline()
        full = join_full_funnel(spend, pipeline)
        add_deltas(full)
        return True
    except Exception as e:
        log.error(f"Transform failed: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    run()
