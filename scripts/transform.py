"""Standardize ad CSVs, build pipeline df keyed on HubSpot deal_channel taxonomy,
join, and compute funnel metrics + WoW/MoM deltas."""
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

# Map ad-platform CSV stems to the HubSpot deal_channel taxonomy.
PLATFORM_TO_DEAL_CHANNEL = {
    "google_ads": "Paid Search",
    "linkedin_ads": "Paid Social",
    "meta_ads": "Paid Social",
    "twitter_ads": "Paid Social",
    "bing_ads": "Paid Search",
    "tiktok_ads": "Paid Social",
}

# Manual source -> deal_channel
MANUAL_TO_DEAL_CHANNEL = {
    "vibe": "Offline Advertising",
    "g2": "Paid Display",
    "community": "Community / Influencer",
    "influencer": "Community / Influencer",
    "chatgpt": "Paid AI Search",
}

# Friendly platform label retained for sub-channel granularity on the spend side.
PLATFORM_TO_SUB_CHANNEL = {
    "google_ads": "Google Search",
    "linkedin_ads": "LinkedIn",
    "meta_ads": "Meta",
    "twitter_ads": "Twitter",
    "bing_ads": "Bing",
    "tiktok_ads": "TikTok",
}

MANUAL_TO_SUB_CHANNEL = {
    "vibe": "Vibe CTV",
    "g2": "G2",
    "community": "Community",
    "influencer": "Influencer",
    "chatgpt": "ChatGPT",
}

# HubSpot hs_analytics_source -> deal_channel (used to attribute leads/MQLs).
ANALYTICS_SOURCE_TO_DEAL_CHANNEL = {
    "DIRECT_TRAFFIC": "Direct",
    "ORGANIC_SEARCH": "Organic Search",
    "PAID_SEARCH": "Paid Search",
    "PAID_SOCIAL": "Paid Social",
    "SOCIAL_MEDIA": "Organic Social",
    "REFERRALS": "Referral",
    "EMAIL_MARKETING": "Email",
    "OFFLINE": "Offline Advertising",
    "OTHER_CAMPAIGNS": "Other",
    "INTEGRATION": "Other",
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


def build_spend() -> pd.DataFrame:
    parts = []
    for stem, deal_channel in PLATFORM_TO_DEAL_CHANNEL.items():
        df = _read_csv(RAW / f"{stem}.csv")
        if df.empty:
            continue
        df = df.copy()
        df["deal_channel"] = deal_channel
        df["deal_sub_channel"] = PLATFORM_TO_SUB_CHANNEL.get(stem, stem)
        parts.append(df)

    manual = _read_csv(RAW / "manual_channels.csv")
    if not manual.empty and "source" in manual.columns:
        manual = manual.copy()
        manual["deal_channel"] = manual["source"].map(MANUAL_TO_DEAL_CHANNEL).fillna("Other")
        manual["deal_sub_channel"] = manual["source"].map(MANUAL_TO_SUB_CHANNEL).fillna(manual["source"])
        manual["campaign_id"] = None
        parts.append(manual)

    cols = ["deal_channel", "deal_sub_channel", "campaign_name", "campaign_id",
            "impressions", "clicks", "spend", "date", "week"]
    if not parts:
        return pd.DataFrame(columns=cols)

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


def _contact_deal_channel(row) -> str:
    src = row.get("airops_original_traffic_source")
    if isinstance(src, str) and src.strip():
        return src.strip()
    src = row.get("hs_analytics_source")
    if isinstance(src, str) and src.strip():
        return ANALYTICS_SOURCE_TO_DEAL_CHANNEL.get(src.strip().upper(), "Other")
    return "Other"


def build_pipeline() -> pd.DataFrame:
    contacts = _read_csv(RAW / "hubspot_contacts.csv")
    deals = _read_csv(RAW / "hubspot_deals.csv")

    cols = ["deal_channel", "deal_sub_channel", "week", "segment",
            "leads", "mqls", "s1", "s2", "cw", "arr"]
    if contacts.empty and deals.empty:
        empty = pd.DataFrame(columns=cols)
        empty.to_csv(PIPELINE_OUT, index=False)
        return empty

    if not contacts.empty:
        contacts = contacts.copy()
        contacts["deal_channel"] = contacts.apply(_contact_deal_channel, axis=1)
        contacts["deal_sub_channel"] = "(unattributed)"
        contacts["createdate"] = pd.to_datetime(contacts["createdate"], errors="coerce", utc=True)
        contacts["week"] = contacts["createdate"].dt.tz_convert(None).apply(
            lambda d: week_floor(d) if pd.notna(d) else None
        )
        contacts["segment"] = "Non-ENT"
        contacts["lifecyclestage"] = contacts.get("lifecyclestage", "").fillna("").astype(str).str.lower()
        contacts["is_lead"] = (contacts["lifecyclestage"] == "lead").astype(int)
        contacts["is_mql"] = (contacts["lifecyclestage"] == "marketingqualifiedlead").astype(int)

        contact_agg = contacts.groupby(
            ["deal_channel", "deal_sub_channel", "week", "segment"], dropna=False
        ).agg(leads=("is_lead", "sum"), mqls=("is_mql", "sum")).reset_index()
    else:
        contact_agg = pd.DataFrame(columns=["deal_channel", "deal_sub_channel", "week", "segment", "leads", "mqls"])

    if not deals.empty:
        deals = deals.copy()
        deals["createdate"] = pd.to_datetime(deals["createdate"], errors="coerce", utc=True)
        deals["week"] = deals["createdate"].dt.tz_convert(None).apply(
            lambda d: week_floor(d) if pd.notna(d) else None
        )
        deals["amount"] = pd.to_numeric(deals.get("amount"), errors="coerce").fillna(0)
        deals["funnel_stage"] = deals["dealstage"].apply(_classify_deal_stage)
        deals["deal_channel"] = deals.get("deal_channel", "Other").fillna("Other")
        deals["deal_sub_channel"] = deals.get("deal_sub_channel", "Unknown").fillna("Unknown")
        deals["segment"] = deals.get("segment", "Non-ENT").fillna("Non-ENT")

        deals["is_s1"] = (deals["funnel_stage"] == "s1").astype(int)
        deals["is_s2"] = (deals["funnel_stage"] == "s2").astype(int)
        deals["is_cw"] = (deals["funnel_stage"] == "cw").astype(int)
        deals["cw_amount"] = deals["is_cw"] * deals["amount"]

        deal_agg = deals.groupby(
            ["deal_channel", "deal_sub_channel", "week", "segment"], dropna=False
        ).agg(
            s1=("is_s1", "sum"), s2=("is_s2", "sum"), cw=("is_cw", "sum"),
            arr=("cw_amount", "sum"),
        ).reset_index()
    else:
        deal_agg = pd.DataFrame(columns=["deal_channel", "deal_sub_channel", "week", "segment", "s1", "s2", "cw", "arr"])

    pipeline = contact_agg.merge(
        deal_agg, on=["deal_channel", "deal_sub_channel", "week", "segment"], how="outer"
    )
    for col in ["leads", "mqls", "s1", "s2", "cw"]:
        pipeline[col] = pd.to_numeric(pipeline.get(col), errors="coerce").fillna(0).astype(int)
    pipeline["arr"] = pd.to_numeric(pipeline.get("arr"), errors="coerce").fillna(0).round(2)
    pipeline["pulled_at"] = utc_now_iso()
    pipeline.to_csv(PIPELINE_OUT, index=False)
    log.info(f"Wrote {len(pipeline)} pipeline rows to {PIPELINE_OUT}")
    return pipeline


def join_full_funnel(spend: pd.DataFrame, pipeline: pd.DataFrame) -> pd.DataFrame:
    spend_agg = spend.groupby(["deal_channel", "deal_sub_channel", "week"], dropna=False).agg(
        impressions=("impressions", "sum"), clicks=("clicks", "sum"), spend=("spend", "sum"),
    ).reset_index() if not spend.empty else pd.DataFrame(
        columns=["deal_channel", "deal_sub_channel", "week", "impressions", "clicks", "spend"]
    )

    pipe_agg = pipeline.groupby(["deal_channel", "deal_sub_channel", "week"], dropna=False).agg(
        leads=("leads", "sum"), mqls=("mqls", "sum"), s1=("s1", "sum"),
        s2=("s2", "sum"), cw=("cw", "sum"), arr=("arr", "sum"),
    ).reset_index() if not pipeline.empty else pd.DataFrame(
        columns=["deal_channel", "deal_sub_channel", "week", "leads", "mqls", "s1", "s2", "cw", "arr"]
    )

    full = spend_agg.merge(pipe_agg, on=["deal_channel", "deal_sub_channel", "week"], how="outer")

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

    full = full.sort_values(["deal_channel", "deal_sub_channel", "week_dt"])
    group_keys = ["deal_channel", "deal_sub_channel"]
    for m in metrics:
        prior = full.groupby(group_keys)[m].shift(1).astype(float)
        denom = prior.where(prior != 0, np.nan)
        full[f"{m}_wow_delta"] = (full[m].astype(float) - prior) / denom

    monthly = full.groupby(group_keys + ["month"], dropna=False)[metrics].sum(min_count=1).reset_index()
    monthly = monthly.sort_values(group_keys + ["month"])
    for m in metrics:
        prior_m = monthly.groupby(group_keys)[m].shift(1).astype(float)
        denom = prior_m.where(prior_m != 0, np.nan)
        monthly[f"{m}_mom_delta"] = (monthly[m].astype(float) - prior_m) / denom

    mom_cols = group_keys + ["month"] + [f"{m}_mom_delta" for m in metrics]
    full = full.merge(monthly[mom_cols], on=group_keys + ["month"], how="left")
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
