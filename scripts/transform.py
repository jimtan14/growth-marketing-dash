"""Transform: build pipeline keyed on HubSpot deal_channel + original_traffic_source_channel
with Event-Date-based stage triggers, then compute funnel + WoW/MoM deltas."""
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

PLATFORM_TO_DEAL_CHANNEL = {
    "google_ads": "Paid Search",
    "linkedin_ads": "Paid Social",
    "meta_ads": "Paid Social",
    "twitter_ads": "Paid Social",
    "bing_ads": "Paid Search",
    "tiktok_ads": "Paid Social",
}
MANUAL_TO_DEAL_CHANNEL = {
    "vibe": "Offline Advertising",
    "g2": "Paid Display",
    "community": "Community / Influencer",
    "influencer": "Community / Influencer",
    "chatgpt": "Paid AI Search",
}
PLATFORM_TO_SUB_CHANNEL = {
    "google_ads": "Google Search", "linkedin_ads": "LinkedIn", "meta_ads": "Meta",
    "twitter_ads": "Twitter", "bing_ads": "Bing", "tiktok_ads": "TikTok",
}
MANUAL_TO_SUB_CHANNEL = {
    "vibe": "Vibe CTV", "g2": "G2", "community": "Community",
    "influencer": "Influencer", "chatgpt": "ChatGPT",
}


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


def build_pipeline() -> pd.DataFrame:
    contacts = _read_csv(RAW / "hubspot_contacts.csv")
    deals = _read_csv(RAW / "hubspot_deals.csv")

    cols = ["deal_channel", "deal_sub_channel", "week", "segment",
            "leads", "mqls", "s1", "s2", "cw", "arr", "s2_amount"]
    if contacts.empty and deals.empty:
        empty = pd.DataFrame(columns=cols)
        empty.to_csv(PIPELINE_OUT, index=False)
        return empty

    # MQL contacts: keyed on event date (date entered MQL)
    if not contacts.empty:
        contacts = contacts.copy()
        # Channel = original_traffic_source_channel (HubSpot canonical)
        contacts["deal_channel"] = contacts.get(
            "original_traffic_source_channel", "(No value)"
        ).fillna("(No value)").astype(str).str.strip()
        contacts["deal_channel"] = contacts["deal_channel"].replace("", "(No value)")
        contacts["deal_sub_channel"] = "(unattributed)"
        # Event-date based week
        event_col = "mql_event_date" if "mql_event_date" in contacts.columns else "createdate"
        contacts["event_dt"] = pd.to_datetime(contacts[event_col], errors="coerce", utc=True)
        contacts["week"] = contacts["event_dt"].dt.tz_convert(None).apply(
            lambda d: week_floor(d) if pd.notna(d) else None
        )
        contacts["segment"] = "Non-ENT"
        # We only pulled contacts with hs_v2_date_entered_marketingqualifiedlead populated,
        # so every row is by definition an MQL.
        contacts["is_mql"] = 1
        # Lead count requires a separate pull on hs_v2_date_entered_lead — until that
        # lands, do NOT inflate Lead by re-counting MQLs as leads.
        contacts["is_lead"] = 0
        contact_agg = contacts.groupby(
            ["deal_channel", "deal_sub_channel", "week", "segment"], dropna=False
        ).agg(leads=("is_lead", "sum"), mqls=("is_mql", "sum")).reset_index()
    else:
        contact_agg = pd.DataFrame(columns=["deal_channel", "deal_sub_channel", "week", "segment", "leads", "mqls"])

    # Deals: S1 = newbusiness deal created; S2 = override_date_entered__discovery; CW = is_closed_won
    if not deals.empty:
        deals = deals.copy()
        deals["amount"] = pd.to_numeric(deals.get("amount"), errors="coerce").fillna(0)
        deals["deal_channel"] = deals.get("deal_channel", "Other").fillna("Other").astype(str).str.strip()
        deals["deal_channel"] = deals["deal_channel"].replace("", "Other")
        deals["deal_sub_channel"] = deals.get("deal_sub_channel", "Unknown").fillna("Unknown").astype(str).str.strip()
        deals["deal_sub_channel"] = deals["deal_sub_channel"].replace("", "Unknown")
        deals["segment"] = "Non-ENT"
        deals["is_newbusiness"] = (deals.get("dealtype", "") == "newbusiness").astype(int)

        # S1 event = deal createdate (only for newbusiness)
        deals["s1_dt"] = pd.to_datetime(deals.get("createdate"), errors="coerce", utc=True)
        deals["s1_week"] = deals["s1_dt"].dt.tz_convert(None).apply(
            lambda d: week_floor(d) if pd.notna(d) else None
        )
        deals["is_s1"] = deals["is_newbusiness"]

        # S2 event = override_date_entered__discovery (newbusiness only)
        deals["s2_dt"] = pd.to_datetime(deals.get("s2_event_date"), errors="coerce", utc=True)
        deals["s2_week"] = deals["s2_dt"].dt.tz_convert(None).apply(
            lambda d: week_floor(d) if pd.notna(d) else None
        )
        deals["is_s2"] = (deals["s2_dt"].notna() & deals["is_newbusiness"].astype(bool)).astype(int)
        deals["s2_amount_val"] = deals["is_s2"] * deals["amount"]

        # CW = is_closed_won (newbusiness only)
        deals["is_cw"] = ((deals.get("is_closed_won", False).astype(str).str.lower() == "true") &
                          deals["is_newbusiness"].astype(bool)).astype(int)
        deals["cw_dt"] = pd.to_datetime(deals.get("closedate"), errors="coerce", utc=True)
        deals["cw_week"] = deals["cw_dt"].dt.tz_convert(None).apply(
            lambda d: week_floor(d) if pd.notna(d) else None
        )
        deals["cw_amount"] = deals["is_cw"] * deals["amount"]

        # Pivot per stage on its own event date
        s1_agg = deals[deals["is_s1"] == 1].groupby(
            ["deal_channel", "deal_sub_channel", "s1_week", "segment"], dropna=False
        ).agg(s1=("is_s1", "sum")).reset_index().rename(columns={"s1_week": "week"})

        s2_agg = deals[deals["is_s2"] == 1].groupby(
            ["deal_channel", "deal_sub_channel", "s2_week", "segment"], dropna=False
        ).agg(s2=("is_s2", "sum"), s2_amount=("s2_amount_val", "sum")).reset_index().rename(columns={"s2_week": "week"})

        cw_agg = deals[deals["is_cw"] == 1].groupby(
            ["deal_channel", "deal_sub_channel", "cw_week", "segment"], dropna=False
        ).agg(cw=("is_cw", "sum"), arr=("cw_amount", "sum")).reset_index().rename(columns={"cw_week": "week"})
    else:
        s1_agg = pd.DataFrame(columns=["deal_channel", "deal_sub_channel", "week", "segment", "s1"])
        s2_agg = pd.DataFrame(columns=["deal_channel", "deal_sub_channel", "week", "segment", "s2", "s2_amount"])
        cw_agg = pd.DataFrame(columns=["deal_channel", "deal_sub_channel", "week", "segment", "cw", "arr"])

    pipeline = contact_agg
    for other in (s1_agg, s2_agg, cw_agg):
        pipeline = pipeline.merge(other, on=["deal_channel", "deal_sub_channel", "week", "segment"], how="outer")
    for col in ["leads", "mqls", "s1", "s2", "cw"]:
        pipeline[col] = pd.to_numeric(pipeline.get(col), errors="coerce").fillna(0).astype(int)
    for col in ["arr", "s2_amount"]:
        pipeline[col] = pd.to_numeric(pipeline.get(col), errors="coerce").fillna(0).round(2)
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
        s2_amount=("s2_amount", "sum"),
    ).reset_index() if not pipeline.empty else pd.DataFrame(
        columns=["deal_channel", "deal_sub_channel", "week", "leads", "mqls", "s1", "s2", "cw", "arr", "s2_amount"]
    )

    full = spend_agg.merge(pipe_agg, on=["deal_channel", "deal_sub_channel", "week"], how="outer")
    for col in ["impressions", "clicks", "leads", "mqls", "s1", "s2", "cw"]:
        full[col] = pd.to_numeric(full.get(col), errors="coerce").fillna(0).astype(int)
    for col in ["spend", "arr", "s2_amount"]:
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
               "spend", "leads", "mqls", "s1", "s2", "cw", "arr", "s2_amount"]
    full = full.sort_values(["deal_channel", "deal_sub_channel", "week_dt"])
    grp = ["deal_channel", "deal_sub_channel"]
    for m in metrics:
        if m in full.columns:
            prior = full.groupby(grp)[m].shift(1).astype(float)
            denom = prior.where(prior != 0, np.nan)
            full[f"{m}_wow_delta"] = (full[m].astype(float) - prior) / denom

    monthly = full.groupby(grp + ["month"], dropna=False)[metrics].sum(min_count=1).reset_index()
    monthly = monthly.sort_values(grp + ["month"])
    for m in metrics:
        prior_m = monthly.groupby(grp)[m].shift(1).astype(float)
        denom = prior_m.where(prior_m != 0, np.nan)
        monthly[f"{m}_mom_delta"] = (monthly[m].astype(float) - prior_m) / denom
    mom_cols = grp + ["month"] + [f"{m}_mom_delta" for m in metrics]
    full = full.merge(monthly[mom_cols], on=grp + ["month"], how="left")
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
