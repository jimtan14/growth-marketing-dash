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

# HubSpot deal_source_2024 internal name -> human-readable Sub-Channel label
SUB_CHANNEL_LABEL = {
    "Inbound - Network Referral [Name]": "Network Referral",
    "Inbound - Customer Referral [Name]": "Customer Referral",
    "Inbound - Partner Referral: [Name]": "Partner Referral",
    "Inbound – Partner Co-Marketing": "Partner Co-Marketing",
    "Inbound - Referral / WOM": "Word of Mouth",
    "Inbound - Webinar": "Webinar Series / Workshop",
    "Inbound - LinkedIn": "LinkedIn",
    "Inbound - Affiliate": "Affiliate",
    "Inbound - Community / Newsletter / Influencer": "Community / Newsletter",
    "Inbound - Content / SEO / LLMs": "Google",
    "Enablement - Cohort": "Cohort",
    "Outbound - LinkedIn Dripify Sequence": "LinkedIn Message",
    "Outbound - Conference": "Conference",
    "Outbound - Founder Connection": "Founder Connection",
    "Outbound - Email Sequence": "Email Sequence",
    "Outbound - Event/Dinner Program": "Dinner Program",
    "Outbound - GTN LI Outreach": "GTN LI Outreach",
    "Product-Led Opp - Intercom": "Intercom",
    "Product-Led Opp - Builder Slack": "Builder Slack",
    "Product-Led Opp - Email Sequence": "Customer.io",
    "Inbound - Other": "Other",
}


def _label_sub_channel(s):
    if not isinstance(s, str):
        return s
    return SUB_CHANNEL_LABEL.get(s.strip(), s.strip())


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


def _load_aggregated_leads() -> pd.DataFrame:
    """Load monthly aggregated Lead counts from data/manual/_leads_monthly.csv.

    The file contains pre-aggregated monthly totals per deal_channel sourced from
    the HubSpot Lead report (filter: hs_object_source_label != 'Sales Extension').
    We bucket each month's total onto a single mid-month week so it appears in
    the right monthly bucket without needing to paginate 42K raw contact rows.
    """
    path = ROOT_MANUAL = Path(__file__).resolve().parent.parent / "data" / "manual" / "_leads_monthly.csv"
    if not path.exists():
        return pd.DataFrame(columns=["deal_channel", "deal_sub_channel", "week", "segment", "leads"])
    df = pd.read_csv(path)
    if df.empty:
        return pd.DataFrame(columns=["deal_channel", "deal_sub_channel", "week", "segment", "leads"])
    # Place the entire month's leads on a single mid-month Sunday so it always
    # bucks correctly when grouped by month (and the dashboard pivot can show
    # weekly with a single non-zero week per month).
    df["week"] = df["month"].apply(lambda m: f"{m}-15").apply(lambda d: week_floor(d))
    df["deal_sub_channel"] = "(unattributed)"
    df["segment"] = "Non-ENT"
    df["leads"] = df["leads"].astype(int)
    return df[["deal_channel", "deal_sub_channel", "week", "segment", "leads"]]


def _build_create_date_pipeline(contacts: pd.DataFrame, deals: pd.DataFrame) -> pd.DataFrame:
    """Cohort view: each metric for a lead/deal lands in that object's createdate week."""
    parts = []
    if not contacts.empty:
        c = contacts.copy()
        c["deal_channel"] = c.get("original_traffic_source_channel", "(No value)").fillna("(No value)").astype(str).str.strip().replace("", "(No value)")
        c["deal_sub_channel"] = "(unattributed)"
        c["segment"] = "Non-ENT"
        c["create_dt"] = pd.to_datetime(c["createdate"], errors="coerce", utc=True)
        c["week"] = c["create_dt"].dt.tz_convert(None).apply(lambda d: week_floor(d) if pd.notna(d) else None)
        c["is_mql"] = pd.to_datetime(c.get("mql_event_date"), errors="coerce", utc=True).notna().astype(int)
        agg = c.groupby(["deal_channel", "deal_sub_channel", "week", "segment"], dropna=False).agg(
            mqls=("is_mql", "sum"),
        ).reset_index()
        parts.append(agg)
    if not deals.empty:
        d = deals.copy()
        d["amount"] = pd.to_numeric(d.get("amount"), errors="coerce").fillna(0)
        d["deal_channel"] = d.get("deal_channel", "Other").fillna("Other").astype(str).str.strip().replace("", "Other")
        d["deal_sub_channel"] = d.get("deal_sub_channel", "Unknown").fillna("Unknown").astype(str).str.strip().replace("", "Unknown").apply(_label_sub_channel)
        d["segment"] = "Non-ENT"
        d["create_dt"] = pd.to_datetime(d["createdate"], errors="coerce", utc=True)
        d["week"] = d["create_dt"].dt.tz_convert(None).apply(lambda d: week_floor(d) if pd.notna(d) else None)
        d["is_newbusiness"] = (d.get("dealtype", "") == "newbusiness").astype(int)
        d["is_s1"] = d["is_newbusiness"]
        d["is_s2"] = (pd.to_datetime(d.get("s2_event_date"), errors="coerce", utc=True).notna() & d["is_newbusiness"].astype(bool)).astype(int)
        d["is_cw"] = ((d.get("is_closed_won", False).astype(str).str.lower() == "true") & d["is_newbusiness"].astype(bool)).astype(int)
        d["s2_amount_val"] = d["is_s2"] * d["amount"]
        d["cw_amount"] = d["is_cw"] * d["amount"]
        agg = d.groupby(["deal_channel", "deal_sub_channel", "week", "segment"], dropna=False).agg(
            s1=("is_s1", "sum"), s2=("is_s2", "sum"), cw=("is_cw", "sum"),
            arr=("cw_amount", "sum"), s2_amount=("s2_amount_val", "sum"),
        ).reset_index()
        parts.append(agg)
    if not parts:
        return pd.DataFrame(columns=["deal_channel", "deal_sub_channel", "week", "segment",
                                     "mqls", "s1", "s2", "cw", "arr", "s2_amount"])
    out = parts[0]
    for o in parts[1:]:
        out = out.merge(o, on=["deal_channel", "deal_sub_channel", "week", "segment"], how="outer")
    for col in ["mqls", "s1", "s2", "cw"]:
        out[col] = pd.to_numeric(out.get(col), errors="coerce").fillna(0).astype(int)
    for col in ["arr", "s2_amount"]:
        out[col] = pd.to_numeric(out.get(col), errors="coerce").fillna(0).round(2)
    out["leads"] = 0  # leads come from manual aggregated CSV (already createdate-cohorted)
    return out


def build_pipeline() -> pd.DataFrame:
    contacts = _read_csv(RAW / "hubspot_contacts.csv")
    deals = _read_csv(RAW / "hubspot_deals.csv")
    leads_agg = _load_aggregated_leads()

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
        deals["deal_sub_channel"] = deals["deal_sub_channel"].replace("", "Unknown").apply(_label_sub_channel)
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

    # Override the (over-counted) lead column from contact_agg with aggregated
    # Lead totals from the manual Lead report.
    if "leads" in contact_agg.columns:
        contact_agg = contact_agg.drop(columns=["leads"])
    pipeline = contact_agg.merge(
        leads_agg, on=["deal_channel", "deal_sub_channel", "week", "segment"], how="outer"
    )
    for other in (s1_agg, s2_agg, cw_agg):
        pipeline = pipeline.merge(other, on=["deal_channel", "deal_sub_channel", "week", "segment"], how="outer")
    for col in ["leads", "mqls", "s1", "s2", "cw"]:
        pipeline[col] = pd.to_numeric(pipeline.get(col), errors="coerce").fillna(0).astype(int)
    for col in ["arr", "s2_amount"]:
        pipeline[col] = pd.to_numeric(pipeline.get(col), errors="coerce").fillna(0).round(2)
    pipeline["grouping"] = "event"

    # Build the parallel cohort (Create Date) view: each lead/deal lands in its own createdate week
    create_pipeline = _build_create_date_pipeline(contacts, deals)
    if not create_pipeline.empty:
        # leads come from the manual aggregated CSV (already cohorted by createdate)
        if "leads" in create_pipeline.columns:
            create_pipeline = create_pipeline.drop(columns=["leads"])
        create_pipeline = create_pipeline.merge(
            leads_agg, on=["deal_channel", "deal_sub_channel", "week", "segment"], how="outer"
        )
        for col in ["leads", "mqls", "s1", "s2", "cw"]:
            create_pipeline[col] = pd.to_numeric(create_pipeline.get(col), errors="coerce").fillna(0).astype(int)
        for col in ["arr", "s2_amount"]:
            create_pipeline[col] = pd.to_numeric(create_pipeline.get(col), errors="coerce").fillna(0).round(2)
        create_pipeline["grouping"] = "create"

    combined = pd.concat([pipeline, create_pipeline], ignore_index=True)
    combined["pulled_at"] = utc_now_iso()
    combined.to_csv(PIPELINE_OUT, index=False)
    log.info(f"Wrote {len(combined)} pipeline rows to {PIPELINE_OUT} "
             f"({(combined['grouping']=='event').sum()} event-date, "
             f"{(combined['grouping']=='create').sum()} create-date cohort)")
    return combined


def join_full_funnel(spend: pd.DataFrame, pipeline: pd.DataFrame) -> pd.DataFrame:
    """Join spend data to BOTH cohort and event-date pipeline views and emit one row per
    (deal_channel, sub, week, grouping)."""
    spend_agg = spend.groupby(["deal_channel", "deal_sub_channel", "week"], dropna=False).agg(
        impressions=("impressions", "sum"), clicks=("clicks", "sum"), spend=("spend", "sum"),
    ).reset_index() if not spend.empty else pd.DataFrame(
        columns=["deal_channel", "deal_sub_channel", "week", "impressions", "clicks", "spend"]
    )

    pipe_groups = []
    if not pipeline.empty:
        for grouping_val, sub_pipe in pipeline.groupby("grouping"):
            pa = sub_pipe.groupby(["deal_channel", "deal_sub_channel", "week"], dropna=False).agg(
                leads=("leads", "sum"), mqls=("mqls", "sum"), s1=("s1", "sum"),
                s2=("s2", "sum"), cw=("cw", "sum"), arr=("arr", "sum"),
                s2_amount=("s2_amount", "sum"),
            ).reset_index()
            # Merge spend into each grouping (spend is grouping-agnostic — same Cost goes to both)
            joined = spend_agg.merge(pa, on=["deal_channel", "deal_sub_channel", "week"], how="outer")
            joined["grouping"] = grouping_val
            pipe_groups.append(joined)
    if not pipe_groups:
        full = spend_agg.copy()
        full["grouping"] = "event"
    else:
        full = pd.concat(pipe_groups, ignore_index=True)
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
    if "grouping" not in full.columns:
        full["grouping"] = "event"
    full = full.sort_values(["grouping", "deal_channel", "deal_sub_channel", "week_dt"])
    grp = ["grouping", "deal_channel", "deal_sub_channel"]
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
