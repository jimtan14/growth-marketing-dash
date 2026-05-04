"""Pull HubSpot contacts and deals created in the last 30 days."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import RAW, env, get_logger, utc_now_iso, date_range

log = get_logger("pull_hubspot")

CONTACTS_PATH = RAW / "hubspot_contacts.csv"
DEALS_PATH = RAW / "hubspot_deals.csv"
BASE = "https://api.hubapi.com"

CONTACT_PROPS = [
    "hs_object_id", "email", "createdate", "hs_lead_status", "lifecyclestage",
    "airops_original_traffic_source", "hs_analytics_source",
    "hs_analytics_source_data_1", "hs_analytics_source_data_2",
    "utm_source", "utm_medium", "utm_campaign",
]

DEAL_PROPS = [
    "hs_object_id", "dealname", "dealstage", "pipeline", "createdate", "closedate",
    "amount", "deal_channel", "deal_sub_channel", "segment",
]


def _headers():
    key = env("HUBSPOT_API_KEY")
    if not key:
        raise RuntimeError("HUBSPOT_API_KEY not set")
    return {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}


def _search(object_type: str, properties: list, since_iso: str, associations: list | None = None):
    url = f"{BASE}/crm/v3/objects/{object_type}/search"
    headers = _headers()
    after = None
    rows = []
    while True:
        body = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "createdate",
                    "operator": "GTE",
                    "value": since_iso,
                }]
            }],
            "properties": properties,
            "limit": 100,
        }
        if associations:
            body["associations"] = associations
        if after:
            body["after"] = after
        r = requests.post(url, headers=headers, json=body, timeout=60)
        r.raise_for_status()
        payload = r.json()
        for result in payload.get("results", []):
            row = dict(result.get("properties", {}))
            row["id"] = result.get("id")
            if associations:
                assoc = result.get("associations", {})
                contacts = assoc.get("contacts", {}).get("results", [])
                row["associated_contact_id"] = contacts[0]["id"] if contacts else None
            rows.append(row)
        paging = payload.get("paging", {}).get("next", {})
        after = paging.get("after")
        if not after:
            break
    return rows


def fetch():
    start, end = date_range(30)
    since_ms = int(pd.Timestamp(start).tz_localize("UTC").timestamp() * 1000)

    contacts = _search("contacts", CONTACT_PROPS, str(since_ms))
    deals = _search("deals", DEAL_PROPS, str(since_ms), associations=["contacts"])
    return contacts, deals


def run():
    log.info("Starting HubSpot pull")
    try:
        contacts, deals = fetch()
    except Exception as e:
        log.error(f"HubSpot pull failed: {e}")
        pd.DataFrame(columns=CONTACT_PROPS + ["id", "pulled_at"]).to_csv(CONTACTS_PATH, index=False)
        pd.DataFrame(columns=DEAL_PROPS + ["id", "associated_contact_id", "pulled_at"]).to_csv(DEALS_PATH, index=False)
        return False

    cdf = pd.DataFrame(contacts)
    ddf = pd.DataFrame(deals)
    cdf["pulled_at"] = utc_now_iso()
    ddf["pulled_at"] = utc_now_iso()
    cdf.to_csv(CONTACTS_PATH, index=False)
    ddf.to_csv(DEALS_PATH, index=False)
    log.info(f"Wrote {len(cdf)} contacts to {CONTACTS_PATH}, {len(ddf)} deals to {DEALS_PATH}")
    return True


if __name__ == "__main__":
    run()
