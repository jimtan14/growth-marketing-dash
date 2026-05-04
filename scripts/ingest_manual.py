"""Read all manually-dropped CSVs in /data/manual/ and combine them."""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import RAW, MANUAL, get_logger, utc_now_iso, week_floor

log = get_logger("ingest_manual")

OUTPUT_PATH = RAW / "manual_channels.csv"
VALID_SOURCES = {"vibe", "g2", "community", "influencer", "chatgpt"}

COLUMN_ALIASES = {
    "campaign": "campaign_name",
    "campaign name": "campaign_name",
    "campaign_name": "campaign_name",
    "imps": "impressions",
    "impressions": "impressions",
    "clicks": "clicks",
    "spend": "spend",
    "cost": "spend",
    "date": "date",
    "day": "date",
    "source": "source",
}


def _source_from_filename(name: str) -> str:
    stem = Path(name).stem.lower()
    stem = re.sub(r"[_\-]?\d{4}[_\-]?\d{2}[_\-]?\d{2}.*$", "", stem)
    stem = re.sub(r"[_\-]?\d{6,8}$", "", stem)
    return stem.strip("_-")


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={c: COLUMN_ALIASES.get(c.strip().lower(), c.strip().lower()) for c in df.columns})
    for col in ["campaign_name", "impressions", "clicks", "spend", "date"]:
        if col not in df.columns:
            df[col] = None
    return df


def run():
    log.info("Starting manual channel ingest")
    files = sorted(MANUAL.glob("*.csv"))
    if not files:
        log.warning(f"No manual CSVs found in {MANUAL}")
        pd.DataFrame(columns=["source", "campaign_name", "impressions", "clicks",
                              "spend", "date", "week", "pulled_at"]).to_csv(OUTPUT_PATH, index=False)
        return True

    frames = []
    for f in files:
        try:
            df = pd.read_csv(f)
            df = _normalize(df)
            if "source" not in df.columns or df["source"].isna().all():
                df["source"] = _source_from_filename(f.name)
            df["source"] = df["source"].astype(str).str.lower()
            df = df[df["source"].isin(VALID_SOURCES)]
            df["impressions"] = pd.to_numeric(df["impressions"], errors="coerce").fillna(0).astype(int)
            df["clicks"] = pd.to_numeric(df["clicks"], errors="coerce").fillna(0).astype(int)
            df["spend"] = pd.to_numeric(df["spend"], errors="coerce").fillna(0).round(2)
            frames.append(df[["source", "campaign_name", "impressions", "clicks", "spend", "date"]])
        except Exception as e:
            log.error(f"Failed to read {f.name}: {e}")
            continue

    if not frames:
        out = pd.DataFrame(columns=["source", "campaign_name", "impressions", "clicks",
                                    "spend", "date", "week", "pulled_at"])
    else:
        out = pd.concat(frames, ignore_index=True)
        out["week"] = out["date"].apply(week_floor)
    out["pulled_at"] = utc_now_iso()
    out.to_csv(OUTPUT_PATH, index=False)
    log.info(f"Wrote {len(out)} rows from {len(files)} files to {OUTPUT_PATH}")
    return True


if __name__ == "__main__":
    run()
