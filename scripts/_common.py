import logging
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
MANUAL = ROOT / "data" / "manual"
OUTPUT = ROOT / "output"

for p in (RAW, PROCESSED, MANUAL, OUTPUT):
    p.mkdir(parents=True, exist_ok=True)

load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def date_range(days: int = 30):
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    return start, end


def week_floor(d):
    import pandas as pd
    return pd.to_datetime(d).to_period("W-SUN").start_time.date().isoformat()


def env(name: str, default=None) -> str:
    return os.environ.get(name, default) or ""
