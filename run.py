"""Entry point: run the full reporting pipeline."""
from __future__ import annotations

import importlib
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "scripts"))

from _common import OUTPUT, PROCESSED, get_logger  # noqa: E402

log = get_logger("run")

STEPS = [
    "pull_google_ads",
    "pull_linkedin_ads",
    "pull_meta_ads",
    "pull_twitter_ads",
    "pull_bing_ads",
    "pull_tiktok_ads",
    "pull_hockeystack",
    "pull_hubspot",
    "ingest_manual",
    "transform",
    "flag",
    "output",
]


def main():
    succeeded = []
    failed = []
    for name in STEPS:
        log.info(f"=== START {name} ===")
        t0 = time.time()
        try:
            mod = importlib.import_module(name)
            ok = mod.run()
        except Exception as e:
            log.error(f"{name} crashed: {e}", exc_info=True)
            ok = False
        dur = time.time() - t0
        status = "OK" if ok else "FAIL"
        log.info(f"=== END   {name} [{status}] in {dur:.2f}s ===")
        (succeeded if ok else failed).append(name)

    log.info("")
    log.info("===== PIPELINE SUMMARY =====")
    log.info(f"Succeeded ({len(succeeded)}): {', '.join(succeeded) or 'none'}")
    log.info(f"Failed    ({len(failed)}): {', '.join(failed) or 'none'}")
    log.info(f"Outputs:")
    log.info(f"  - {PROCESSED / 'full_funnel_with_deltas.csv'}")
    log.info(f"  - {PROCESSED / 'alerts.json'}")
    log.info(f"  - {OUTPUT / 'dashboard.html'}")
    log.info(f"  - {OUTPUT / 'weekly_report.md'}")


if __name__ == "__main__":
    main()
