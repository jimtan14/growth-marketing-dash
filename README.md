# growth-marketing-dash

End-to-end weekly pipeline-reporting system: pulls spend from 8 ad platforms, joins to HubSpot pipeline + HockeyStack attribution, computes full-funnel metrics with WoW/MoM deltas, flags red/yellow/green alerts, and outputs an HTML dashboard, a markdown weekly report, and a Slack notification.

## Quickstart

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in your credentials
python run.py
```

## Project layout

```
pipeline-reporting/
├── run.py                  # orchestrator: runs all 12 steps with timing + isolated error handling
├── .env.example            # all required credentials
├── requirements.txt
├── data/
│   ├── manual/             # drop CSVs here (filename stem becomes source: vibe, g2, community, influencer, chatgpt)
│   ├── raw/                # ad-platform pulls land here (gitignored)
│   └── processed/          # joined / metric-decorated outputs (gitignored)
├── output/
│   ├── dashboard.html      # static dashboard refreshed each run
│   └── weekly_report.md    # markdown summary
└── scripts/
    ├── _common.py          # paths, env loading, logging
    ├── pull_google_ads.py
    ├── pull_linkedin_ads.py
    ├── pull_meta_ads.py
    ├── pull_twitter_ads.py
    ├── pull_bing_ads.py
    ├── pull_tiktok_ads.py
    ├── pull_hubspot.py
    ├── pull_hockeystack.py
    ├── ingest_manual.py
    ├── transform.py        # standardize → join → 13 funnel metrics → WoW + MoM deltas
    ├── flag.py             # red/yellow/green alert rules → alerts.json
    └── output.py           # dashboard refresh + markdown + Slack POST
```

## Channel coverage

| Source              | Script                  | Channel name in reports |
| ------------------- | ----------------------- | ----------------------- |
| Google Ads          | `pull_google_ads.py`    | Google Search           |
| LinkedIn Ads        | `pull_linkedin_ads.py`  | LinkedIn                |
| Meta Ads            | `pull_meta_ads.py`      | Meta                    |
| X / Twitter Ads     | `pull_twitter_ads.py`   | Twitter                 |
| Bing (Microsoft)    | `pull_bing_ads.py`      | Bing                    |
| TikTok              | `pull_tiktok_ads.py`    | TikTok                  |
| Vibe CTV (manual)   | `ingest_manual.py`      | Vibe CTV                |
| G2 (manual)         | `ingest_manual.py`      | G2                      |
| Community (manual)  | `ingest_manual.py`      | Community               |
| Influencer (manual) | `ingest_manual.py`      | Influencer              |
| ChatGPT (manual)    | `ingest_manual.py`      | ChatGPT                 |
| HubSpot CRM         | `pull_hubspot.py`       | (pipeline data)         |
| HockeyStack         | `pull_hockeystack.py`   | (attribution data)      |

## Notes

- `twitter-ads` SDK is unmaintained on modern Python — `pull_twitter_ads.py` uses the X Ads REST API via `requests` + OAuth1.
- `linkedin-api` package targets member endpoints, not the Marketing API — `pull_linkedin_ads.py` uses the REST `/rest/adAnalytics` endpoint directly.
- All pull scripts log credential-missing errors and exit cleanly so one failed source never blocks the rest of the pipeline.
- All currency rounded to 2 decimals; all rate metrics stored as floats in [0, 1]; every CSV carries a `pulled_at` UTC timestamp.
