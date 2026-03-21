# Instagram Follower Scraper

Scrape Instagram follower profiles via [Apify](https://apify.com/) and generate analysis reports. Designed to work with an official Instagram data export as input.

Only makes Apify API calls for **new followers** — profiles already in output files from previous runs are skipped automatically.

## Prerequisites

- Python 3.10+
- An [Apify](https://console.apify.com/account/integrations) API token
- An Instagram data export (JSON format) placed in `instagram-export-<target>/`

### Getting your Instagram export

1. Go to Instagram Settings > Your Activity > Download Your Information
2. Choose **JSON** format, request at least "Followers and following"
3. Extract the ZIP into the project directory as `instagram-export-<target>/`

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

export APIFY_TOKEN=your_token_here
```

## Usage

### Scrape new followers

```bash
python apify_to_report.py scrape --target chuckforqueens
```

This will:
1. Load follower usernames from `data/chuckforqueens_followers_export.json`
2. Check what's already been processed (profiles CSV, Apify raw JSON, failed enrichments)
3. Send only new usernames to Apify in batches (default 1,000 per batch)
4. Merge new results into `data/chuckforqueens_profiles_export.csv`
5. Generate analysis reports in `data/chuckforqueens_reports/`

Options:
- `--usernames path/to/list.txt` — use a text file (one username per line) instead of the followers export
- `--batch-size 500` — change the number of usernames per Apify run (default: 1,000)

### Convert existing Apify JSON

```bash
python apify_to_report.py convert --input data/apify-data-combined.json --target chuckforqueens
```

Converts a raw Apify JSON dump into the profiles CSV and generates reports. No API calls made.

### Re-run analysis

```bash
python apify_to_report.py analyze --target chuckforqueens
```

Regenerates reports from the existing profiles CSV without making any API calls.

## Output files

All output goes into `data/`:

| File | Description |
|------|-------------|
| `<target>_followers_export.json` | Parsed followers from IG export (input) |
| `<target>_following_export.json` | Parsed following list from IG export (used for mutual follow detection) |
| `<target>_apify_profiles_raw.json` | Raw Apify API responses (used for resume/dedup) |
| `<target>_profiles_export.csv` | Enriched profiles — merged from all sources |
| `<target>_failed_enrichments.txt` | Usernames that failed enrichment (skipped on re-runs) |
| `<target>_reports/` | Analysis report CSVs |

### Reports generated

- **all_followers.csv** — full list sorted by follower count
- **noteworthy_accounts.csv** — verified or 5k+ followers
- **local_collaborators.csv** — Queens/NYC keywords in bio
- **large_followings.csv** — 25k+ followers
- **business_accounts.csv** — business/professional accounts
- **follower_growth.csv** — monthly follower growth timeline
- **mutual_follows.csv** — followers you also follow back
- **not_following_back.csv** — followers you don't follow back

## Resume support

The script saves progress after each batch. If interrupted, just re-run the same command — it picks up where it left off.
