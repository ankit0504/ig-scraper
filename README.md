# Instagram Scraper

Scrape Instagram follower profiles and post engagement data via [Apify](https://apify.com/).

Two scripts:
- **`apify_to_report.py`** — enrich follower profiles from an Instagram data export
- **`post_engagers.py`** — scrape likers and commenters for specific posts

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

## Follower Enrichment (`apify_to_report.py`)

Only makes Apify API calls for **new followers** — profiles already in output files from previous runs are skipped automatically.

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

### Recover an interrupted run

```bash
python apify_to_report.py recover --target chuckforqueens --run-id <apify_run_id>
```

If the script was interrupted (Ctrl+C, lost connection), the Apify run may still complete on their servers. Use the run ID from the logs to fetch results without paying again. On re-run, pending runs are also detected and recovered automatically.

### Re-run analysis

```bash
python apify_to_report.py analyze --target chuckforqueens
```

Regenerates reports from the existing profiles CSV without making any API calls.

## Post Engagement (`post_engagers.py`)

Scrape likers and commenters for any public Instagram post.

### Scrape a single post

```bash
python post_engagers.py scrape --post https://www.instagram.com/p/DWH7GlqjUR5/
```

This will:
1. Fetch all likers via `patient_discovery/instagram-likes`
2. Fetch all commenters via `apify/instagram-comment-scraper`
3. Merge into a single CSV with `liked`/`commented` flags
4. Print a summary showing who liked, commented, or both

### Scrape multiple posts

```bash
python post_engagers.py scrape --posts-file posts.txt
```

Where `posts.txt` has one Instagram post URL per line.

### Re-run analysis

```bash
python post_engagers.py analyze --post https://www.instagram.com/p/DWH7GlqjUR5/
```

Regenerates the CSV from existing raw data without making API calls.

### Post engagement output files

| File | Description |
|------|-------------|
| `post_<shortcode>_engagers_raw.json` | Raw Apify responses (likers + commenters) |
| `post_<shortcode>_engagers.csv` | Merged engagers with `liked`, `commented`, `comment_text`, `comment_likes` columns |

Skips re-scraping if raw data already exists for a post.

## Safety checks

When no existing data files are found for a target, the script will prompt for confirmation before making any API calls. This prevents accidentally re-scraping everything if you forgot to place output files from a previous run.

## Follower enrichment output files

All output goes into `data/`:

| File | Description |
|------|-------------|
| `<target>_followers_export.json` | Parsed followers from IG export (input) |
| `<target>_following_export.json` | Parsed following list from IG export (used for mutual follow detection) |
| `<target>_apify_profiles_raw.json` | Raw Apify API responses (used for resume/dedup) |
| `<target>_profiles_export.csv` | Enriched profiles — merged from all sources, with `status` column |
| `<target>_failed_enrichments.txt` | Usernames that failed enrichment (skipped on re-runs) |
| `<target>_pending_run.json` | Temporary file tracking in-progress Apify runs (auto-cleaned) |
| `<target>_reports/` | Analysis report CSVs |

### Follower reports generated

- **all_followers.csv** — full list sorted by follower count
- **noteworthy_accounts.csv** — verified or 5k+ followers
- **local_collaborators.csv** — Queens/NYC keywords in bio
- **large_followings.csv** — 25k+ followers
- **business_accounts.csv** — business/professional accounts
- **follower_growth.csv** — monthly follower growth timeline
- **mutual_follows.csv** — followers you also follow back
- **not_following_back.csv** — followers you don't follow back
- **unfollowers.csv** — profiles from previous runs no longer in the current export

## Unfollower detection

Each profile in the CSV has a `status` column (`following` or `unfollowed`). When you download a new Instagram export and re-run, anyone missing from the new export is marked as `unfollowed` and appears in the unfollowers report. They remain in the CSV for historical reference but are excluded from other reports.

## Resume support

The script saves progress after each batch. If interrupted, just re-run the same command — it picks up where it left off. Apify runs that were started but not fetched are automatically recovered.
