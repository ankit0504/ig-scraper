# Instagram Follower Scraper & Analyzer

Collects follower data (including IGUIDs) for an Instagram account and generates analysis reports — noteworthy accounts, local collaborators (Queens/NYC), large followings, business accounts, and more.

There are four scripts, each taking a different approach to the same goal. Pick whichever one matches your access level and setup.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Getting Instagram Session Cookies](#getting-instagram-session-cookies)
- [Scripts](#scripts)
  - [1. export_scraper.py — Official IG Data Export + API](#1-export_scraperpy--official-ig-data-export--api)
  - [2. ig_api_scraper.py — Direct Instagram API](#2-ig_api_scraperpy--direct-instagram-api)
  - [3. scrape_followers.py — Instaloader Library](#3-scrape_followerspy--instaloader-library)
  - [4. apify_scrape.py — Apify Cloud Service](#4-apify_scrapepy--apify-cloud-service)
- [Output Reports](#output-reports)
- [Rate Limits & Resuming](#rate-limits--resuming)

## Prerequisites

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

The `requirements.txt` includes:
- `instaloader` — for `scrape_followers.py`
- `pandas` — for report generation (all scripts)
- `requests` — for direct API calls (`ig_api_scraper.py`, `export_scraper.py`)

For the Apify script you also need:
```bash
pip install apify-client
```

## Getting Instagram Session Cookies

Scripts 1 (`export_scraper.py`) and 2 (`ig_api_scraper.py`) authenticate using browser session cookies. Here's how to get them:

1. Open your browser and log into [instagram.com](https://www.instagram.com)
2. Open Developer Tools:
   - **Chrome/Edge**: Press `F12` or `Cmd+Option+I` (Mac) / `Ctrl+Shift+I` (Windows)
   - **Firefox**: Press `F12` or `Cmd+Option+I`
   - **Safari**: Enable Developer menu in Preferences → Advanced, then `Cmd+Option+I`
3. Go to the **Application** tab (Chrome/Edge) or **Storage** tab (Firefox)
4. In the left sidebar, expand **Cookies** → click **https://www.instagram.com**
5. Find and copy these three cookie values:

| Cookie Name  | Environment Variable |
|-------------|---------------------|
| `sessionid`  | `IG_SESSION_ID`     |
| `csrftoken`  | `IG_CSRF_TOKEN`     |
| `ds_user_id` | `IG_DS_USER_ID`     |

6. Set them in your terminal:

```bash
export IG_SESSION_ID=<your sessionid value>
export IG_CSRF_TOKEN=<your csrftoken value>
export IG_DS_USER_ID=<your ds_user_id value>
```

**Note**: Session cookies expire. If you get a 401 error, refresh instagram.com in your browser and grab new cookie values.

## Scripts

### 1. `export_scraper.py` — Official IG Data Export + API

**Best for**: When you have admin access to the target account.

This is the recommended approach. It parses the official Instagram data export to get the complete follower list (no API pagination needed), then enriches each follower's profile via the API.

**Why this is the best option**: The official export gives you the authoritative, complete follower list without any risk of missing followers due to pagination bugs or rate limits. It also includes follow timestamps, which none of the other scripts can get. The API is only used for enrichment (profile details), not for the follower list itself.

**Step 1**: Download your data from Instagram:
- Go to Settings → Your Activity → Download Your Information
- Choose **JSON** format
- Request at least "Followers and following"
- Wait for the export email, download the ZIP, and extract it

**Step 2**: Set session cookies (see [Getting Instagram Session Cookies](#getting-instagram-session-cookies))

**Step 3**: Run it:

```bash
# Parse only (no API calls needed)
python export_scraper.py parse --export-path ~/Downloads/instagram-export --target ACCOUNT_NAME

# Parse + enrich + analyze (all steps)
python export_scraper.py run --export-path ~/Downloads/instagram-export --target ACCOUNT_NAME

# Or run steps individually:
python export_scraper.py enrich --target ACCOUNT_NAME
python export_scraper.py analyze --target ACCOUNT_NAME
```

**Unique features**:
- Follower growth timeline (by month) from follow-date timestamps
- Mutual follow detection (cross-references followers vs. following)
- Accepts ZIP files or extracted directories

---

### 2. `ig_api_scraper.py` — Direct Instagram API

**Best for**: When you don't have the data export but do have a logged-in session.

Hits Instagram's internal `/api/v1/` endpoints directly to paginate through all followers, then enriches each with detailed profile info.

**Important**: This does **not** use an official Meta developer API key. There is no public Instagram API that lets you enumerate another account's followers or look up arbitrary user profiles. The [Instagram Graph API](https://developers.facebook.com/docs/instagram-api/) (the official one with API keys) only provides insights for your own business account. This script uses the same internal endpoints that the Instagram web app uses, authenticated with browser session cookies — so it is subject to the same rate limits as `export_scraper.py`.

**Setup**: Set session cookies (see [Getting Instagram Session Cookies](#getting-instagram-session-cookies))

```bash
# Run all steps (followers → enrich → analyze)
python ig_api_scraper.py run --target ACCOUNT_NAME

# Or individually:
python ig_api_scraper.py followers --target ACCOUNT_NAME
python ig_api_scraper.py enrich --target ACCOUNT_NAME
python ig_api_scraper.py analyze --target ACCOUNT_NAME
```

---

### 3. `scrape_followers.py` — Instaloader Library

**Best for**: Backup option if the direct API approach stops working.

Uses the [instaloader](https://instaloader.github.io/) Python library, which internally calls Instagram's GraphQL API.

**Why this doesn't fully work on its own**: Instagram has progressively locked down their GraphQL endpoints. Instaloader frequently hits `QueryReturnedBadRequestException` and `429 Too Many Requests` errors, especially when:
- Scraping follower lists of accounts with more than a few hundred followers
- Enriching profiles one-by-one (each profile lookup is a separate GraphQL query)
- Running without long delays between requests

The rate limits are aggressive and unpredictable. A full scrape of an account with ~1,000 followers can require multiple sessions spread across hours or days, with frequent 15-30 minute forced pauses. The direct API scripts (`ig_api_scraper.py` and `export_scraper.py`) use Instagram's REST-style endpoints which tend to have more generous rate limits.

**Setup**:

```bash
# Login (one-time, saves session to data/)
python scrape_followers.py login --username YOUR_IG_USERNAME

# Collect followers
python scrape_followers.py followers --username YOUR_IG_USERNAME --target ACCOUNT_NAME

# Enrich profiles
python scrape_followers.py enrich --username YOUR_IG_USERNAME --target ACCOUNT_NAME

# Analyze
python scrape_followers.py analyze --target ACCOUNT_NAME
```

---

### 4. `apify_scrape.py` — Apify Cloud Service (paid, not fully working)

**Status**: This script was an experiment and is **not fully working**. The Apify actors it depends on have unreliable output formats and the service costs money per run.

**What it does**: Uses [Apify](https://apify.com) cloud actors to do the scraping server-side. This is a **paid service** — each scrape run consumes Apify compute units that cost money. The free tier is limited.

**Unique feature**: This is the only script that also scrapes **post comments** and identifies top commenters, commenters who aren't followers, etc.

**Known issues**:
- Apify actor field names are inconsistent and change between versions, so the normalization logic may need updating
- The follower scraper actor may not return complete results for large accounts
- Cost adds up quickly for accounts with many posts/comments

**Setup**:
1. Create an account at [apify.com](https://apify.com)
2. Get your API token from the [integrations page](https://console.apify.com/account/integrations)
3. Install the client: `pip install apify-client`
4. Set the token:
   ```bash
   export APIFY_TOKEN=your_token_here
   ```

```bash
# Run everything (followers + posts + comments + analyze)
python apify_scrape.py run --target ACCOUNT_NAME

# Or individually:
python apify_scrape.py followers --target ACCOUNT_NAME
python apify_scrape.py posts --target ACCOUNT_NAME
python apify_scrape.py comments --target ACCOUNT_NAME
python apify_scrape.py analyze --target ACCOUNT_NAME
```

## Output Reports

All scripts generate reports in `data/ACCOUNT_NAME_reports/`:

| Report | Description |
|--------|-------------|
| `all_followers.csv` | Complete follower list with all profile data |
| `noteworthy_accounts.csv` | Verified accounts or those with 5,000+ followers |
| `local_collaborators.csv` | Followers with Queens/NYC keywords in their bio |
| `large_followings.csv` | Followers with 10,000+ followers |
| `business_accounts.csv` | Business and professional accounts |
| `follower_growth.csv` | Monthly follower growth timeline (export_scraper only) |
| `mutual_follows.csv` | Followers you also follow back (export_scraper only) |
| `not_following_back.csv` | Followers you don't follow back (export_scraper only) |
| `top_commenters.csv` | Most active commenters (apify_scrape only) |
| `commenters_not_following.csv` | Commenters who don't follow the account (apify_scrape only) |

## Rate Limits & Resuming

All scripts save progress incrementally and can resume where they left off:

- **If rate limited**: Wait 15-30 minutes, then re-run the same command. It will skip already-processed profiles.
- **If interrupted** (Ctrl+C): Progress is saved. Just re-run.
- **Session expired** (401 error): Refresh instagram.com in your browser, grab new cookie values, re-export the environment variables, and re-run.

The enrichment step is the bottleneck — each profile lookup requires its own API call with a delay between requests to stay under rate limits. For an account with 1,000 followers, expect the enrichment to run for roughly an hour.
