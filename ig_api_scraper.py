"""
Instagram API Scraper — uses Instagram's web API endpoints directly.

Collects follower data including IGUIDs for a business account.

Setup:
    1. Log into Instagram in your browser
    2. Open Developer Tools (F12) → Application → Cookies → instagram.com
    3. Copy these cookie values and set them as env vars:

       export IG_SESSION_ID=<sessionid>
       export IG_CSRF_TOKEN=<csrftoken>
       export IG_DS_USER_ID=<ds_user_id>

Usage:
    # Scrape followers (basic info + IGUIDs)
    python ig_api_scraper.py followers --target chuckforqueens

    # Enrich with detailed profile data
    python ig_api_scraper.py enrich --target chuckforqueens

    # Generate analysis reports
    python ig_api_scraper.py analyze --target chuckforqueens

    # Run all steps in sequence
    python ig_api_scraper.py run --target chuckforqueens
"""

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path

import requests

DATA_DIR = Path("data")

# Instagram web app ID (public, used by the web client)
IG_APP_ID = "936619743392459"


# ---------------------------------------------------------------------------
# Session / auth
# ---------------------------------------------------------------------------

def get_session() -> requests.Session:
    """Build an authenticated requests.Session using Instagram cookies."""
    session_id = os.environ.get("IG_SESSION_ID")
    csrf_token = os.environ.get("IG_CSRF_TOKEN")
    ds_user_id = os.environ.get("IG_DS_USER_ID")

    if not all([session_id, csrf_token, ds_user_id]):
        print("Error: Missing Instagram session cookies.")
        print()
        print("Set these environment variables:")
        print("  export IG_SESSION_ID=<your sessionid cookie>")
        print("  export IG_CSRF_TOKEN=<your csrftoken cookie>")
        print("  export IG_DS_USER_ID=<your ds_user_id cookie>")
        print()
        print("To get these values:")
        print("  1. Log into Instagram in your browser")
        print("  2. Open DevTools (F12) → Application → Cookies → instagram.com")
        print("  3. Copy the values for: sessionid, csrftoken, ds_user_id")
        sys.exit(1)

    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "X-IG-App-ID": IG_APP_ID,
        "X-CSRFToken": csrf_token,
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.instagram.com/",
        "Origin": "https://www.instagram.com",
    })
    session.cookies.set("sessionid", session_id, domain=".instagram.com")
    session.cookies.set("csrftoken", csrf_token, domain=".instagram.com")
    session.cookies.set("ds_user_id", ds_user_id, domain=".instagram.com")

    return session


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def api_get(session: requests.Session, url: str, params: dict | None = None,
            max_retries: int = 3) -> dict:
    """GET with retry + rate-limit handling."""
    for attempt in range(max_retries):
        try:
            resp = session.get(url, params=params, timeout=30)

            if resp.status_code == 429:
                wait = 60 * (2 ** attempt)
                print(f"    Rate limited (429). Waiting {wait}s...")
                time.sleep(wait)
                continue

            if resp.status_code == 401:
                print("    Session expired or invalid. Refresh your cookies.")
                sys.exit(1)

            if resp.status_code == 404:
                return {}

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise
            wait = 30 * (2 ** attempt)
            print(f"    Request error: {e}. Retrying in {wait}s...")
            time.sleep(wait)

    return {}


def resolve_user_id(session: requests.Session, username: str) -> tuple[str, dict]:
    """Resolve username → (user_id, full_profile_dict)."""
    url = "https://www.instagram.com/api/v1/users/web_profile_info/"
    data = api_get(session, url, params={"username": username})
    user = data.get("data", {}).get("user")
    if not user:
        print(f"  Could not resolve @{username}")
        sys.exit(1)
    return user["id"], user


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_followers(args: argparse.Namespace) -> list[dict]:
    """Scrape all followers — returns basic info + IGUIDs."""
    DATA_DIR.mkdir(exist_ok=True)
    session = get_session()
    target = args.target
    fast = getattr(args, "fast", False)
    followers_file = DATA_DIR / f"{target}_followers_api.json"

    # Resolve target user ID
    print(f"Resolving @{target}...")
    target_id, target_profile = resolve_user_id(session, target)
    follower_count = (
        target_profile.get("edge_followed_by", {}).get("count")
        or target_profile.get("follower_count", "?")
    )
    print(f"  @{target}  IGUID: {target_id}  Followers: {follower_count}")

    # Load existing progress
    collected: dict[str, dict] = {}
    if followers_file.exists():
        with open(followers_file) as f:
            for item in json.load(f):
                collected[str(item["ig_user_id"])] = item
        print(f"  Resuming: {len(collected)} already collected")

    # Paginate the followers endpoint
    print("Fetching followers...")
    max_id = ""
    page = 0

    while True:
        page += 1
        params: dict = {"count": 100}
        if max_id:
            params["max_id"] = max_id

        url = f"https://www.instagram.com/api/v1/friendships/{target_id}/followers/"
        data = api_get(session, url, params)

        users = data.get("users", [])
        if not users:
            break

        for u in users:
            uid = str(u.get("pk") or u.get("pk_id", ""))
            if not uid:
                continue
            collected[uid] = {
                "ig_user_id": uid,
                "handle": u.get("username", ""),
                "full_name": u.get("full_name", ""),
                "is_private": u.get("is_private", False),
                "is_verified": u.get("is_verified", False),
                "profile_pic_url": u.get("profile_pic_url", ""),
            }

        # Save after every page
        with open(followers_file, "w") as f:
            json.dump(list(collected.values()), f, indent=2)

        print(f"  Page {page}: {len(collected)} followers")

        next_id = data.get("next_max_id")
        if not next_id:
            break
        max_id = str(next_id)

        # Pacing
        time.sleep(1 if fast else 2)
        if page % 10 == 0:
            pause = 5 if fast else 15
            print(f"  Pausing {pause}s to stay under rate limits...")
            time.sleep(pause)

    result = list(collected.values())
    with open(followers_file, "w") as f:
        json.dump(result, f, indent=2)
    print(f"Saved {len(result)} followers → {followers_file}")
    return result


def cmd_enrich(args: argparse.Namespace) -> None:
    """Fetch detailed profile info for each follower."""
    DATA_DIR.mkdir(exist_ok=True)
    session = get_session()
    target = args.target
    fast = getattr(args, "fast", False)

    followers_file = DATA_DIR / f"{target}_followers_api.json"
    profiles_file = DATA_DIR / f"{target}_profiles_api.csv"

    if not followers_file.exists():
        print("No followers file. Run 'followers' first.")
        sys.exit(1)

    with open(followers_file) as f:
        followers = json.load(f)

    # Resume support
    enriched_ids: set[str] = set()
    existing_rows: list[dict] = []
    if profiles_file.exists():
        with open(profiles_file, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                enriched_ids.add(row["ig_user_id"])
                existing_rows.append(row)
        print(f"Resuming: {len(enriched_ids)} already enriched")

    remaining = [f for f in followers if str(f["ig_user_id"]) not in enriched_ids]
    print(f"{len(remaining)} to enrich out of {len(followers)} total")

    if not remaining:
        print("All profiles already enriched!")
        return

    fieldnames = [
        "handle", "ig_user_id", "full_name",
        "follower_count", "following_count",
        "is_verified", "is_private",
        "is_business", "is_professional",
        "category", "bio",
        "external_url", "post_count",
        "profile_pic_url",
    ]

    # Write header + existing rows
    with open(profiles_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in existing_rows:
            writer.writerow(row)

    processed = 0
    errors = 0

    try:
        for follower in remaining:
            username = follower["handle"]
            uid = str(follower["ig_user_id"])

            try:
                # Use web_profile_info for full details
                url = "https://www.instagram.com/api/v1/users/web_profile_info/"
                data = api_get(session, url, params={"username": username})
                user = data.get("data", {}).get("user")

                if not user:
                    errors += 1
                    continue

                row = {
                    "handle": user.get("username", username),
                    "ig_user_id": user.get("id", uid),
                    "full_name": user.get("full_name", ""),
                    "follower_count": (
                        user.get("edge_followed_by", {}).get("count", 0)
                    ),
                    "following_count": (
                        user.get("edge_follow", {}).get("count", 0)
                    ),
                    "is_verified": user.get("is_verified", False),
                    "is_private": user.get("is_private", False),
                    "is_business": user.get("is_business_account", False),
                    "is_professional": user.get(
                        "is_professional_account", False
                    ),
                    "category": (
                        user.get("category_name", "")
                        or user.get("business_category_name", "")
                        or ""
                    ),
                    "bio": (
                        (user.get("biography", "") or "")
                        .replace("\n", " | ")
                    ),
                    "external_url": user.get("external_url", "") or "",
                    "post_count": (
                        user.get("edge_owner_to_timeline_media", {})
                        .get("count", 0)
                    ),
                    "profile_pic_url": (
                        user.get("profile_pic_url_hd", "")
                        or user.get("profile_pic_url", "")
                    ),
                }

                with open(profiles_file, "a", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writerow(row)

                processed += 1
                if processed % 10 == 0:
                    print(
                        f"  {processed}/{len(remaining)} enriched "
                        f"({errors} errors)"
                    )

                # Pacing — web_profile_info is rate-limited aggressively
                time.sleep(1.5 if fast else 4)
                if processed % (50 if fast else 40) == 0:
                    pause = 15 if fast else 45
                    print(f"  Batch pause ({pause}s)...")
                    time.sleep(pause)

            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 429:
                    print(
                        f"\nRate limited at {processed}/{len(remaining)}. "
                        "Wait ~15 min and re-run to resume."
                    )
                    break
                errors += 1
                time.sleep(5)
            except Exception:
                errors += 1
                time.sleep(5)

    except KeyboardInterrupt:
        print("\nInterrupted — progress saved.")

    total = len(enriched_ids) + processed
    print(
        f"Done. {total}/{len(followers)} enriched ({errors} errors). "
        f"Data → {profiles_file}"
    )


def cmd_analyze(args: argparse.Namespace) -> None:
    """Generate analysis reports from enriched CSV."""
    import pandas as pd

    target = args.target
    profiles_file = DATA_DIR / f"{target}_profiles_api.csv"

    if not profiles_file.exists():
        print("No profiles file. Run 'enrich' first.")
        sys.exit(1)

    df = pd.read_csv(profiles_file)
    print(f"Loaded {len(df)} enriched profiles\n")

    output_dir = DATA_DIR / f"{target}_reports"
    output_dir.mkdir(exist_ok=True)

    # 1. All followers (full dump)
    df.to_csv(output_dir / "all_followers.csv", index=False)

    # 2. Noteworthy accounts (verified OR 5k+ followers)
    noteworthy = df[
        (df["is_verified"] == True) | (df["follower_count"] >= 5000)
    ].sort_values("follower_count", ascending=False)

    noteworthy.to_csv(output_dir / "noteworthy_accounts.csv", index=False)
    print(f"Noteworthy (verified or 5k+ followers): {len(noteworthy)}")
    if len(noteworthy) > 0:
        cols = ["handle", "ig_user_id", "full_name", "follower_count",
                "is_verified"]
        print(noteworthy[cols].head(20).to_string(index=False))
    print()

    # 3. Local collaborators (Queens / NYC keywords in bio)
    local_keywords = [
        "queens", "nyc", "new york", "flushing", "jamaica", "astoria",
        "jackson heights", "long island city", "lic", "woodside",
        "elmhurst", "corona", "forest hills", "rego park", "bayside",
        "fresh meadows", "whitestone", "sunnyside", "ridgewood",
        "maspeth", "middle village", "kew gardens", "howard beach",
        "ozone park", "south ozone", "richmond hill", "woodhaven",
        "rockaways", "rockaway", "far rockaway", "broad channel",
        "queens ny", "qns", "district 25", "district 19", "cd25",
    ]
    bio_lower = df["bio"].fillna("").str.lower()
    local_mask = bio_lower.apply(
        lambda bio: any(kw in bio for kw in local_keywords)
    )
    local = df[local_mask].sort_values("follower_count", ascending=False)

    local.to_csv(output_dir / "local_collaborators.csv", index=False)
    print(f"Local collaborators (Queens/NYC in bio): {len(local)}")
    if len(local) > 0:
        cols = ["handle", "ig_user_id", "full_name", "follower_count", "bio"]
        print(local[cols].head(20).to_string(index=False))
    print()

    # 4. Large followings (25k+)
    large = df[df["follower_count"] >= 25000].sort_values(
        "follower_count", ascending=False
    )
    large.to_csv(output_dir / "large_followings.csv", index=False)
    print(f"Large followings (25k+): {len(large)}")
    if len(large) > 0:
        cols = ["handle", "ig_user_id", "full_name", "follower_count",
                "is_verified"]
        print(large[cols].head(20).to_string(index=False))
    print()

    # 5. Business / Professional accounts
    biz_mask = (df["is_business"] == True) | (df["is_professional"] == True)
    business = df[biz_mask].sort_values("follower_count", ascending=False)
    business.to_csv(output_dir / "business_accounts.csv", index=False)
    print(f"Business/Professional accounts: {len(business)}")
    print()

    # --- Summary ---
    print("=== Summary ===")
    print(f"Total followers: {len(df)}")
    print(f"Verified: {df['is_verified'].sum()}")
    biz_count = biz_mask.sum() if biz_mask.any() else 0
    print(f"Business/Professional: {biz_count}")
    print(f"Private: {df['is_private'].sum()}")
    print(f"Median follower count: {df['follower_count'].median():.0f}")
    print(f"Mean follower count: {df['follower_count'].mean():.0f}")
    print(f"\nAll reports saved → {output_dir}/")


def cmd_run(args: argparse.Namespace) -> None:
    """Run all steps in sequence."""
    print("=== Step 1/3: Scraping followers ===")
    cmd_followers(args)

    print("\n=== Step 2/3: Enriching profiles ===")
    cmd_enrich(args)

    print("\n=== Step 3/3: Analyzing ===")
    cmd_analyze(args)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Instagram API Scraper — collect follower data with IGUIDs"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    for cmd_name, help_text in [
        ("followers", "Scrape followers (basic info + IGUIDs)"),
        ("enrich", "Enrich profiles with full details"),
        ("analyze", "Generate analysis reports (CSVs)"),
        ("run", "Run all steps: followers → enrich → analyze"),
    ]:
        sp = subparsers.add_parser(cmd_name, help=help_text)
        sp.add_argument(
            "--target", required=True, help="Target IG username (no @)"
        )
        if cmd_name in ("followers", "enrich", "run"):
            sp.add_argument(
                "--fast", action="store_true",
                help="Faster pacing (higher rate-limit risk, but ~3x speed)",
            )

    args = parser.parse_args()

    {
        "followers": cmd_followers,
        "enrich": cmd_enrich,
        "analyze": cmd_analyze,
        "run": cmd_run,
    }[args.command](args)


if __name__ == "__main__":
    main()
