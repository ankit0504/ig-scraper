"""
Instagram Export Scraper — parses the official IG data export, enriches
follower profiles via the API, and generates analysis reports.

Setup:
    1. Download your data from Instagram:
       Settings → Your Activity → Download Your Information
       Choose JSON format, request "All available information" or at least
       "Followers and following".
    2. Extract the ZIP somewhere on disk.
    3. Set session cookies for API enrichment (same as ig_api_scraper.py):

       export IG_SESSION_ID=<sessionid>
       export IG_CSRF_TOKEN=<csrftoken>
       export IG_DS_USER_ID=<ds_user_id>

Usage:
    # Parse the export only (no API calls needed)
    python export_scraper.py parse --export-path ~/Downloads/instagram-export --target chuckforqueens

    # Parse + enrich with API profile data
    python export_scraper.py enrich --export-path ~/Downloads/instagram-export --target chuckforqueens

    # Generate analysis reports (after enrich)
    python export_scraper.py analyze --target chuckforqueens

    # Run all steps in sequence
    python export_scraper.py run --export-path ~/Downloads/instagram-export --target chuckforqueens
"""

import argparse
import csv
import json
import os
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

DATA_DIR = Path("data")

IG_APP_ID = "936619743392459"


# ---------------------------------------------------------------------------
# Instagram export parsing
# ---------------------------------------------------------------------------

def find_export_root(export_path: Path) -> Path:
    """Locate the root of the Instagram export directory.

    Handles both extracted directories and ZIP files. The export structure
    varies across versions, so we search for the followers file.
    """
    if export_path.suffix == ".zip":
        extract_dir = export_path.parent / export_path.stem
        if not extract_dir.exists():
            print(f"Extracting {export_path}...")
            with zipfile.ZipFile(export_path, "r") as zf:
                zf.extractall(extract_dir)
            print(f"  Extracted to {extract_dir}")
        return extract_dir

    if not export_path.is_dir():
        print(f"Error: {export_path} is not a directory or ZIP file.")
        sys.exit(1)

    return export_path


def find_follower_files(root: Path) -> list[Path]:
    """Find all follower JSON files in the export.

    Instagram splits large follower lists across multiple files:
    followers_1.json, followers_2.json, etc.
    """
    seen_paths: set[Path] = set()
    candidates = []

    # Search common paths across export format versions
    search_dirs = [
        root / "connections" / "followers_and_following",
        root / "followers_and_following",
        root,
    ]

    # Also check one level deeper in case the ZIP had a wrapper folder
    for child in root.iterdir():
        if child.is_dir():
            search_dirs.append(child / "connections" / "followers_and_following")
            search_dirs.append(child / "followers_and_following")

    for d in search_dirs:
        if not d.is_dir():
            continue
        for f in sorted(d.iterdir()):
            resolved = f.resolve()
            if f.name.startswith("followers") and f.suffix == ".json" and resolved not in seen_paths:
                seen_paths.add(resolved)
                candidates.append(f)

    return candidates


def find_following_file(root: Path) -> Path | None:
    """Find the following.json file in the export."""
    search_dirs = [
        root / "connections" / "followers_and_following",
        root / "followers_and_following",
        root,
    ]

    for child in root.iterdir():
        if child.is_dir():
            search_dirs.append(child / "connections" / "followers_and_following")
            search_dirs.append(child / "followers_and_following")

    seen: set[Path] = set()
    for d in search_dirs:
        if not d.is_dir():
            continue
        candidate = d / "following.json"
        if candidate.exists() and candidate.resolve() not in seen:
            return candidate

    return None


def parse_relationship_entries(data) -> list[dict]:
    """Parse follower/following entries from either export format.

    Format A (list):
        [{"title": "", "string_list_data": [{"value": "user", ...}]}, ...]

    Format B (object):
        {"relationships_followers": [<same as format A entries>]}
    """
    entries = []

    if isinstance(data, dict):
        # Unwrap the object — the key varies
        for key, value in data.items():
            if isinstance(value, list):
                data = value
                break
        else:
            return entries

    for item in data:
        string_data = item.get("string_list_data", [])
        if not string_data:
            continue
        for sd in string_data:
            username = sd.get("value", "").strip()
            if not username:
                href = sd.get("href", "")
                if "instagram.com/" in href:
                    username = href.rstrip("/").split("/")[-1]
            if not username:
                continue

            timestamp = sd.get("timestamp", 0)
            follow_date = ""
            if timestamp:
                follow_date = (
                    datetime.fromtimestamp(timestamp, tz=timezone.utc)
                    .strftime("%Y-%m-%d")
                )

            entries.append({
                "handle": username,
                "follow_date": follow_date,
                "timestamp": timestamp,
            })

    return entries


def parse_export(export_path: Path) -> tuple[list[dict], list[dict]]:
    """Parse followers and following from the Instagram data export.

    Returns (followers, following) as lists of dicts.
    """
    root = find_export_root(export_path)

    # --- Followers ---
    follower_files = find_follower_files(root)
    if not follower_files:
        print("Error: Could not find any followers JSON files in the export.")
        print(f"  Searched in: {root}")
        print("  Expected: connections/followers_and_following/followers_1.json")
        sys.exit(1)

    followers = []
    seen = set()
    for fpath in follower_files:
        print(f"  Parsing {fpath.name}...")
        with open(fpath, encoding="utf-8") as f:
            data = json.load(f)
        for entry in parse_relationship_entries(data):
            if entry["handle"] not in seen:
                seen.add(entry["handle"])
                followers.append(entry)

    print(f"  Found {len(followers)} unique followers")

    # --- Following ---
    following = []
    following_file = find_following_file(root)
    if following_file:
        print(f"  Parsing {following_file.name}...")
        with open(following_file, encoding="utf-8") as f:
            data = json.load(f)
        following = parse_relationship_entries(data)
        print(f"  Found {len(following)} following")
    else:
        print("  No following.json found (optional)")

    return followers, following


# ---------------------------------------------------------------------------
# Session / API (reused from ig_api_scraper.py)
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


def api_get(session: requests.Session, url: str, params: dict | None = None,
            max_retries: int = 8) -> dict:
    """GET with retry + rate-limit handling."""
    for attempt in range(max_retries):
        try:
            resp = session.get(url, params=params, timeout=30)

            if resp.status_code == 429:
                wait = min(60 * (2 ** attempt), 900)  # cap at 15 min
                print(f"    Rate limited (429). Waiting {wait}s (attempt {attempt + 1}/{max_retries})...")
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
            wait = min(30 * (2 ** attempt), 900)
            print(f"    Request error: {e}. Retrying in {wait}s...")
            time.sleep(wait)

    return {}


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_parse(args: argparse.Namespace) -> list[dict]:
    """Parse the Instagram export and save follower/following lists."""
    DATA_DIR.mkdir(exist_ok=True)
    target = args.target
    export_path = Path(args.export_path).expanduser().resolve()
    since = getattr(args, "since", None)

    print(f"Parsing Instagram export at {export_path}...")
    followers, following = parse_export(export_path)

    # Filter by --since date
    if since:
        before = len(followers)
        followers = [
            f for f in followers
            if f["follow_date"] and f["follow_date"] >= since
        ]
        print(f"  Filtered to follows since {since}: {before} → {len(followers)}")

    # Save followers
    followers_file = DATA_DIR / f"{target}_followers_export.json"
    with open(followers_file, "w") as f:
        json.dump(followers, f, indent=2)
    print(f"Saved {len(followers)} followers → {followers_file}")

    # Save following
    if following:
        following_file = DATA_DIR / f"{target}_following_export.json"
        with open(following_file, "w") as f:
            json.dump(following, f, indent=2)
        print(f"Saved {len(following)} following → {following_file}")

    # Quick stats
    if followers:
        dates = [f["follow_date"] for f in followers if f["follow_date"]]
        if dates:
            print(f"  Earliest follow: {min(dates)}")
            print(f"  Latest follow:   {max(dates)}")

    return followers


def cmd_enrich(args: argparse.Namespace) -> None:
    """Enrich parsed followers with detailed API profile data."""
    DATA_DIR.mkdir(exist_ok=True)
    target = args.target
    fast = getattr(args, "fast", False)

    followers_file = DATA_DIR / f"{target}_followers_export.json"
    profiles_file = DATA_DIR / f"{target}_profiles_export.csv"

    # Parse first if followers file doesn't exist
    if not followers_file.exists():
        if not args.export_path:
            print("No parsed followers file found. Provide --export-path or run 'parse' first.")
            sys.exit(1)
        cmd_parse(args)

    with open(followers_file) as f:
        followers = json.load(f)

    session = get_session()

    # Resume support
    enriched_ids: set[str] = set()
    existing_rows: list[dict] = []
    if profiles_file.exists():
        with open(profiles_file, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                enriched_ids.add(row["handle"])
                existing_rows.append(row)
        print(f"Resuming: {len(enriched_ids)} already enriched")

    remaining = [f for f in followers if f["handle"] not in enriched_ids]
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
        "profile_pic_url", "follow_date",
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
            follow_date = follower.get("follow_date", "")

            try:
                url = "https://www.instagram.com/api/v1/users/web_profile_info/"
                data = api_get(session, url, params={"username": username})
                user = data.get("data", {}).get("user")

                if not user:
                    errors += 1
                    continue

                row = {
                    "handle": user.get("username", username),
                    "ig_user_id": user.get("id", ""),
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
                    "follow_date": follow_date,
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

                # Pacing
                time.sleep(1.5 if fast else 4)
                if processed % (50 if fast else 40) == 0:
                    pause = 15 if fast else 45
                    print(f"  Batch pause ({pause}s)...")
                    time.sleep(pause)

            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 429:
                    print(
                        f"\n    Rate limited at {processed}/{len(remaining)}. "
                        "Backing off 15 min then continuing..."
                    )
                    time.sleep(900)
                    continue
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
    """Generate analysis reports from enriched export data."""
    import pandas as pd

    target = args.target
    profiles_file = DATA_DIR / f"{target}_profiles_export.csv"

    if not profiles_file.exists():
        print("No profiles file. Run 'enrich' first.")
        sys.exit(1)

    df = pd.read_csv(profiles_file)
    print(f"Loaded {len(df)} enriched profiles\n")

    output_dir = DATA_DIR / f"{target}_reports"
    output_dir.mkdir(exist_ok=True)

    # Load following list for cross-referencing
    following_file = DATA_DIR / f"{target}_following_export.json"
    following_handles: set[str] = set()
    if following_file.exists():
        with open(following_file) as f:
            following_handles = {e["handle"] for e in json.load(f)}
        df["is_mutual"] = df["handle"].isin(following_handles)
        print(f"Cross-referenced with {len(following_handles)} following")
    else:
        df["is_mutual"] = False

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

    # 6. Follower growth timeline (unique to export — uses follow_date)
    if "follow_date" in df.columns:
        dated = df[df["follow_date"].notna() & (df["follow_date"] != "")]
        if len(dated) > 0:
            dated = dated.copy()
            dated["follow_date"] = pd.to_datetime(dated["follow_date"])
            growth = (
                dated.groupby(dated["follow_date"].dt.to_period("M"))
                .size()
                .reset_index(name="new_followers")
            )
            growth["follow_date"] = growth["follow_date"].astype(str)
            growth["cumulative"] = growth["new_followers"].cumsum()
            growth.to_csv(output_dir / "follower_growth.csv", index=False)
            print("Follower growth by month:")
            print(growth.tail(12).to_string(index=False))
            print()

    # 7. Mutual follows (only if following data exists)
    if following_handles:
        mutuals = df[df["is_mutual"] == True].sort_values(
            "follower_count", ascending=False
        )
        mutuals.to_csv(output_dir / "mutual_follows.csv", index=False)
        not_following_back = df[df["is_mutual"] == False].sort_values(
            "follower_count", ascending=False
        )
        not_following_back.to_csv(
            output_dir / "not_following_back.csv", index=False
        )
        print(f"Mutual follows: {len(mutuals)}")
        print(f"Followers you don't follow back: {len(not_following_back)}")
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
    if following_handles:
        print(f"Mutual follows: {df['is_mutual'].sum()}")
    print(f"\nAll reports saved → {output_dir}/")


def cmd_run(args: argparse.Namespace) -> None:
    """Run all steps in sequence: parse → enrich → analyze."""
    print("=== Step 1/3: Parsing export ===")
    cmd_parse(args)

    print("\n=== Step 2/3: Enriching profiles ===")
    cmd_enrich(args)

    print("\n=== Step 3/3: Analyzing ===")
    cmd_analyze(args)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Instagram Export Scraper — parse official IG data export, "
            "enrich via API, and generate analysis reports"
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # parse
    parse_sp = subparsers.add_parser(
        "parse", help="Parse followers/following from IG data export"
    )
    parse_sp.add_argument(
        "--export-path", required=True,
        help="Path to extracted export directory or ZIP file",
    )
    parse_sp.add_argument("--target", required=True, help="Target IG username")
    parse_sp.add_argument("--since",
                          help="Only include follows on or after this date (YYYY-MM-DD)")

    # enrich
    enrich_sp = subparsers.add_parser(
        "enrich", help="Enrich parsed followers with API profile data"
    )
    enrich_sp.add_argument(
        "--export-path",
        help="Path to export (only needed if 'parse' hasn't been run yet)",
    )
    enrich_sp.add_argument("--target", required=True, help="Target IG username")
    enrich_sp.add_argument("--fast", action="store_true",
                           help="Faster pacing (1.5s/req, 15s batch pause). Higher rate-limit risk.")

    # analyze
    analyze_sp = subparsers.add_parser(
        "analyze", help="Generate analysis reports from enriched data"
    )
    analyze_sp.add_argument("--target", required=True, help="Target IG username")

    # run (all steps)
    run_sp = subparsers.add_parser(
        "run", help="Run all steps: parse → enrich → analyze"
    )
    run_sp.add_argument(
        "--export-path", required=True,
        help="Path to extracted export directory or ZIP file",
    )
    run_sp.add_argument("--target", required=True, help="Target IG username")
    run_sp.add_argument("--since",
                        help="Only include follows on or after this date (YYYY-MM-DD)")
    run_sp.add_argument("--fast", action="store_true",
                        help="Faster pacing (1.5s/req, 15s batch pause). Higher rate-limit risk.")

    args = parser.parse_args()

    {
        "parse": cmd_parse,
        "enrich": cmd_enrich,
        "analyze": cmd_analyze,
        "run": cmd_run,
    }[args.command](args)


if __name__ == "__main__":
    main()
