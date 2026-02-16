"""
Scrape Instagram profiles via Apify, convert to CSV, and generate reports.

Setup:
    export APIFY_TOKEN=your_token_here

Usage:
    # Scrape profiles for all followers in the export, then analyze
    python apify_to_report.py scrape --target chuckforqueens

    # Scrape from a plain text file (one username per line)
    python apify_to_report.py scrape --target chuckforqueens --usernames followers_all.txt

    # Convert an existing Apify JSON to reports (no scraping)
    python apify_to_report.py convert --input data/apify-data-combined.json --target chuckforqueens
"""

import argparse
import csv
import json
import os
import sys
from pathlib import Path

from apify_client import ApifyClient

DATA_DIR = Path("data")

PROFILE_ACTOR = "apify/instagram-profile-scraper"


def get_client() -> ApifyClient:
    token = os.environ.get("APIFY_TOKEN")
    if not token:
        print("Error: Set APIFY_TOKEN environment variable.")
        print("Get your token at https://console.apify.com/account/integrations")
        sys.exit(1)
    return ApifyClient(token)


def load_usernames(target: str, usernames_file: str | None = None) -> list[str]:
    """Load usernames to scrape from a text file or the followers export."""
    if usernames_file:
        path = Path(usernames_file)
        with open(path) as f:
            usernames = [line.strip().strip('"').strip(',').strip('"')
                         for line in f if line.strip()]
        print(f"Loaded {len(usernames)} usernames from {path}")
        return usernames

    # Fall back to the followers export
    followers_file = DATA_DIR / f"{target}_followers_export.json"
    if followers_file.exists():
        with open(followers_file) as f:
            usernames = [e["handle"] for e in json.load(f)]
        print(f"Loaded {len(usernames)} usernames from {followers_file}")
        return usernames

    print("No username source found. Provide --usernames or run export_scraper.py parse first.")
    sys.exit(1)


def load_already_scraped(target: str) -> set[str]:
    """Load usernames that have already been scraped from prior runs."""
    raw_file = DATA_DIR / f"{target}_apify_profiles_raw.json"
    if not raw_file.exists():
        return set()
    with open(raw_file) as f:
        data = json.load(f)
    scraped = {entry["username"] for entry in data if entry.get("username")}
    print(f"Already scraped: {len(scraped)} profiles (will skip)")
    return scraped


def cmd_scrape(args: argparse.Namespace) -> None:
    """Scrape profiles via Apify and generate reports."""
    DATA_DIR.mkdir(exist_ok=True)
    target = args.target
    batch_size = args.batch_size
    raw_file = DATA_DIR / f"{target}_apify_profiles_raw.json"

    client = get_client()

    # Load usernames and filter out already-scraped
    all_usernames = load_usernames(target, getattr(args, "usernames", None))
    already_scraped = load_already_scraped(target)
    remaining = [u for u in all_usernames if u not in already_scraped]

    print(f"Total: {len(all_usernames)} | Already scraped: {len(already_scraped)} | Remaining: {len(remaining)}")

    if not remaining:
        print("All profiles already scraped!")
    else:
        # Load existing raw results for appending
        existing: list[dict] = []
        if raw_file.exists():
            with open(raw_file) as f:
                existing = json.load(f)

        total_batches = (len(remaining) + batch_size - 1) // batch_size

        for i in range(0, len(remaining), batch_size):
            batch = remaining[i : i + batch_size]
            batch_num = (i // batch_size) + 1
            print(f"\n--- Batch {batch_num}/{total_batches} ({len(batch)} usernames) ---")

            try:
                run = client.actor(PROFILE_ACTOR).call(
                    run_input={
                        "usernames": batch,
                    },
                    timeout_secs=3600,
                    memory_mbytes=4096,
                )

                dataset_id = run["defaultDatasetId"]
                items = list(client.dataset(dataset_id).iterate_items())
                existing.extend(items)

                print(f"  Got {len(items)} profiles (total: {len(existing)})")

                # Save after each batch for resume support
                with open(raw_file, "w") as f:
                    json.dump(existing, f, indent=2)

            except Exception as e:
                print(f"  Batch {batch_num} failed: {e}")
                print("  Progress saved. Re-run to resume from where you left off.")
                break

    # Convert and analyze
    print("\n=== Converting to CSV ===")
    convert_apify_to_csv(raw_file, target)

    print("\n=== Running analysis ===")
    import export_scraper
    export_scraper.cmd_analyze(argparse.Namespace(target=target))


def convert_apify_to_csv(input_path: Path, target: str) -> Path:
    """Convert Apify JSON to the profiles CSV format."""
    with open(input_path) as f:
        apify_data = json.load(f)

    print(f"Loaded {len(apify_data)} profiles from {input_path}")

    # Load follow_date from followers export if available
    followers_file = DATA_DIR / f"{target}_followers_export.json"
    handle_to_date: dict[str, str] = {}
    if followers_file.exists():
        with open(followers_file) as f:
            for entry in json.load(f):
                handle_to_date[entry["handle"]] = entry.get("follow_date", "")
        print(f"Loaded follow dates for {len(handle_to_date)} followers")

    fieldnames = [
        "handle", "ig_user_id", "full_name",
        "follower_count", "following_count",
        "is_verified", "is_private",
        "is_business", "is_professional",
        "category", "bio",
        "external_url", "post_count",
        "profile_pic_url", "follow_date",
    ]

    profiles_file = DATA_DIR / f"{target}_profiles_export.csv"
    written = 0
    skipped = 0

    with open(profiles_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for entry in apify_data:
            username = entry.get("username", "")
            if not username:
                skipped += 1
                continue

            external_urls = entry.get("externalUrls") or []
            external_url = external_urls[0] if external_urls else ""
            if isinstance(external_url, dict):
                external_url = external_url.get("url", "")

            bio = (entry.get("biography") or "").replace("\n", " | ")

            row = {
                "handle": username,
                "ig_user_id": entry.get("id", ""),
                "full_name": entry.get("fullName", ""),
                "follower_count": entry.get("followersCount", 0),
                "following_count": entry.get("followsCount", 0),
                "is_verified": entry.get("verified", False),
                "is_private": entry.get("private", False),
                "is_business": entry.get("isBusinessAccount", False),
                "is_professional": entry.get("isProfessionalAccount", False),
                "category": entry.get("businessCategoryName") or "",
                "bio": bio,
                "external_url": external_url,
                "post_count": entry.get("postsCount", 0),
                "profile_pic_url": entry.get("profilePicUrlHD") or entry.get("profilePicUrl", ""),
                "follow_date": handle_to_date.get(username, ""),
            }

            writer.writerow(row)
            written += 1

    print(f"Wrote {written} profiles to {profiles_file} (skipped {skipped})")
    return profiles_file


def cmd_convert(args: argparse.Namespace) -> None:
    """Convert existing Apify JSON to reports (no scraping)."""
    input_path = Path(args.input)
    target = args.target

    convert_apify_to_csv(input_path, target)

    print("\n=== Running analysis ===")
    import export_scraper
    export_scraper.cmd_analyze(argparse.Namespace(target=target))


def main():
    parser = argparse.ArgumentParser(
        description="Scrape IG profiles via Apify and generate reports"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # scrape
    scrape_sp = subparsers.add_parser(
        "scrape", help="Scrape profiles via Apify, then convert and analyze"
    )
    scrape_sp.add_argument("--target", required=True, help="Target IG username")
    scrape_sp.add_argument(
        "--usernames",
        help="Path to a text file with one username per line (default: use followers export)",
    )
    scrape_sp.add_argument(
        "--batch-size", type=int, default=1000,
        help="Usernames per Apify run (default: 1000)",
    )

    # convert (existing JSON → reports, no scraping)
    convert_sp = subparsers.add_parser(
        "convert", help="Convert existing Apify JSON to reports"
    )
    convert_sp.add_argument("--input", required=True, help="Path to Apify JSON file")
    convert_sp.add_argument("--target", required=True, help="Target IG username")

    args = parser.parse_args()

    {"scrape": cmd_scrape, "convert": cmd_convert}[args.command](args)


if __name__ == "__main__":
    main()
