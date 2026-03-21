"""
Scrape likers and commenters for Instagram posts via Apify.

Setup:
    export APIFY_TOKEN=your_token_here

Usage:
    # Scrape engagers for a single post
    python post_engagers.py scrape --post https://www.instagram.com/p/DWH7GlqjUR5/

    # Scrape engagers for multiple posts
    python post_engagers.py scrape --posts-file posts.txt

    # Re-run analysis on existing data
    python post_engagers.py analyze --post https://www.instagram.com/p/DWH7GlqjUR5/
"""

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path

from apify_client import ApifyClient

DATA_DIR = Path("data")

LIKERS_ACTOR = "patient_discovery/instagram-likes"
COMMENTS_ACTOR = "apify/instagram-comment-scraper"


def get_client() -> ApifyClient:
    token = os.environ.get("APIFY_TOKEN")
    if not token:
        print("Error: Set APIFY_TOKEN environment variable.")
        print("Get your token at https://console.apify.com/account/integrations")
        sys.exit(1)
    return ApifyClient(token)


def shortcode_from_url(url: str) -> str:
    """Extract shortcode from an Instagram post URL."""
    return url.rstrip("/").split("/")[-1]


def output_prefix(post_url: str) -> str:
    """Generate a file prefix from a post shortcode."""
    return f"post_{shortcode_from_url(post_url)}"


def scrape_likers(client: ApifyClient, post_url: str) -> list[dict]:
    """Fetch all likers for a post."""
    shortcode = shortcode_from_url(post_url)
    print(f"Fetching likers (shortcode: {shortcode})...")

    run = client.actor(LIKERS_ACTOR).start(
        run_input={"postUrls": [post_url], "postCode": shortcode},
        memory_mbytes=4096,
    )
    run_id = run["id"]
    dataset_id = run["defaultDatasetId"]

    print(f"  Started run {run_id} — waiting...")
    while True:
        run_info = client.run(run_id).get()
        status = run_info["status"]
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            break
        time.sleep(3)
    print(f"  Run finished: {status}")

    items = list(client.dataset(dataset_id).iterate_items())
    print(f"  Got {len(items)} likers")
    return items


def scrape_commenters(client: ApifyClient, post_url: str) -> list[dict]:
    """Fetch all commenters for a post."""
    print(f"Fetching commenters...")

    run = client.actor(COMMENTS_ACTOR).start(
        run_input={"directUrls": [post_url]},
        memory_mbytes=4096,
    )
    run_id = run["id"]
    dataset_id = run["defaultDatasetId"]

    print(f"  Started run {run_id} — waiting...")
    while True:
        run_info = client.run(run_id).get()
        status = run_info["status"]
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            break
        time.sleep(3)
    print(f"  Run finished: {status}")

    items = list(client.dataset(dataset_id).iterate_items())
    print(f"  Got {len(items)} comments")
    return items


def scrape_post(client: ApifyClient, post_url: str) -> dict:
    """Scrape likers and commenters for a single post, save raw data."""
    DATA_DIR.mkdir(exist_ok=True)
    prefix = output_prefix(post_url)
    raw_file = DATA_DIR / f"{prefix}_engagers_raw.json"

    # Load existing data to avoid re-scraping
    existing: dict = {}
    if raw_file.exists():
        with open(raw_file) as f:
            existing = json.load(f)
        print(f"Found existing data: {len(existing.get('likers', []))} likers, {len(existing.get('commenters', []))} commenters")

    # Scrape likers if not already done
    if existing.get("likers"):
        print(f"Skipping likers (already have {len(existing['likers'])})")
        likers = existing["likers"]
    else:
        likers = scrape_likers(client, post_url)

    # Scrape commenters if not already done
    if existing.get("commenters"):
        print(f"Skipping commenters (already have {len(existing['commenters'])})")
        commenters = existing["commenters"]
    else:
        commenters = scrape_commenters(client, post_url)

    # Save raw data
    raw_data = {
        "post_url": post_url,
        "shortcode": shortcode_from_url(post_url),
        "likers": likers,
        "commenters": commenters,
    }
    with open(raw_file, "w") as f:
        json.dump(raw_data, f, indent=2)
    print(f"\nRaw data saved to {raw_file}")

    return raw_data


def build_engagers_csv(raw_data: dict) -> Path:
    """Build a unified CSV of all engagers from raw data."""
    prefix = output_prefix(raw_data["post_url"])
    csv_file = DATA_DIR / f"{prefix}_engagers.csv"

    # Build unified engager map
    engagers: dict[str, dict] = {}

    for liker in raw_data["likers"]:
        username = liker.get("username", "")
        if not username:
            continue
        engagers[username] = {
            "username": username,
            "ig_user_id": liker.get("id", ""),
            "full_name": liker.get("full_name", ""),
            "is_verified": liker.get("is_verified", False),
            "is_private": liker.get("is_private", False),
            "liked": True,
            "commented": False,
            "comment_text": "",
            "comment_likes": 0,
        }

    for comment in raw_data["commenters"]:
        username = comment.get("ownerUsername", "")
        if not username:
            continue
        comment_text = comment.get("text", "")
        comment_likes = comment.get("likesCount", 0)

        if username in engagers:
            engagers[username]["commented"] = True
            engagers[username]["comment_text"] = comment_text
            engagers[username]["comment_likes"] = comment_likes
        else:
            owner = comment.get("owner", {})
            engagers[username] = {
                "username": username,
                "ig_user_id": owner.get("id", ""),
                "full_name": owner.get("full_name", ""),
                "is_verified": owner.get("is_verified", False),
                "is_private": owner.get("is_private", False),
                "liked": False,
                "commented": True,
                "comment_text": comment_text,
                "comment_likes": comment_likes,
            }

    fieldnames = [
        "username", "ig_user_id", "full_name",
        "is_verified", "is_private",
        "liked", "commented",
        "comment_text", "comment_likes",
    ]

    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in engagers.values():
            writer.writerow(row)

    # Print summary
    likers_count = sum(1 for e in engagers.values() if e["liked"])
    commenters_count = sum(1 for e in engagers.values() if e["commented"])
    both_count = sum(1 for e in engagers.values() if e["liked"] and e["commented"])
    comment_only = sum(1 for e in engagers.values() if e["commented"] and not e["liked"])

    print(f"\n=== Engagers Summary ===")
    print(f"Total unique engagers: {len(engagers)}")
    print(f"Likers: {likers_count}")
    print(f"Commenters: {commenters_count}")
    print(f"Liked + commented: {both_count}")
    print(f"Commented but didn't like: {comment_only}")
    print(f"\nSaved to {csv_file}")

    return csv_file


def cmd_scrape(args: argparse.Namespace) -> None:
    """Scrape engagers for one or more posts."""
    client = get_client()

    post_urls: list[str] = []
    if args.post:
        post_urls.append(args.post)
    if args.posts_file:
        with open(args.posts_file) as f:
            post_urls.extend(line.strip() for line in f if line.strip())

    if not post_urls:
        print("Provide --post or --posts-file")
        sys.exit(1)

    for post_url in post_urls:
        print(f"\n{'='*60}")
        print(f"Post: {post_url}")
        print(f"{'='*60}")
        raw_data = scrape_post(client, post_url)
        build_engagers_csv(raw_data)


def cmd_analyze(args: argparse.Namespace) -> None:
    """Re-run analysis on existing raw data."""
    post_url = args.post
    prefix = output_prefix(post_url)
    raw_file = DATA_DIR / f"{prefix}_engagers_raw.json"

    if not raw_file.exists():
        print(f"No raw data found at {raw_file}. Run 'scrape' first.")
        sys.exit(1)

    with open(raw_file) as f:
        raw_data = json.load(f)

    build_engagers_csv(raw_data)


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Instagram post likers and commenters via Apify"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # scrape
    scrape_sp = subparsers.add_parser(
        "scrape", help="Scrape likers and commenters for posts"
    )
    scrape_sp.add_argument("--post", help="Instagram post URL")
    scrape_sp.add_argument(
        "--posts-file",
        help="Text file with one post URL per line",
    )

    # analyze
    analyze_sp = subparsers.add_parser(
        "analyze", help="Re-run analysis on existing raw data"
    )
    analyze_sp.add_argument("--post", required=True, help="Instagram post URL")

    args = parser.parse_args()
    {"scrape": cmd_scrape, "analyze": cmd_analyze}[args.command](args)


if __name__ == "__main__":
    main()
