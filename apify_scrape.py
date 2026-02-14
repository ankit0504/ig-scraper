"""
Scrape Instagram followers and post commenters via Apify.

Setup:
    1. Create an Apify account at https://apify.com
    2. Get your API token from https://console.apify.com/account/integrations
    3. Set it: export APIFY_TOKEN=your_token_here

Usage:
    # Run everything (followers + posts + comments)
    python apify_scrape.py run --target chuckforqueens

    # Or run steps individually:
    python apify_scrape.py followers --target chuckforqueens
    python apify_scrape.py posts --target chuckforqueens
    python apify_scrape.py comments --target chuckforqueens

    # Analyze results
    python apify_scrape.py analyze --target chuckforqueens
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

# Apify actor IDs
FOLLOWERS_ACTOR = "instaprism/instagram-followers-scraper"
POST_ACTOR = "apify/instagram-post-scraper"
COMMENT_ACTOR = "apify/instagram-comment-scraper"


def get_client() -> ApifyClient:
    token = os.environ.get("APIFY_TOKEN")
    if not token:
        print("Error: Set APIFY_TOKEN environment variable.")
        print("Get your token at https://console.apify.com/account/integrations")
        sys.exit(1)
    return ApifyClient(token)


def wait_for_run(client: ApifyClient, run: dict, label: str) -> dict:
    """Poll a run until it finishes."""
    run_id = run["id"]
    print(f"  Run started: {run_id}")
    print(f"  Monitor at: https://console.apify.com/actors/runs/{run_id}")

    while True:
        run_info = client.run(run_id).get()
        status = run_info["status"]
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            break
        print(f"  Status: {status}... waiting 15s")
        time.sleep(15)

    if status != "SUCCEEDED":
        print(f"  {label} run {status}. Check Apify console for details.")
        sys.exit(1)

    print(f"  {label} completed successfully.")
    return run_info


def fetch_dataset(client: ApifyClient, dataset_id: str) -> list[dict]:
    """Fetch all items from an Apify dataset."""
    items = []
    for item in client.dataset(dataset_id).iterate_items():
        items.append(item)
    return items


def cmd_followers(args: argparse.Namespace) -> list[dict]:
    """Scrape followers for the target account."""
    DATA_DIR.mkdir(exist_ok=True)
    client = get_client()
    target = args.target
    followers_file = DATA_DIR / f"{target}_followers_apify.json"

    print(f"Starting followers scrape for @{target}...")
    run = client.actor(FOLLOWERS_ACTOR).call(
        run_input={
            "username": target,
            "limit": 50000,
            "resultsLimit": 50000,
        },
        timeout_secs=86400,  # 24h max
        memory_mbytes=4096,
    )

    dataset_id = run["defaultDatasetId"]
    items = fetch_dataset(client, dataset_id)

    with open(followers_file, "w") as f:
        json.dump(items, f, indent=2)

    print(f"Saved {len(items)} followers to {followers_file}")
    return items


def cmd_posts(args: argparse.Namespace) -> list[dict]:
    """Scrape all posts for the target account."""
    DATA_DIR.mkdir(exist_ok=True)
    client = get_client()
    target = args.target
    posts_file = DATA_DIR / f"{target}_posts_apify.json"

    print(f"Starting posts scrape for @{target}...")
    run = client.actor(POST_ACTOR).call(
        run_input={
            "username": [target],
            "resultsLimit": 1000,
        },
        timeout_secs=3600,
        memory_mbytes=4096,
    )

    dataset_id = run["defaultDatasetId"]
    items = fetch_dataset(client, dataset_id)

    with open(posts_file, "w") as f:
        json.dump(items, f, indent=2)

    print(f"Saved {len(items)} posts to {posts_file}")
    return items


def cmd_comments(args: argparse.Namespace) -> list[dict]:
    """Scrape comments for all posts of the target account."""
    DATA_DIR.mkdir(exist_ok=True)
    client = get_client()
    target = args.target
    posts_file = DATA_DIR / f"{target}_posts_apify.json"
    comments_file = DATA_DIR / f"{target}_comments_apify.json"

    if not posts_file.exists():
        print("No posts file found. Run 'posts' first.")
        sys.exit(1)

    with open(posts_file) as f:
        posts = json.load(f)

    # Extract post URLs
    post_urls = []
    for post in posts:
        url = post.get("url") or post.get("displayUrl")
        shortcode = post.get("shortCode") or post.get("shortcode")
        if shortcode:
            post_urls.append(f"https://www.instagram.com/p/{shortcode}/")
        elif url and "instagram.com" in url:
            post_urls.append(url)

    if not post_urls:
        print("No post URLs found in posts data.")
        sys.exit(1)

    print(f"Scraping comments for {len(post_urls)} posts...")

    # Process in batches to avoid timeout
    batch_size = 50
    all_comments = []

    for i in range(0, len(post_urls), batch_size):
        batch = post_urls[i : i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(post_urls) + batch_size - 1) // batch_size
        print(f"  Batch {batch_num}/{total_batches} ({len(batch)} posts)...")

        run = client.actor(COMMENT_ACTOR).call(
            run_input={
                "directUrls": batch,
                "resultsLimit": 500,  # per post
            },
            timeout_secs=3600,
            memory_mbytes=4096,
        )

        dataset_id = run["defaultDatasetId"]
        items = fetch_dataset(client, dataset_id)
        all_comments.extend(items)
        print(f"    Got {len(items)} comments")

        # Save progress after each batch
        with open(comments_file, "w") as f:
            json.dump(all_comments, f, indent=2)

    print(f"Saved {len(all_comments)} total comments to {comments_file}")
    return all_comments


def cmd_run(args: argparse.Namespace) -> None:
    """Run all scraping steps in sequence."""
    print("=== Step 1/3: Scraping followers ===")
    cmd_followers(args)

    print("\n=== Step 2/3: Scraping posts ===")
    cmd_posts(args)

    print("\n=== Step 3/3: Scraping comments ===")
    cmd_comments(args)

    print("\n=== Step 4/4: Analyzing ===")
    cmd_analyze(args)


def cmd_analyze(args: argparse.Namespace) -> None:
    """Analyze followers and commenters data."""
    import pandas as pd

    target = args.target
    followers_file = DATA_DIR / f"{target}_followers_apify.json"
    comments_file = DATA_DIR / f"{target}_comments_apify.json"
    output_dir = DATA_DIR / f"{target}_reports"
    output_dir.mkdir(exist_ok=True)

    # --- Load followers ---
    if followers_file.exists():
        with open(followers_file) as f:
            raw_followers = json.load(f)

        # Normalize follower data — field names vary by actor
        followers = []
        for f_data in raw_followers:
            followers.append({
                "handle": (
                    f_data.get("username")
                    or f_data.get("userName")
                    or f_data.get("handle")
                    or ""
                ),
                "ig_user_id": (
                    f_data.get("id")
                    or f_data.get("pk")
                    or f_data.get("userId")
                    or f_data.get("iguid")
                    or ""
                ),
                "full_name": f_data.get("fullName") or f_data.get("full_name") or "",
                "follower_count": (
                    f_data.get("followerCount")
                    or f_data.get("followers")
                    or f_data.get("follower_count")
                    or 0
                ),
                "following_count": (
                    f_data.get("followingCount")
                    or f_data.get("following")
                    or f_data.get("following_count")
                    or 0
                ),
                "is_verified": (
                    f_data.get("isVerified")
                    or f_data.get("is_verified")
                    or False
                ),
                "is_private": (
                    f_data.get("isPrivate")
                    or f_data.get("is_private")
                    or False
                ),
                "bio": (
                    (f_data.get("biography") or f_data.get("bio") or "")
                    .replace("\n", " | ")
                ),
                "post_count": (
                    f_data.get("mediaCount")
                    or f_data.get("postsCount")
                    or f_data.get("post_count")
                    or 0
                ),
                "external_url": (
                    f_data.get("externalUrl")
                    or f_data.get("external_url")
                    or ""
                ),
                "profile_pic_url": (
                    f_data.get("profilePicUrl")
                    or f_data.get("profile_pic_url")
                    or ""
                ),
            })

        df = pd.DataFrame(followers)
        print(f"Loaded {len(df)} followers")
    else:
        df = pd.DataFrame()
        print("No followers data found.")

    # --- Load comments ---
    commenter_engagement = {}
    if comments_file.exists():
        with open(comments_file) as f:
            raw_comments = json.load(f)

        for c in raw_comments:
            username = (
                c.get("ownerUsername")
                or c.get("username")
                or c.get("owner", {}).get("username")
                or ""
            )
            if not username:
                continue
            if username not in commenter_engagement:
                commenter_engagement[username] = {"comment_count": 0, "comments": []}
            commenter_engagement[username]["comment_count"] += 1
            text = c.get("text") or c.get("body") or ""
            if text:
                commenter_engagement[username]["comments"].append(text[:200])

        print(f"Loaded {len(raw_comments)} comments from {len(commenter_engagement)} unique commenters")
    else:
        print("No comments data found.")

    if len(df) == 0 and not commenter_engagement:
        print("No data to analyze.")
        return

    # Add comment counts to follower data
    if len(df) > 0 and commenter_engagement:
        df["comment_count"] = df["handle"].map(
            lambda h: commenter_engagement.get(h, {}).get("comment_count", 0)
        )
    elif len(df) > 0:
        df["comment_count"] = 0

    # --- Reports ---
    print(f"\nGenerating reports in {output_dir}/\n")

    # 1. All followers (full data)
    if len(df) > 0:
        all_file = output_dir / "all_followers.csv"
        df.to_csv(all_file, index=False)

    # 2. Noteworthy accounts (verified or 5k+ followers)
    if len(df) > 0:
        noteworthy = df[
            (df["is_verified"] == True) | (df["follower_count"] >= 5000)
        ].sort_values("follower_count", ascending=False)

        noteworthy.to_csv(output_dir / "noteworthy_accounts.csv", index=False)
        print(f"Noteworthy (verified or 5k+ followers): {len(noteworthy)}")
        if len(noteworthy) > 0:
            print(
                noteworthy[["handle", "ig_user_id", "full_name", "follower_count", "is_verified", "comment_count"]]
                .head(20)
                .to_string(index=False)
            )
        print()

    # 3. Local collaborators (Queens/NYC in bio)
    if len(df) > 0:
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
            print(
                local[["handle", "full_name", "follower_count", "bio"]]
                .head(20)
                .to_string(index=False)
            )
        print()

    # 4. Large followings (10k+)
    if len(df) > 0:
        large = df[df["follower_count"] >= 10000].sort_values(
            "follower_count", ascending=False
        )
        large.to_csv(output_dir / "large_followings.csv", index=False)
        print(f"Large followings (10k+): {len(large)}")
        if len(large) > 0:
            print(
                large[["handle", "full_name", "follower_count", "is_verified"]]
                .head(20)
                .to_string(index=False)
            )
        print()

    # 5. Most engaged commenters
    if commenter_engagement:
        commenter_rows = []
        for username, data in commenter_engagement.items():
            # Cross-reference with follower data
            follower_info = {}
            if len(df) > 0:
                match = df[df["handle"] == username]
                if len(match) > 0:
                    follower_info = match.iloc[0].to_dict()

            commenter_rows.append({
                "handle": username,
                "comment_count": data["comment_count"],
                "follower_count": follower_info.get("follower_count", "unknown"),
                "is_verified": follower_info.get("is_verified", "unknown"),
                "bio": follower_info.get("bio", "unknown"),
                "is_follower": len(follower_info) > 0,
                "sample_comments": " /// ".join(data["comments"][:3]),
            })

        commenter_df = pd.DataFrame(commenter_rows).sort_values(
            "comment_count", ascending=False
        )
        commenter_df.to_csv(output_dir / "top_commenters.csv", index=False)
        print(f"Unique commenters: {len(commenter_df)}")
        print(
            commenter_df[["handle", "comment_count", "follower_count", "is_follower"]]
            .head(20)
            .to_string(index=False)
        )
        print()

    # 6. Commenters NOT following (potential follow targets)
    if commenter_engagement and len(df) > 0:
        follower_handles = set(df["handle"])
        non_follower_commenters = [
            u for u in commenter_engagement if u not in follower_handles
        ]
        print(
            f"Commenters who are NOT followers (potential follow targets): "
            f"{len(non_follower_commenters)}"
        )
        if non_follower_commenters:
            nfc_data = [
                {"handle": u, "comment_count": commenter_engagement[u]["comment_count"]}
                for u in non_follower_commenters
            ]
            nfc_df = pd.DataFrame(nfc_data).sort_values(
                "comment_count", ascending=False
            )
            nfc_df.to_csv(
                output_dir / "commenters_not_following.csv", index=False
            )
            print(nfc_df.head(10).to_string(index=False))
        print()

    # --- Summary ---
    print("=== Summary ===")
    if len(df) > 0:
        print(f"Total followers scraped: {len(df)}")
        print(f"Verified accounts: {df['is_verified'].sum()}")
        print(f"Private accounts: {df['is_private'].sum()}")
        print(f"Median follower count: {df['follower_count'].median():.0f}")
        print(f"Mean follower count: {df['follower_count'].mean():.0f}")
    if commenter_engagement:
        print(f"Total unique commenters: {len(commenter_engagement)}")
        total_comments = sum(
            d["comment_count"] for d in commenter_engagement.values()
        )
        print(f"Total comments: {total_comments}")
    print(f"\nAll reports saved to {output_dir}/")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape Instagram followers and comments via Apify"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # run (all steps)
    run_parser = subparsers.add_parser("run", help="Run all steps")
    run_parser.add_argument("--target", required=True, help="Target IG account")

    # followers
    f_parser = subparsers.add_parser("followers", help="Scrape followers only")
    f_parser.add_argument("--target", required=True, help="Target IG account")

    # posts
    p_parser = subparsers.add_parser("posts", help="Scrape posts only")
    p_parser.add_argument("--target", required=True, help="Target IG account")

    # comments
    c_parser = subparsers.add_parser("comments", help="Scrape comments only")
    c_parser.add_argument("--target", required=True, help="Target IG account")

    # analyze
    a_parser = subparsers.add_parser("analyze", help="Analyze scraped data")
    a_parser.add_argument("--target", required=True, help="Target IG account")

    args = parser.parse_args()

    commands = {
        "run": cmd_run,
        "followers": cmd_followers,
        "posts": cmd_posts,
        "comments": cmd_comments,
        "analyze": cmd_analyze,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
