"""
Scrape followers of an Instagram account and collect profile information.

Usage:
    # Step 1: Login (one-time, saves session)
    python scrape_followers.py login --username YOUR_IG_USERNAME

    # Step 2: Collect follower usernames
    python scrape_followers.py followers --target chuckforqueens

    # Step 3: Enrich profiles with detailed info
    python scrape_followers.py enrich --target chuckforqueens

    # Step 4: Generate analysis report
    python scrape_followers.py analyze --target chuckforqueens
"""

import argparse
import csv
import json
import os
import sys
import time
import getpass
from pathlib import Path

import instaloader

DATA_DIR = Path("data")


def get_loader(username: str | None = None) -> instaloader.Instaloader:
    """Create an Instaloader instance, optionally loading a saved session."""
    loader = instaloader.Instaloader(
        download_pictures=False,
        download_videos=False,
        download_video_thumbnails=False,
        download_geotags=False,
        download_comments=False,
        save_metadata=False,
        compress_json=False,
        quiet=True,
    )
    if username:
        session_file = DATA_DIR / f"session-{username}"
        if session_file.exists():
            loader.load_session_from_file(username, str(session_file))
            print(f"Loaded session for @{username}")
        else:
            print(f"No saved session found for @{username}. Run 'login' first.")
            sys.exit(1)
    return loader


def cmd_login(args: argparse.Namespace) -> None:
    """Authenticate and save session for reuse."""
    DATA_DIR.mkdir(exist_ok=True)
    loader = instaloader.Instaloader(quiet=True)
    username = args.username
    password = args.password or os.environ.get("IG_PASSWORD")
    if not password:
        password = getpass.getpass(f"Password for @{username}: ")

    try:
        loader.login(username, password)
        session_file = DATA_DIR / f"session-{username}"
        loader.save_session_to_file(str(session_file))
        print(f"Login successful. Session saved to {session_file}")
    except instaloader.exceptions.BadCredentialsException:
        print("Login failed: bad credentials.")
        sys.exit(1)
    except instaloader.exceptions.TwoFactorAuthRequiredException:
        print("Two-factor auth required.")
        code = args.twofa_code or input("2FA code: ")
        loader.two_factor_login(code)
        session_file = DATA_DIR / f"session-{username}"
        loader.save_session_to_file(str(session_file))
        print(f"Login successful with 2FA. Session saved to {session_file}")


def cmd_followers(args: argparse.Namespace) -> None:
    """Collect follower usernames for the target account."""
    DATA_DIR.mkdir(exist_ok=True)
    loader = get_loader(args.username)
    target = args.target
    fast = getattr(args, "fast", False)

    followers_file = DATA_DIR / f"{target}_followers.json"

    # Load existing progress
    collected = set()
    if followers_file.exists():
        with open(followers_file) as f:
            collected = set(json.load(f))
        print(f"Resuming: {len(collected)} followers already collected")

    print(f"Fetching follower list for @{target}...")
    try:
        profile = instaloader.Profile.from_username(loader.context, target)
    except instaloader.exceptions.ProfileNotExistsException:
        print(f"Profile @{target} does not exist.")
        sys.exit(1)

    print(f"@{target} has {profile.followers} followers")
    print("This will take a while for large accounts. Progress is saved automatically.")

    max_retries = 5
    retry_delay = 60  # start with 60s, doubles each retry

    for attempt in range(max_retries):
        try:
            if attempt > 0:
                wait = retry_delay * (2 ** (attempt - 1))
                print(f"  Retry {attempt}/{max_retries}, waiting {wait}s...")
                time.sleep(wait)

            follower_iter = profile.get_followers()
            count = len(collected)
            save_interval = 100

            for follower in follower_iter:
                if follower.username in collected:
                    continue
                collected.add(follower.username)
                count += 1

                if count % save_interval == 0:
                    with open(followers_file, "w") as f:
                        json.dump(sorted(collected), f)
                    print(f"  ... {count} followers collected (saved)")

                # Delay every few followers to stay under radar
                if count % 50 == 0:
                    time.sleep(1 if fast else 3)
                if count % 500 == 0:
                    pause = 5 if fast else 15
                    print(f"  Longer pause at {count} ({pause}s)...")
                    time.sleep(pause)

            # If we get here, we completed successfully
            break

        except (
            instaloader.exceptions.QueryReturnedBadRequestException,
            instaloader.exceptions.ConnectionException,
        ) as e:
            with open(followers_file, "w") as f:
                json.dump(sorted(collected), f)
            print(f"\nRate limited ({e.__class__.__name__}). {len(collected)} saved so far.")
            if attempt == max_retries - 1:
                print("Max retries reached. Re-run later to resume.")
            continue
        except KeyboardInterrupt:
            print("\nInterrupted by user. Saving progress...")
            break

    with open(followers_file, "w") as f:
        json.dump(sorted(collected), f)
    print(f"Saved {len(collected)} follower usernames to {followers_file}")


def cmd_enrich(args: argparse.Namespace) -> None:
    """Fetch detailed profile info for each follower."""
    DATA_DIR.mkdir(exist_ok=True)
    loader = get_loader(args.username)
    target = args.target
    fast = getattr(args, "fast", False)

    followers_file = DATA_DIR / f"{target}_followers.json"
    profiles_file = DATA_DIR / f"{target}_profiles.csv"

    if not followers_file.exists():
        print(f"No followers file found. Run 'followers' first.")
        sys.exit(1)

    with open(followers_file) as f:
        all_followers = json.load(f)

    # Load already-enriched profiles
    enriched = set()
    existing_rows = []
    if profiles_file.exists():
        with open(profiles_file, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                enriched.add(row["handle"])
                existing_rows.append(row)
        print(f"Resuming: {len(enriched)} profiles already enriched")

    remaining = [u for u in all_followers if u not in enriched]
    print(f"{len(remaining)} profiles to enrich out of {len(all_followers)} total")

    if not remaining:
        print("All profiles already enriched!")
        return

    fieldnames = [
        "handle",
        "ig_user_id",
        "full_name",
        "follower_count",
        "following_count",
        "is_verified",
        "is_business",
        "business_category",
        "bio",
        "external_url",
        "post_count",
        "reel_count",
        "is_private",
        "profile_pic_url",
    ]

    # Write header + existing rows
    with open(profiles_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in existing_rows:
            writer.writerow(row)

    batch_size = 75 if fast else 50
    delay_between_profiles = 1 if fast else 3
    delay_between_batches = 10 if fast else 30

    processed = 0
    errors = 0

    try:
        for username in remaining:
            try:
                profile = instaloader.Profile.from_username(
                    loader.context, username
                )
                row = {
                    "handle": username,
                    "ig_user_id": profile.userid,
                    "full_name": profile.full_name,
                    "follower_count": profile.followers,
                    "following_count": profile.followees,
                    "is_verified": profile.is_verified,
                    "is_business": profile.is_business_account,
                    "business_category": profile.business_category_name or "",
                    "bio": (profile.biography or "").replace("\n", " | "),
                    "external_url": profile.external_url or "",
                    "post_count": profile.mediacount,
                    "reel_count": _get_reel_count(profile),
                    "is_private": profile.is_private,
                    "profile_pic_url": profile.profile_pic_url,
                }

                with open(profiles_file, "a", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writerow(row)

                processed += 1
                if processed % 10 == 0:
                    print(
                        f"  ... {processed}/{len(remaining)} enriched "
                        f"({errors} errors)"
                    )

                time.sleep(delay_between_profiles)

                if processed % batch_size == 0:
                    print(
                        f"  Batch pause ({delay_between_batches}s to avoid "
                        f"rate limit)..."
                    )
                    time.sleep(delay_between_batches)

            except instaloader.exceptions.ProfileNotExistsException:
                errors += 1
                continue
            except instaloader.exceptions.ConnectionException as e:
                if "429" in str(e) or "rate" in str(e).lower():
                    print(
                        f"\nRate limited at {processed}/{len(remaining)}. "
                        f"Wait ~15 min and re-run."
                    )
                    break
                errors += 1
                print(f"  Connection error for @{username}: {e}")
                time.sleep(10)
                continue
            except instaloader.exceptions.QueryReturnedBadRequestException:
                print(
                    f"\nRate limited at {processed}/{len(remaining)}. "
                    f"Wait ~15 min and re-run."
                )
                break

    except KeyboardInterrupt:
        print("\nInterrupted. Progress saved.")

    total_enriched = len(enriched) + processed
    print(
        f"Done. {total_enriched}/{len(all_followers)} profiles enriched "
        f"({errors} errors). Data in {profiles_file}"
    )


def _get_reel_count(profile: instaloader.Profile) -> int:
    """Try to get reel count. Returns -1 if unavailable (private profile)."""
    try:
        return profile.igtvcount + profile.reels_count if hasattr(profile, 'reels_count') else profile.igtvcount
    except Exception:
        return -1


def cmd_analyze(args: argparse.Namespace) -> None:
    """Generate analysis reports from enriched profile data."""
    import pandas as pd

    target = args.target
    profiles_file = DATA_DIR / f"{target}_profiles.csv"

    if not profiles_file.exists():
        print(f"No profiles file found. Run 'enrich' first.")
        sys.exit(1)

    df = pd.read_csv(profiles_file)
    print(f"Loaded {len(df)} profiles\n")

    output_dir = DATA_DIR / f"{target}_reports"
    output_dir.mkdir(exist_ok=True)

    # --- 1. Noteworthy accounts (verified or high followers) ---
    noteworthy = df[
        (df["is_verified"] == True) | (df["follower_count"] >= 5000)
    ].sort_values("follower_count", ascending=False)

    noteworthy_file = output_dir / "noteworthy_accounts.csv"
    noteworthy.to_csv(noteworthy_file, index=False)
    print(f"Noteworthy accounts (verified or 5k+ followers): {len(noteworthy)}")
    if len(noteworthy) > 0:
        print(noteworthy[["handle", "full_name", "follower_count", "is_verified"]].head(20).to_string(index=False))
    print()

    # --- 2. Local collaborators (Queens/NYC keywords in bio) ---
    local_keywords = [
        "queens", "nyc", "new york", "flushing", "jamaica", "astoria",
        "jackson heights", "long island city", "lic", "woodside",
        "elmhurst", "corona", "forest hills", "rego park", "bayside",
        "fresh meadows", "whitestone", "sunnyside", "ridgewood",
        "maspeth", "middle village", "kew gardens", "howard beach",
        "ozone park", "south ozone", "richmond hill", "woodhaven",
        "rockaways", "rockaway", "far rockaway", "broad channel",
    ]
    bio_lower = df["bio"].fillna("").str.lower()
    local_mask = bio_lower.apply(
        lambda bio: any(kw in bio for kw in local_keywords)
    )
    local = df[local_mask].sort_values("follower_count", ascending=False)

    local_file = output_dir / "local_collaborators.csv"
    local.to_csv(local_file, index=False)
    print(f"Potential local collaborators (Queens/NYC in bio): {len(local)}")
    if len(local) > 0:
        print(local[["handle", "full_name", "follower_count", "bio"]].head(20).to_string(index=False))
    print()

    # --- 3. Large followings ---
    large = df[df["follower_count"] >= 25000].sort_values(
        "follower_count", ascending=False
    )

    large_file = output_dir / "large_followings.csv"
    large.to_csv(large_file, index=False)
    print(f"Accounts with 25k+ followers: {len(large)}")
    if len(large) > 0:
        print(large[["handle", "full_name", "follower_count", "is_verified"]].head(20).to_string(index=False))
    print()

    # --- 4. Business/Creator accounts (potential partners) ---
    business = df[df["is_business"] == True].sort_values(
        "follower_count", ascending=False
    )

    business_file = output_dir / "business_accounts.csv"
    business.to_csv(business_file, index=False)
    print(f"Business/Creator accounts: {len(business)}")
    print()

    # --- Summary stats ---
    print("=== Summary ===")
    print(f"Total followers scraped: {len(df)}")
    print(f"Verified accounts: {df['is_verified'].sum()}")
    print(f"Business/Creator accounts: {df['is_business'].sum()}")
    print(f"Private accounts: {df['is_private'].sum()}")
    print(f"Median follower count: {df['follower_count'].median():.0f}")
    print(f"Mean follower count: {df['follower_count'].mean():.0f}")
    print(f"\nReports saved to {output_dir}/")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape Instagram followers and analyze profiles"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # login
    login_parser = subparsers.add_parser("login", help="Login and save session")
    login_parser.add_argument("--username", required=True, help="Your IG username")
    login_parser.add_argument("--password", help="Your IG password (or set IG_PASSWORD env var)")
    login_parser.add_argument("--twofa-code", help="2FA code if required")

    # followers
    followers_parser = subparsers.add_parser(
        "followers", help="Collect follower usernames"
    )
    followers_parser.add_argument("--username", required=True, help="Your IG username")
    followers_parser.add_argument(
        "--target", required=True, help="Target account to scrape"
    )
    followers_parser.add_argument(
        "--fast", action="store_true",
        help="Faster pacing (higher rate-limit risk, but ~3x speed)",
    )

    # enrich
    enrich_parser = subparsers.add_parser(
        "enrich", help="Fetch detailed profile info"
    )
    enrich_parser.add_argument("--username", required=True, help="Your IG username")
    enrich_parser.add_argument(
        "--target", required=True, help="Target account"
    )
    enrich_parser.add_argument(
        "--fast", action="store_true",
        help="Faster pacing (higher rate-limit risk, but ~3x speed)",
    )

    # analyze
    analyze_parser = subparsers.add_parser(
        "analyze", help="Generate analysis reports"
    )
    analyze_parser.add_argument(
        "--target", required=True, help="Target account"
    )

    args = parser.parse_args()

    if args.command == "login":
        cmd_login(args)
    elif args.command == "followers":
        cmd_followers(args)
    elif args.command == "enrich":
        cmd_enrich(args)
    elif args.command == "analyze":
        cmd_analyze(args)


if __name__ == "__main__":
    main()
