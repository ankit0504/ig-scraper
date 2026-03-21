"""
Scrape Instagram profiles via Apify, convert to CSV, and generate reports.

Setup:
    export APIFY_TOKEN=your_token_here

Usage:
    # Scrape profiles for all followers in the export, then analyze
    # Only scrapes NEW followers not already in existing output files
    python apify_to_report.py scrape --target chuckforqueens

    # Scrape from a plain text file (one username per line)
    python apify_to_report.py scrape --target chuckforqueens --usernames followers_all.txt

    # Convert an existing Apify JSON to reports (no scraping)
    python apify_to_report.py convert --input data/apify-data-combined.json --target chuckforqueens

    # Re-run analysis reports on existing data (no scraping)
    python apify_to_report.py analyze --target chuckforqueens
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
    """Load usernames already scraped from prior Apify runs, existing CSV, and failed enrichments."""
    scraped: set[str] = set()

    # 1. Check Apify raw JSON from prior runs
    raw_file = DATA_DIR / f"{target}_apify_profiles_raw.json"
    if raw_file.exists():
        with open(raw_file) as f:
            data = json.load(f)
        apify_set = {entry["username"] for entry in data if entry.get("username")}
        scraped.update(apify_set)
        print(f"  From Apify raw JSON: {len(apify_set)} profiles")

    # 2. Check existing profiles CSV (from export_scraper.py or prior Apify runs)
    csv_file = DATA_DIR / f"{target}_profiles_export.csv"
    if csv_file.exists():
        with open(csv_file, newline="") as f:
            reader = csv.DictReader(f)
            csv_set = {row["handle"] for row in reader if row.get("handle")}
        scraped.update(csv_set)
        print(f"  From profiles CSV: {len(csv_set)} profiles")

    # 3. Check failed enrichments (don't waste Apify calls on known failures)
    failed_file = DATA_DIR / f"{target}_failed_enrichments.txt"
    if failed_file.exists():
        with open(failed_file) as f:
            failed_set = {line.strip() for line in f if line.strip()}
        scraped.update(failed_set)
        print(f"  From failed enrichments: {len(failed_set)} usernames")

    if scraped:
        print(f"Already processed: {len(scraped)} total (will skip)")
    return scraped


def recover_run(client: ApifyClient, run_id: str, target: str) -> set[str]:
    """Recover results from a previous Apify run that was interrupted locally."""
    raw_file = DATA_DIR / f"{target}_apify_profiles_raw.json"

    run_info = client.run(run_id).get()
    status = run_info["status"]

    if status == "RUNNING":
        print(f"  Run is still going — waiting for it to finish...")
        while status == "RUNNING":
            time.sleep(5)
            run_info = client.run(run_id).get()
            status = run_info["status"]
            status_msg = run_info.get("statusMessage", "")
            print(f"  [{status}] {status_msg}", end="\r")
        print()

    if status not in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
        print(f"  Run status: {status} — cannot recover.")
        return set()

    dataset_id = run_info["defaultDatasetId"]
    items = list(client.dataset(dataset_id).iterate_items())

    if not items:
        print("  No results to recover.")
        return set()

    # Merge into existing raw file
    existing: list[dict] = []
    if raw_file.exists():
        with open(raw_file) as f:
            existing = json.load(f)

    existing_usernames = {e["username"] for e in existing if e.get("username")}
    new_items = [i for i in items if i.get("username") not in existing_usernames]
    existing.extend(new_items)

    with open(raw_file, "w") as f:
        json.dump(existing, f, indent=2)

    recovered = {i["username"] for i in new_items if i.get("username")}
    print(f"  Recovered {len(recovered)} new profiles from run {run_id}")
    return recovered


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
        # Check for a pending run from a previous interrupted session
        pending_file = DATA_DIR / f"{target}_pending_run.json"
        if pending_file.exists():
            with open(pending_file) as f:
                pending = json.load(f)
            print(f"\nFound pending run from previous session: {pending['run_id']}")
            print("Recovering results...")
            recovered = recover_run(client, pending["run_id"], target)
            if recovered:
                remaining = [u for u in remaining if u not in recovered]
                print(f"Recovered {len(recovered)} profiles. Remaining: {len(remaining)}")
            pending_file.unlink()

        if not remaining:
            print("All profiles now scraped after recovery!")
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
                    # Start the run (non-blocking) and save run ID for recovery
                    run = client.actor(PROFILE_ACTOR).start(
                        run_input={
                            "usernames": batch,
                        },
                        memory_mbytes=4096,
                    )
                    run_id = run["id"]
                    dataset_id = run["defaultDatasetId"]

                    # Save run ID so we can recover if interrupted
                    with open(pending_file, "w") as f:
                        json.dump({"run_id": run_id, "dataset_id": dataset_id}, f)

                    print(f"  Started run {run_id} — waiting for completion...")

                    # Poll until done
                    while True:
                        run_info = client.run(run_id).get()
                        status = run_info["status"]
                        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                            break
                        status_msg = run_info.get("statusMessage", "")
                        print(f"  [{status}] {status_msg}", end="\r")
                        time.sleep(5)
                    print()

                    if status != "SUCCEEDED":
                        print(f"  Run finished with status: {status}")
                        print("  Trying to fetch partial results anyway...")

                    items = list(client.dataset(dataset_id).iterate_items())
                    existing.extend(items)

                    print(f"  Got {len(items)} profiles (total: {len(existing)})")

                    # Save after each batch for resume support
                    with open(raw_file, "w") as f:
                        json.dump(existing, f, indent=2)

                    # Clear pending run
                    if pending_file.exists():
                        pending_file.unlink()

                except KeyboardInterrupt:
                    print(f"\n\nInterrupted! Run {run_id} is still going on Apify.")
                    print(f"Re-run the same command to recover results when it finishes.")
                    # Save what we have so far
                    if existing:
                        with open(raw_file, "w") as f:
                            json.dump(existing, f, indent=2)
                    sys.exit(1)

                except Exception as e:
                    print(f"  Batch {batch_num} failed: {e}")
                    print("  Progress saved. Re-run to resume from where you left off.")
                    break

    # Convert and analyze
    print("\n=== Converting to CSV ===")
    if not raw_file.exists():
        print(f"No Apify raw data at {raw_file}, skipping conversion.")
    else:
        convert_apify_to_csv(raw_file, target)

    print("\n=== Running analysis ===")
    run_analysis(target)


def convert_apify_to_csv(input_path: Path, target: str) -> Path:
    """Convert Apify JSON to the profiles CSV format."""
    with open(input_path) as f:
        apify_data = json.load(f)

    print(f"Loaded {len(apify_data)} profiles from {input_path}")

    # Load current followers from export for follow_date and unfollower detection
    followers_file = DATA_DIR / f"{target}_followers_export.json"
    handle_to_date: dict[str, str] = {}
    current_follower_handles: set[str] = set()
    if followers_file.exists():
        with open(followers_file) as f:
            for entry in json.load(f):
                handle_to_date[entry["handle"]] = entry.get("follow_date", "")
                current_follower_handles.add(entry["handle"])
        print(f"Loaded {len(current_follower_handles)} current followers from export")

    fieldnames = [
        "handle", "ig_user_id", "full_name",
        "follower_count", "following_count",
        "is_verified", "is_private",
        "is_business", "is_professional",
        "category", "bio",
        "external_url", "post_count",
        "profile_pic_url", "follow_date",
        "status",
    ]

    profiles_file = DATA_DIR / f"{target}_profiles_export.csv"

    # Load existing profiles from CSV to preserve prior enrichment data
    existing_profiles: dict[str, dict] = {}
    if profiles_file.exists():
        with open(profiles_file, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("handle"):
                    existing_profiles[row["handle"]] = row
        print(f"Loaded {len(existing_profiles)} existing profiles from CSV")

    # Convert new Apify data and merge (Apify data overwrites existing for same handle)
    new_count = 0
    skipped = 0

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
            "status": "following" if username in current_follower_handles else "unfollowed",
        }

        if username not in existing_profiles:
            new_count += 1
        existing_profiles[username] = row

    # Update status for existing profiles not in Apify data
    unfollowed_count = 0
    for handle, row in existing_profiles.items():
        if handle in current_follower_handles:
            row["status"] = "following"
        elif row.get("status") != "unfollowed":
            row["status"] = "unfollowed"
            unfollowed_count += 1

    if unfollowed_count:
        print(f"Detected {unfollowed_count} unfollowers since last export")

    # Write merged profiles
    with open(profiles_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in existing_profiles.values():
            writer.writerow(row)

    print(f"Wrote {len(existing_profiles)} profiles to {profiles_file} ({new_count} new from Apify, skipped {skipped})")
    return profiles_file


def cmd_convert(args: argparse.Namespace) -> None:
    """Convert existing Apify JSON to reports (no scraping)."""
    input_path = Path(args.input)
    target = args.target

    convert_apify_to_csv(input_path, target)

    print("\n=== Running analysis ===")
    run_analysis(target)


def run_analysis(target: str) -> None:
    """Generate analysis reports from enriched profile data."""
    import pandas as pd

    profiles_file = DATA_DIR / f"{target}_profiles_export.csv"

    if not profiles_file.exists():
        print("No profiles file. Run 'scrape' first.")
        sys.exit(1)

    df = pd.read_csv(profiles_file, dtype={"ig_user_id": str})
    print(f"Loaded {len(df)} enriched profiles\n")

    output_dir = DATA_DIR / f"{target}_reports"
    output_dir.mkdir(exist_ok=True)

    # Split current followers from unfollowers
    if "status" in df.columns:
        unfollowers = df[df["status"] == "unfollowed"].sort_values(
            "follower_count", ascending=False
        )
        if len(unfollowers) > 0:
            unfollowers.to_csv(output_dir / "unfollowers.csv", index=False)
            print(f"Unfollowers: {len(unfollowers)}")
            cols = ["handle", "full_name", "follower_count", "follow_date"]
            print(unfollowers[cols].head(20).to_string(index=False))
            print()

        # Filter to current followers for the rest of the reports
        df = df[df["status"] != "unfollowed"].copy()
        print(f"Current followers: {len(df)}")

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

    # 1. All followers (full dump, sorted by follower count descending)
    df.sort_values("follower_count", ascending=False).to_csv(
        output_dir / "all_followers.csv", index=False
    )

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

    # 6. Follower growth timeline (uses follow_date)
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

    # analyze (run reports on existing CSV, no scraping)
    analyze_sp = subparsers.add_parser(
        "analyze", help="Generate analysis reports from existing profiles CSV"
    )
    analyze_sp.add_argument("--target", required=True, help="Target IG username")

    # recover (fetch results from a previous Apify run by ID)
    recover_sp = subparsers.add_parser(
        "recover", help="Recover results from a previous Apify run"
    )
    recover_sp.add_argument("--target", required=True, help="Target IG username")
    recover_sp.add_argument("--run-id", required=True, help="Apify run ID to recover")

    args = parser.parse_args()

    def cmd_analyze(args):
        run_analysis(args.target)

    def cmd_recover(args):
        client = get_client()
        recovered = recover_run(client, args.run_id, args.target)
        if recovered:
            print(f"\n=== Converting to CSV ===")
            raw_file = DATA_DIR / f"{args.target}_apify_profiles_raw.json"
            convert_apify_to_csv(raw_file, args.target)
            print(f"\n=== Running analysis ===")
            run_analysis(args.target)

    {"scrape": cmd_scrape, "convert": cmd_convert, "analyze": cmd_analyze, "recover": cmd_recover}[args.command](args)


if __name__ == "__main__":
    main()
