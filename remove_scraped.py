import json
import sys

FOLLOWERS_FILE = "/Users/agupta/Development/ig-scraper/instagram-export-chuckforqueens/connections/followers_and_following/followers_all.txt"
DATASET_FILE = "/Users/agupta/Downloads/dataset_instagram-profile-scraper_2026-02-16_00-21-34-749-2.json"

# Load scraped usernames from the dataset
with open(DATASET_FILE) as f:
    dataset = json.load(f)

scraped = {entry["username"] for entry in dataset if "username" in entry}
print(f"Scraped usernames to remove: {len(scraped)}")

# Load all followers
with open(FOLLOWERS_FILE) as f:
    followers = [line.strip() for line in f if line.strip()]

print(f"Followers before: {len(followers)}")

# Remove scraped usernames
remaining = [u for u in followers if u not in scraped]
print(f"Followers after: {len(remaining)}")
print(f"Removed: {len(followers) - len(remaining)}")

# Write back
with open(FOLLOWERS_FILE, "w") as f:
    f.write("\n".join(remaining) + "\n")

print(f"Updated {FOLLOWERS_FILE}")
