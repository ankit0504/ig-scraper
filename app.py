"""
Streamlit UI for the Instagram scraper tools.

Run locally:
    streamlit run app.py

Deploy:
    Push to GitHub, then connect at https://share.streamlit.io
"""

import io
import os
import sys
import contextlib
from pathlib import Path

import streamlit as st

st.set_page_config(page_title="IG Scraper", page_icon="📊", layout="wide")

# Inject Apify token into env before importing modules that use it
apify_token = st.sidebar.text_input("Apify Token", type="password", help="Get yours at https://console.apify.com/account/integrations")
if apify_token:
    os.environ["APIFY_TOKEN"] = apify_token

# Now safe to import project modules
from apify_to_report import (
    DATA_DIR,
    load_usernames,
    load_already_scraped,
    convert_apify_to_csv,
    run_analysis,
    get_client,
    PROFILE_ACTOR,
)
from post_engagers import (
    scrape_post,
    build_engagers_csv,
    shortcode_from_url,
    output_prefix,
)
import json
import time
import pandas as pd


def capture_prints(fn, *args, **kwargs):
    """Run a function, capturing its print output. Returns (result, output_text)."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        result = fn(*args, **kwargs)
    return result, buf.getvalue()


def require_token():
    if not apify_token:
        st.warning("Enter your Apify token in the sidebar to use scraping features.")
        return False
    return True


# --- Sidebar ---
st.sidebar.title("IG Scraper")
st.sidebar.markdown("Instagram profile & post engagement analysis powered by [Apify](https://apify.com).")

tool = st.sidebar.radio("Tool", ["Profile Scraper", "Post Engagers", "Analyze Existing Data"])

DATA_DIR.mkdir(exist_ok=True)

# --- Welcome / Getting Started ---
st.title("IG Scraper")

with st.expander("Getting Started — Read this first!", expanded=not any(DATA_DIR.iterdir())):
    st.markdown("""
### What this tool does
Scrape and analyze Instagram follower profiles and post engagement data using [Apify](https://apify.com).

### Quick start

**Option A: Scrape new data** (requires Apify token)
1. Get a free Apify token at [console.apify.com/account/integrations](https://console.apify.com/account/integrations)
2. Paste it in the **Apify Token** field in the sidebar
3. Pick **Profile Scraper** or **Post Engagers** from the sidebar
4. Follow the prompts and download your results when done

**Option B: Analyze existing data** (no token needed)
1. Upload your data files using the **Upload Data** section at the bottom of the sidebar
2. Pick **Analyze Existing Data** from the sidebar
3. Select your target account and run the analysis

### File naming
Uploaded files **must** follow these naming conventions or the tool won't recognize them:

| File | Name it | What it's for |
|------|---------|---------------|
| Followers list | `{account}_followers_export.json` | List of follower handles to scrape |
| Following list | `{account}_following_export.json` | Detect mutual follows |
| Scraped profiles (raw) | `{account}_apify_profiles_raw.json` | Raw Apify profile data |
| Scraped profiles (CSV) | `{account}_profiles_export.csv` | Enriched profile spreadsheet |
| Username list | `usernames.txt` | Plain text, one username per line |

Replace `{account}` with the Instagram username you're analyzing (e.g. `chuckforqueens_followers_export.json`).

### Tools overview
- **Profile Scraper** — Scrape profile details (follower count, bio, verified status, etc.) for a list of usernames. Generates reports like noteworthy accounts, local collaborators, business accounts, and unfollower detection.
- **Post Engagers** — Scrape who liked and commented on specific Instagram posts.
- **Analyze Existing Data** — Re-run analysis reports on data you've already scraped or uploaded. No API calls, no token needed.
""")

st.markdown("---")

# =============================================================================
# Profile Scraper
# =============================================================================
if tool == "Profile Scraper":
    st.header("Profile Scraper")
    st.caption("Scrape Instagram profiles via Apify, then generate analysis reports.")

    target = st.text_input("Target account (your IG username)", placeholder="e.g. chuckforqueens")

    username_source = st.radio("Username source", ["Upload text file", "Use existing followers export"])

    uploaded_file = None
    if username_source == "Upload text file":
        uploaded_file = st.file_uploader("Upload a text file (one username per line)", type=["txt"])

    batch_size = st.number_input("Batch size", min_value=10, max_value=5000, value=1000, step=100)

    if st.button("Start Scraping", type="primary", disabled=not target):
        if not require_token():
            st.stop()

        client = get_client()

        # Resolve usernames
        usernames: list[str] = []
        if uploaded_file:
            content = uploaded_file.read().decode("utf-8")
            usernames = [line.strip().strip('"').strip(",").strip('"') for line in content.splitlines() if line.strip()]
            st.info(f"Loaded {len(usernames)} usernames from uploaded file")
        else:
            try:
                _, load_output = capture_prints(load_usernames, target)
                usernames_result = load_usernames(target)
                usernames = usernames_result
                st.info(f"Loaded {len(usernames)} usernames from followers export")
            except SystemExit:
                st.error("No followers export found. Upload a text file with usernames instead.")
                st.stop()

        # Check already scraped
        _, scraped_output = capture_prints(load_already_scraped, target)
        already_scraped = load_already_scraped(target)
        remaining = [u for u in usernames if u not in already_scraped]

        st.write(f"**Total:** {len(usernames)} | **Already scraped:** {len(already_scraped)} | **Remaining:** {len(remaining)}")

        if not remaining:
            st.success("All profiles already scraped!")
        else:
            raw_file = DATA_DIR / f"{target}_apify_profiles_raw.json"

            # Load existing raw results
            existing: list[dict] = []
            if raw_file.exists():
                with open(raw_file) as f:
                    existing = json.load(f)

            total_batches = (len(remaining) + batch_size - 1) // batch_size
            progress = st.progress(0, text="Starting...")

            for i in range(0, len(remaining), batch_size):
                batch = remaining[i : i + batch_size]
                batch_num = (i // batch_size) + 1
                progress.progress(batch_num / total_batches, text=f"Batch {batch_num}/{total_batches} ({len(batch)} usernames)")

                try:
                    run = client.actor(PROFILE_ACTOR).start(
                        run_input={"usernames": batch},
                        memory_mbytes=4096,
                    )
                    run_id = run["id"]
                    dataset_id = run["defaultDatasetId"]

                    with st.status(f"Batch {batch_num}/{total_batches} — Run {run_id}", expanded=True) as status:
                        while True:
                            run_info = client.run(run_id).get()
                            run_status = run_info["status"]
                            if run_status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                                break
                            status_msg = run_info.get("statusMessage", "")
                            st.write(f"[{run_status}] {status_msg}")
                            time.sleep(5)

                        items = list(client.dataset(dataset_id).iterate_items())
                        existing.extend(items)
                        status.update(label=f"Batch {batch_num} — Got {len(items)} profiles", state="complete")

                    # Save after each batch
                    with open(raw_file, "w") as f:
                        json.dump(existing, f, indent=2)

                except Exception as e:
                    st.error(f"Batch {batch_num} failed: {e}")
                    st.info("Progress saved. You can re-run to resume.")
                    break

            progress.progress(1.0, text="Scraping complete!")

        # Convert and analyze
        raw_file = DATA_DIR / f"{target}_apify_profiles_raw.json"
        if raw_file.exists():
            with st.status("Converting to CSV and running analysis...") as status:
                _, convert_output = capture_prints(convert_apify_to_csv, raw_file, target)
                _, analysis_output = capture_prints(run_analysis, target)
                status.update(label="Analysis complete!", state="complete")

            with st.expander("Conversion log"):
                st.code(convert_output)
            with st.expander("Analysis log"):
                st.code(analysis_output)

            # Show reports and download buttons
            reports_dir = DATA_DIR / f"{target}_reports"
            if reports_dir.exists():
                st.subheader("Reports")
                for csv_path in sorted(reports_dir.glob("*.csv")):
                    df = pd.read_csv(csv_path)
                    with st.expander(f"{csv_path.stem} ({len(df)} rows)"):
                        st.dataframe(df, use_container_width=True)
                        st.download_button(
                            f"Download {csv_path.name}",
                            data=csv_path.read_bytes(),
                            file_name=csv_path.name,
                            mime="text/csv",
                            key=f"dl_{csv_path.stem}",
                        )

# =============================================================================
# Post Engagers
# =============================================================================
elif tool == "Post Engagers":
    st.header("Post Engagers")
    st.caption("Scrape likers and commenters for Instagram posts.")

    input_mode = st.radio("Input", ["Single post URL", "Upload post URLs file"])

    post_urls: list[str] = []
    if input_mode == "Single post URL":
        url = st.text_input("Post URL", placeholder="https://www.instagram.com/p/ABC123/")
        if url:
            post_urls = [url]
    else:
        uploaded = st.file_uploader("Upload a text file (one URL per line)", type=["txt"])
        if uploaded:
            content = uploaded.read().decode("utf-8")
            post_urls = [line.strip() for line in content.splitlines() if line.strip()]
            st.info(f"Loaded {len(post_urls)} post URLs")

    if st.button("Scrape Engagers", type="primary", disabled=not post_urls):
        if not require_token():
            st.stop()

        client = get_client()

        for idx, post_url in enumerate(post_urls):
            st.subheader(f"Post {idx + 1}: {shortcode_from_url(post_url)}")

            with st.status(f"Scraping {shortcode_from_url(post_url)}...", expanded=True) as status:
                raw_data, scrape_output = capture_prints(scrape_post, client, post_url)
                st.code(scrape_output)
                status.update(label=f"Scraping complete for {shortcode_from_url(post_url)}", state="complete")

            csv_path, csv_output = capture_prints(build_engagers_csv, raw_data)
            with st.expander("Summary"):
                st.code(csv_output)

            # Show results
            if csv_path and csv_path.exists():
                df = pd.read_csv(csv_path)
                st.dataframe(df, use_container_width=True)
                st.download_button(
                    f"Download {csv_path.name}",
                    data=csv_path.read_bytes(),
                    file_name=csv_path.name,
                    mime="text/csv",
                    key=f"dl_engagers_{idx}",
                )

# =============================================================================
# Analyze Existing Data
# =============================================================================
elif tool == "Analyze Existing Data":
    st.header("Analyze Existing Data")
    st.caption("Run analysis on data already uploaded — no Apify token or API calls needed.")

    # Detect available targets from existing files
    existing_targets = set()
    for f in DATA_DIR.glob("*_apify_profiles_raw.json"):
        existing_targets.add(f.stem.replace("_apify_profiles_raw", ""))
    for f in DATA_DIR.glob("*_profiles_export.csv"):
        existing_targets.add(f.stem.replace("_profiles_export", ""))

    if existing_targets:
        target = st.selectbox("Target account", sorted(existing_targets))
    else:
        st.info(
            "No data files found yet. Upload your files using the **Upload Data** "
            "section in the sidebar, then come back here.\n\n"
            "Make sure your files are named like `{account}_apify_profiles_raw.json` "
            "or `{account}_profiles_export.csv`."
        )
        target = st.text_input("Or enter a target account name manually")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Run Profile Analysis", disabled=not target):
            profiles_file = DATA_DIR / f"{target}_profiles_export.csv"
            raw_file = DATA_DIR / f"{target}_apify_profiles_raw.json"

            # Convert raw JSON to CSV first if needed
            if raw_file.exists() and not profiles_file.exists():
                _, convert_output = capture_prints(convert_apify_to_csv, raw_file, target)
                with st.expander("Conversion log"):
                    st.code(convert_output)

            if not profiles_file.exists():
                st.error("No profiles data found. Run the scraper first.")
                st.stop()

            _, output = capture_prints(run_analysis, target)
            with st.expander("Analysis log"):
                st.code(output)

            reports_dir = DATA_DIR / f"{target}_reports"
            if reports_dir.exists():
                st.subheader("Reports")
                for csv_path in sorted(reports_dir.glob("*.csv")):
                    df = pd.read_csv(csv_path)
                    with st.expander(f"{csv_path.stem} ({len(df)} rows)"):
                        st.dataframe(df, use_container_width=True)
                        st.download_button(
                            f"Download {csv_path.name}",
                            data=csv_path.read_bytes(),
                            file_name=csv_path.name,
                            mime="text/csv",
                            key=f"dl_analyze_{csv_path.stem}",
                        )

    with col2:
        if st.button("Re-analyze Post Engagers", disabled=not target):
            engager_files = list(DATA_DIR.glob("post_*_engagers_raw.json"))
            if not engager_files:
                st.error("No post engager data found. Run the post scraper first.")
            else:
                for raw_file in engager_files:
                    with open(raw_file) as f:
                        raw_data = json.load(f)
                    csv_path, output = capture_prints(build_engagers_csv, raw_data)
                    st.write(f"**{raw_file.stem}**")
                    with st.expander("Summary"):
                        st.code(output)
                    if csv_path and csv_path.exists():
                        df = pd.read_csv(csv_path)
                        st.dataframe(df, use_container_width=True)

# --- Upload existing data ---
st.sidebar.markdown("---")
st.sidebar.subheader("Upload Data")
st.sidebar.markdown(
    "Upload files to analyze. **File names matter!** Use the format:\n"
    "- `{account}_followers_export.json`\n"
    "- `{account}_apify_profiles_raw.json`\n"
    "- `{account}_profiles_export.csv`\n"
    "\n"
    "Replace `{account}` with the IG username (e.g. `chuckforqueens`).\n"
    "\n"
    "See *Getting Started* above for the full list.",
    help="Files are stored in memory for this session only. They are not saved permanently.",
)
data_uploads = st.sidebar.file_uploader(
    "Upload data files",
    type=["json", "csv", "txt"],
    key="data_upload",
    accept_multiple_files=True,
)
if data_uploads:
    for data_upload in data_uploads:
        dest = DATA_DIR / data_upload.name
        dest.write_bytes(data_upload.read())
        st.sidebar.success(f"Saved: {data_upload.name}")

# Show what's currently in data/
existing_files = sorted(DATA_DIR.glob("*"))
existing_files = [f for f in existing_files if f.is_file()]
if existing_files:
    st.sidebar.markdown("---")
    st.sidebar.caption(f"**Files loaded** ({len(existing_files)})")
    for f in existing_files:
        size_kb = f.stat().st_size / 1024
        st.sidebar.caption(f"  {f.name} ({size_kb:.0f} KB)")
