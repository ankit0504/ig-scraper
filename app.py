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
import json
import time
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="IG Scraper", page_icon="📊", layout="wide")

# Resolve Apify token: Streamlit Secrets (deployed) or env var (local)
try:
    apify_token = st.secrets["APIFY_TOKEN"]
except (FileNotFoundError, KeyError):
    apify_token = os.environ.get("APIFY_TOKEN", "")
if apify_token:
    os.environ["APIFY_TOKEN"] = apify_token

# Now safe to import project modules
from apify_to_report import (
    DATA_DIR,
    convert_apify_to_csv,
    run_analysis,
    get_client,
    load_already_scraped,
    PROFILE_ACTOR,
)
from post_engagers import (
    scrape_post,
    build_engagers_csv,
    shortcode_from_url,
)

DATA_DIR.mkdir(exist_ok=True)

BATCH_SIZE = 1000


def capture_prints(fn, *args, **kwargs):
    """Run a function, capturing its print output. Returns (result, output_text)."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        result = fn(*args, **kwargs)
    return result, buf.getvalue()


def require_token():
    if not apify_token:
        st.error("No Apify token configured. Ask the app owner to set APIFY_TOKEN in Streamlit Secrets.")
        st.stop()


def save_upload(uploaded_file):
    """Save an uploaded file to the data/ directory and return the path."""
    dest = DATA_DIR / uploaded_file.name
    dest.write_bytes(uploaded_file.read())
    return dest


def parse_usernames_from_upload(uploaded_file) -> list[str]:
    """Extract usernames from an uploaded file (.txt or .json)."""
    content = uploaded_file.read().decode("utf-8")
    name = uploaded_file.name.lower()

    if name.endswith(".json"):
        data = json.loads(content)
        # Handle the followers export format: [{"handle": "...", ...}, ...]
        if isinstance(data, list) and data and isinstance(data[0], dict):
            # Try common key names
            for key in ("handle", "username", "user", "name"):
                if key in data[0]:
                    return [entry[key] for entry in data if entry.get(key)]
            st.error(f"JSON has keys {list(data[0].keys())} — expected 'handle' or 'username'.")
            st.stop()
        # Handle plain list of strings: ["user1", "user2"]
        if isinstance(data, list) and data and isinstance(data[0], str):
            return [u.strip() for u in data if u.strip()]
        st.error("Unrecognized JSON format. Expected a list of objects with 'handle'/'username', or a list of strings.")
        st.stop()
    else:
        # Plain text: one username per line
        return [
            line.strip().strip('"').strip(",").strip('"')
            for line in content.splitlines()
            if line.strip()
        ]


def show_reports(reports_dir: Path, key_prefix: str):
    """Display report CSVs with previews and download buttons."""
    if not reports_dir.exists():
        return
    csv_files = sorted(reports_dir.glob("*.csv"))
    if not csv_files:
        return
    st.subheader("Reports")
    for csv_path in csv_files:
        df = pd.read_csv(csv_path)
        with st.expander(f"{csv_path.stem} ({len(df)} rows)"):
            st.dataframe(df, use_container_width=True)
            st.download_button(
                f"Download {csv_path.name}",
                data=csv_path.read_bytes(),
                file_name=csv_path.name,
                mime="text/csv",
                key=f"{key_prefix}_{csv_path.stem}",
            )


# --- Sidebar ---
st.sidebar.title("IG Scraper")
st.sidebar.markdown(
    "Instagram profile & engagement analysis powered by [Apify](https://apify.com)."
)

tool = st.sidebar.radio(
    "Tool",
    ["Profile Scraper", "Post Engagers", "Analyze Existing Data"],
)

# --- Main content ---
st.title("IG Scraper")

with st.expander("How to use this tool", expanded=False):
    st.markdown("""
### Tools

- **Profile Scraper** — Upload a list of Instagram usernames and scrape their profile details
  (follower count, bio, verified status, etc.). Generates reports: noteworthy accounts,
  local collaborators, business accounts, unfollower detection, and more.
- **Post Engagers** — Paste an Instagram post URL to find out who liked and commented on it.
- **Analyze Existing Data** — Upload data from a previous scrape and re-run the analysis
  reports without making any API calls.

### How it works

1. Pick a tool from the sidebar
2. Enter the target IG account name and upload your files
3. Hit the button — results appear inline and can be downloaded as CSV

The Apify API token is already configured — you don't need to provide one.

### Important: upload existing data first!

If this account has been scraped before, **upload the previous results before scraping again**.
This prevents re-scraping profiles we already have and saves API credits. The Profile Scraper
page walks you through this in Step 1.
""")

st.markdown("---")

# =============================================================================
# Profile Scraper
# =============================================================================
if tool == "Profile Scraper":
    st.header("Profile Scraper")

    target = st.text_input(
        "Target account",
        placeholder="The IG username you're analyzing, e.g. chuckforqueens",
    )

    # --- Step 1: Upload existing data to avoid re-scraping ---
    name = target or "youraccountname"

    st.markdown("### Step 1: Upload existing data")
    st.warning(
        "**Have you scraped this account before?** Upload your previous results first "
        "so we skip already-scraped profiles and don't waste API credits. "
        "If this is your first time, skip to Step 2."
    )
    st.markdown(
        f"Rename your files to match these names, then upload them:\n"
        f"- **`{name}_apify_profiles_raw.json`** — raw Apify results from a previous scrape\n"
        f"- **`{name}_profiles_export.csv`** — enriched profiles CSV\n"
        f"- **`{name}_failed_enrichments.txt`** — usernames that previously failed\n"
    )
    existing_data_files = st.file_uploader(
        "Upload previous scrape data (optional)",
        type=["json", "csv", "txt"],
        accept_multiple_files=True,
        key="profile_existing_data",
    )
    if existing_data_files:
        for f in existing_data_files:
            save_upload(f)
            f.seek(0)
        st.success(f"Loaded {len(existing_data_files)} file(s): {', '.join(f.name for f in existing_data_files)}")

    st.markdown("### Step 2: Upload usernames to scrape")
    st.caption(
        "A **.txt** file with one username per line, or a **.json** followers export "
        "(list of objects with a `handle` or `username` field)."
    )
    uploaded_file = st.file_uploader(
        "Usernames file",
        type=["txt", "json"],
        key="profile_usernames",
        label_visibility="collapsed",
    )

    if st.button("Start Scraping", type="primary", disabled=not target or not uploaded_file):
        require_token()
        client = get_client()

        # Also save the file to data/ in case it's a followers export needed by analysis
        uploaded_file.seek(0)
        save_upload(uploaded_file)
        uploaded_file.seek(0)

        usernames = parse_usernames_from_upload(uploaded_file)
        st.info(f"Loaded **{len(usernames)}** usernames from {uploaded_file.name}")

        # Check already scraped
        _, _ = capture_prints(load_already_scraped, target)
        already_scraped = load_already_scraped(target)
        remaining = [u for u in usernames if u not in already_scraped]

        st.write(
            f"**Total:** {len(usernames)} · "
            f"**Already scraped:** {len(already_scraped)} · "
            f"**Remaining:** {len(remaining)}"
        )

        if not remaining:
            st.success("All profiles already scraped!")
        else:
            raw_file = DATA_DIR / f"{target}_apify_profiles_raw.json"
            existing: list[dict] = []
            if raw_file.exists():
                with open(raw_file) as f:
                    existing = json.load(f)

            total_batches = (len(remaining) + BATCH_SIZE - 1) // BATCH_SIZE
            progress = st.progress(0, text="Starting...")

            for i in range(0, len(remaining), BATCH_SIZE):
                batch = remaining[i : i + BATCH_SIZE]
                batch_num = (i // BATCH_SIZE) + 1
                progress.progress(
                    batch_num / total_batches,
                    text=f"Batch {batch_num}/{total_batches} ({len(batch)} usernames)",
                )

                try:
                    run = client.actor(PROFILE_ACTOR).start(
                        run_input={"usernames": batch},
                        memory_mbytes=4096,
                    )
                    run_id = run["id"]
                    dataset_id = run["defaultDatasetId"]

                    with st.status(f"Batch {batch_num}/{total_batches}", expanded=True) as status:
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
                        status.update(
                            label=f"Batch {batch_num} — {len(items)} profiles",
                            state="complete",
                        )

                    with open(raw_file, "w") as f:
                        json.dump(existing, f, indent=2)

                except Exception as e:
                    st.error(f"Batch {batch_num} failed: {e}")
                    st.info("Progress saved. Re-run to resume.")
                    break

            progress.progress(1.0, text="Scraping complete!")

        # Convert and analyze
        raw_file = DATA_DIR / f"{target}_apify_profiles_raw.json"
        if raw_file.exists():
            with st.status("Running analysis...") as status:
                _, convert_output = capture_prints(convert_apify_to_csv, raw_file, target)
                _, analysis_output = capture_prints(run_analysis, target)
                status.update(label="Analysis complete!", state="complete")

            with st.expander("Log"):
                st.code(convert_output + "\n" + analysis_output)

            show_reports(DATA_DIR / f"{target}_reports", "dl_scrape")

# =============================================================================
# Post Engagers
# =============================================================================
elif tool == "Post Engagers":
    st.header("Post Engagers")

    url = st.text_input(
        "Instagram post URL",
        placeholder="https://www.instagram.com/p/ABC123/",
    )

    if st.button("Scrape Engagers", type="primary", disabled=not url):
        require_token()
        client = get_client()

        st.subheader(f"Post: {shortcode_from_url(url)}")

        with st.status("Scraping likers and commenters...", expanded=True) as status:
            raw_data, scrape_output = capture_prints(scrape_post, client, url)
            st.code(scrape_output)
            status.update(label="Scraping complete!", state="complete")

        csv_path, csv_output = capture_prints(build_engagers_csv, raw_data)
        with st.expander("Summary"):
            st.code(csv_output)

        if csv_path and csv_path.exists():
            df = pd.read_csv(csv_path)
            st.dataframe(df, use_container_width=True)
            st.download_button(
                f"Download {csv_path.name}",
                data=csv_path.read_bytes(),
                file_name=csv_path.name,
                mime="text/csv",
            )

# =============================================================================
# Analyze Existing Data
# =============================================================================
elif tool == "Analyze Existing Data":
    st.header("Analyze Existing Data")
    st.caption("Upload data from a previous scrape and re-run analysis. No API calls made.")

    target = st.text_input(
        "Target account",
        placeholder="The IG username, e.g. chuckforqueens",
    )

    st.markdown("**Upload your data files**")

    if target:
        st.info(
            f"**Rename your files before uploading.** The tool matches files by name.\n\n"
            f"| What you have | Rename it to |\n"
            f"|---|---|\n"
            f"| Raw Apify scrape results | `{target}_apify_profiles_raw.json` |\n"
            f"| Enriched profiles CSV | `{target}_profiles_export.csv` |\n"
            f"| Followers list | `{target}_followers_export.json` (optional — enables unfollower detection) |\n"
            f"| Following list | `{target}_following_export.json` (optional — enables mutual follow detection) |\n"
            f"\n"
            f"You need at least one of the first two files."
        )
    else:
        st.info(
            "Enter the target account name above first — the expected file names depend on it."
        )

    uploaded_files = st.file_uploader(
        "Data files",
        type=["json", "csv"],
        accept_multiple_files=True,
        key="analyze_upload",
        label_visibility="collapsed",
    )

    if uploaded_files:
        for f in uploaded_files:
            dest = save_upload(f)
            f.seek(0)
        st.success(f"Uploaded {len(uploaded_files)} file(s): {', '.join(f.name for f in uploaded_files)}")

    # Detect available targets
    existing_targets = set()
    for f in DATA_DIR.glob("*_apify_profiles_raw.json"):
        existing_targets.add(f.stem.replace("_apify_profiles_raw", ""))
    for f in DATA_DIR.glob("*_profiles_export.csv"):
        existing_targets.add(f.stem.replace("_profiles_export", ""))

    if existing_targets and not target:
        st.info(f"Detected data for: **{', '.join(sorted(existing_targets))}** — enter one of these as the target account above.")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Run Profile Analysis", type="primary", disabled=not target):
            profiles_file = DATA_DIR / f"{target}_profiles_export.csv"
            raw_file = DATA_DIR / f"{target}_apify_profiles_raw.json"

            if raw_file.exists() and not profiles_file.exists():
                _, convert_output = capture_prints(convert_apify_to_csv, raw_file, target)
                with st.expander("Conversion log"):
                    st.code(convert_output)

            if not profiles_file.exists():
                st.error(
                    f"No data found for **{target}**. Make sure your uploaded files "
                    f"are named `{target}_apify_profiles_raw.json` or `{target}_profiles_export.csv`."
                )
                st.stop()

            _, output = capture_prints(run_analysis, target)
            with st.expander("Analysis log"):
                st.code(output)

            show_reports(DATA_DIR / f"{target}_reports", "dl_analyze")

    with col2:
        if st.button("Re-analyze Post Engagers", disabled=not target):
            engager_files = list(DATA_DIR.glob("post_*_engagers_raw.json"))
            if not engager_files:
                st.error("No post engager data found. Use the Post Engagers tool to scrape first.")
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
