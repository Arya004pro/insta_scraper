# Instagram Public Scraper (Playwright + FastAPI)

## Quick Start

```bash
pip install -e .[dev]
uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --reload
```

Open a dedicated Opera GX window for scraping (uses the same profile as the scraper):

```bash
open_opera_gx_scraper.bat
```

Opera GX configuration:

```bash
set OPERA_GX_EXECUTABLE_PATH=C:\Users\<YOUR_USER>\AppData\Local\Programs\Opera GX\opera.exe
set OPERA_GX_USER_DATA_DIR=C:\Users\<YOUR_USER>\AppData\Roaming\Opera Software\Opera GX Stable
set OPERA_GX_CLONE_PROFILE_WHEN_RUNNING=1
set OPERA_GX_USE_FRESH_PROFILE=1
```

If OPERA_GX_EXECUTABLE_PATH is not set, the app tries to auto-detect from common Opera GX install paths.
If OPERA_GX_USER_DATA_DIR is not set, the scraper uses `data/browser_state/opera_gx_user_data` (the same profile used by `open_opera_gx_scraper.bat`).
`OPERA_GX_USE_FRESH_PROFILE=1` (default) launches a clean temporary Opera profile per run and applies saved Instagram cookies to reduce tab storms/challenge loops from normal profile extensions/sessions.

Output location (default):

```bash
set OUTPUT_ROOT_DIR=D:\Insta-scraper-opera-gx
```

By default, CSV artifacts are written to D:\Insta-scraper-opera-gx\exports and media files to D:\Insta-scraper-opera-gx\media.

Media download control (default is disabled):

```bash
set DOWNLOAD_MEDIA_ASSETS=0
```

Optional external rotating proxy pool (JSON string):

```bash
set PROXY_POOL_JSON=[{"proxy_id":"p1","server":"http://host:port","username":"u","password":"p"}]
set PROXY_ROTATE_EVERY_N=20
```

Post/reel cap control (default is no cap, scrape full profile):

```bash
set MAX_POSTS_PER_PROFILE=50
```

Set to 0 for no cap (scrape full profile):

```bash
set MAX_POSTS_PER_PROFILE=0
```

Sample mode controls (default is disabled so UI test runs can capture up to the configured entity cap):

```bash
set SAMPLE_COLLECTION_MODE=0
```

In sample mode, the scraper targets exactly one sample each for:

- single image post
- multi image post
- reel

It first uses timeline snapshot data, then falls back to only currently visible grid items (no deep scroll), which helps reduce 429 rate-limit errors.

To force sample mode (quick profile sanity runs):

```bash
set SAMPLE_COLLECTION_MODE=1
```

Speed tuning controls:

```bash
set SCROLL_PAUSE_MIN_MS=450
set SCROLL_PAUSE_MAX_MS=900
set POST_DETAIL_WAIT_MS=300
set POST_DETAIL_NAV_TIMEOUT_MS=12000
set SCRAPE_RUNTIME_BUDGET_SECONDS=900
set BROWSER_START_MAXIMIZED=1
set BROWSER_VIEWPORT_WIDTH=1100
set BROWSER_VIEWPORT_HEIGHT=750
```

`BROWSER_START_MAXIMIZED=1` (default) opens the headed Opera GX scraper window maximized; viewport width/height are used when maximization is disabled or headless mode is on.

VPN rotation + skip list controls:

```bash
set VPN_ROTATE_EVERY_N=50
set VPN_ROTATE_CMD="C:\\Users\\<YOUR_USER>\\Desktop\\Insta scraper\\scripts\\vpn\\vpn_rotate.bat"
set VPN_ROTATE_WAIT_SECONDS=6
set REELS_TAB_MAX_ITEMS=120
set REELS_TAB_MAX_SCROLL_ROUNDS=20
set SKIP_MEDIA_SHORTCODES=ABC123,XYZ789
set SKIP_MEDIA_URLS=https://www.instagram.com/p/ABC123/,https://www.instagram.com/reel/XYZ789/
```

If `VPN_ROTATE_CMD` is set and `VPN_ROTATE_EVERY_N` is omitted or `0`, the app defaults rotation interval to `50`.

Auto-rotate Opera GX VPN (Windows):

1) Install AutoHotkey v2.
2) Open Opera GX, open the VPN panel in the URL bar.
3) Run `scripts\\vpn\\vpn_calibrate.ahk` once to capture click points. This clears old VPN coordinate/state recordings first.
4) Set `VPN_ROTATE_CMD` as above.

Default rotation sequence is `Americas -> Asia -> Europe` (because Opera VPN already starts on `Optimal` by default).
If you want `Optimal` included in rotation, use:
`set VPN_ROTATE_CMD="C:\\Users\\<YOUR_USER>\\Desktop\\Insta scraper\\scripts\\vpn\\vpn_rotate.bat --with-optimal"`

If `VPN_ROTATE_CMD` is not set, the run pauses after each batch and asks you to switch VPN location, then resume.

`SCRAPE_RUNTIME_BUDGET_SECONDS` is enforced in reels-only stats mode so runs return partial-safe CSV output instead of hanging for very long periods.

Challenge auto-recovery controls (best effort before pausing run):

```bash
set CHALLENGE_AUTO_RETRY_ATTEMPTS=3
set CHALLENGE_AUTO_RETRY_WAIT_SECONDS=8
```

Default mode opens a visible browser window (headed mode), so you can spectate while scraping continues automatically.

Optional headless mode (hide browser window):

```bash
set BROWSER_HEADLESS=1
```

## API

- `POST /v1/runs/start`
  - Body:
    - `input_type`: `single_url` or `csv_file`
    - `input_value`: profile URL or local CSV path
    - `use_saved_session`: bool
    - `proxy_pool_id`: optional
    - `max_entities`: optional int (for example `50`)
    - `fast_mode`: optional bool
    - `reels_only`: optional bool
    - `stats_only`: optional bool (skip media downloads but keep metadata extraction)
- `GET /v1/runs/{run_id}`
- `POST /v1/runs/{run_id}/resume`
- `GET /v1/runs/{run_id}/artifacts`
- `GET /v1/runs/{run_id}/events`

## Output

Exports are written as two CSV outputs:

- Global rollup (all scraped profiles): `exports/instagram_profiles_rollup.csv`
- Profile bios (one row per profile): `exports/instagram_profiles_bio.csv`
- Per-profile mixed content CSV (posts + reels): `media/<profile_name>/instagram_<username>_content_latest.csv`

The mixed content CSV includes a `content_type` column (`post` or `reel`), and `views_count` is kept blank for posts.

Sample media downloads are disabled by default. To enable them, set:

```bash
set DOWNLOAD_MEDIA_ASSETS=1
```

When enabled, sample media files are stored under `media/<profile_name>/` with subfolders:

- `posts/`
- `reels/`

`profile.csv` now includes About fields: `date_joined`, `account_based_in`, `active_ads_status`, `active_ads_url`, and `time_verified`.

Master summary uses readable labels (for example, `Scraped At (IST)`, `All Time Reel %`, and URL-based top post/reel columns).

## Local UI

Start API server:

```bash
uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --reload
```

Open dashboard:

- `http://127.0.0.1:8000/`

The UI lets you:

- provide a profile URL and start a run
- watch live run status
- view sample rows for single-image post, multi-image post, and reel
- view all captured post/reel outputs with media previews and metrics (likes, views, comments, hashtags, keywords, mentions)
- open source post/reel URLs used for samples
- download run artifacts (master, posts, reels)
- open local sample media files when `DOWNLOAD_MEDIA_ASSETS=1` (under `media/<profile_name>/`)
