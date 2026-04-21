# Instagram Public Scraper (Playwright + FastAPI)

## Quick Start

```bash
pip install -e .[dev]
camoufox fetch
uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --reload
```

Camoufox configuration:

```bash
set CAMOUFOX_EXECUTABLE_PATH=C:\Users\<YOUR_USER>\AppData\Local\camoufox\camoufox\Cache\camoufox.exe
set CAMOUFOX_USER_DATA_DIR=C:\Users\<YOUR_USER>\AppData\Local\camoufox\Profiles\<latest.default-profile>
set CAMOUFOX_CLONE_PROFILE_WHEN_RUNNING=1
```

If CAMOUFOX_EXECUTABLE_PATH is not set, the app tries to auto-detect from the installed camoufox package.

Output location (default):

```bash
set OUTPUT_ROOT_DIR=D:\Insta-scraper-camoufox
```

By default, CSV artifacts are written to D:\Insta-scraper-camoufox\exports and media files to D:\Insta-scraper-camoufox\media.

Optional external rotating proxy pool (JSON string):

```bash
set PROXY_POOL_JSON=[{"proxy_id":"p1","server":"http://host:port","username":"u","password":"p"}]
set PROXY_ROTATE_EVERY_N=20
```

Post/reel cap control (default is 50 mixed entities per profile):

```bash
set MAX_POSTS_PER_PROFILE=50
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
set BROWSER_VIEWPORT_WIDTH=1100
set BROWSER_VIEWPORT_HEIGHT=750
```

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
- Per-profile mixed content CSV (posts + reels): `media/<profile_name>/instagram_<username>_content_latest.csv`

The mixed content CSV includes a `content_type` column (`post` or `reel`), and `views_count` is kept blank for posts.

Sample media files are stored under `scraped_media/<profile_name>/` in the project root:

or under `Output/<profile_name>/` if using default settings.

- `posts/`
- `reels/`
- `multi_image_posts/`

Filenames are timestamped for latest extraction visibility:

- `<timestamp>_<runid8>_<shortcode>_<index>.<ext>`

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
- open local sample media files from `Output/`

