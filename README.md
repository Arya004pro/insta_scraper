# Instagram Public Scraper (Playwright + FastAPI)

## Quick Start

```bash
pip install -e .[dev]
playwright install chromium
uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --reload
```

Brave-only required configuration:

```bash
set BRAVE_EXECUTABLE_PATH=C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe
set BRAVE_USER_DATA_DIR=C:\Users\<YOUR_USER>\AppData\Local\BraveSoftware\Brave-Browser\User Data
set BRAVE_PROFILE_DIRECTORY=Default
```

On Windows/macOS/Linux, the app now auto-detects common Brave install paths and user-data folders when these env vars are not set.

If Brave is currently open, the scraper can auto-clone your profile snapshot instead of failing:

```bash
set BRAVE_CLONE_PROFILE_WHEN_RUNNING=1
```

If you want strict behavior (fail when Brave is open):

```bash
set BRAVE_CLONE_PROFILE_WHEN_RUNNING=0
```

Optional external rotating proxy pool (JSON string):

```bash
set PROXY_POOL_JSON=[{"proxy_id":"p1","server":"http://host:port","username":"u","password":"p"}]
set PROXY_ROTATE_EVERY_N=20
```

Post cap control (0 or unset means scan all posts):

```bash
set MAX_POSTS_PER_PROFILE=0
```

Sample mode controls (default is enabled for quick test runs):

```bash
set SAMPLE_COLLECTION_MODE=1
```

In sample mode, the scraper targets exactly one sample each for:

- single image post
- multi image post
- reel

It first uses timeline snapshot data, then falls back to only currently visible grid items (no deep scroll), which helps reduce 429 rate-limit errors.

To force full crawling behavior instead of sample mode:

```bash
set SAMPLE_COLLECTION_MODE=0
```

Speed tuning controls:

```bash
set SCROLL_PAUSE_MIN_MS=450
set SCROLL_PAUSE_MAX_MS=900
set POST_DETAIL_WAIT_MS=300
set BROWSER_VIEWPORT_WIDTH=1100
set BROWSER_VIEWPORT_HEIGHT=750
```

Challenge auto-recovery controls (best effort before pausing run):

```bash
set CHALLENGE_AUTO_RETRY_ATTEMPTS=3
set CHALLENGE_AUTO_RETRY_WAIT_SECONDS=8
```

Optional headless mode for Brave:

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
- `GET /v1/runs/{run_id}`
- `POST /v1/runs/{run_id}/resume`
- `GET /v1/runs/{run_id}/artifacts`
- `GET /v1/runs/{run_id}/events`

## Output

Exports are written to `exports/`:

- `*_posts.csv`
- `*_reels.csv`
- `*_master_summary.csv`

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
- open source post/reel URLs used for samples
- download run artifacts (master, posts, reels)
- open local sample media files from `Output/`

