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

Speed tuning controls:

```bash
set SCROLL_PAUSE_MIN_MS=450
set SCROLL_PAUSE_MAX_MS=900
set POST_DETAIL_WAIT_MS=300
set BROWSER_VIEWPORT_WIDTH=1100
set BROWSER_VIEWPORT_HEIGHT=750
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

- `*_normalized.xlsx`
- `*_master_summary.xlsx` (human-readable column names + readable IST timestamp)
- `*_posts.csv`
- `*_master_summary.csv`
- supporting CSVs for run log/profile/highlights/external links/aggregates

Normalized sheets keep `scraped_at_ist` as the first column.
Master summary uses readable labels (for example, `Scraped At (IST)`, `All Time Reel %`, and URL-based top post/reel columns).

