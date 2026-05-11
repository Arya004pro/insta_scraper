from __future__ import annotations

import os

import uvicorn


def main() -> None:
    # Reliable file-watch behavior on Windows dev setups.
    os.environ.setdefault("WATCHFILES_FORCE_POLLING", "true")

    uvicorn.run(
        "app.api.main:app",
        host="127.0.0.1",
        port=8000,
        reload=True,
        access_log=False,
        # Keep reload scope minimal for Windows stability.
        # Uvicorn still reloads on Python changes in app/*.
        reload_dirs=["app"],
        reload_includes=["*.py"],
        reload_excludes=["data/*", "exports/*", "Output/*", "scraped_media/*"],
    )


if __name__ == "__main__":
    main()
