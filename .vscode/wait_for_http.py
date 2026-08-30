"""Wait for one local HTTP endpoint before starting a dependent VS Code launch."""

from __future__ import annotations

import sys
import time
import urllib.error
import urllib.request


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit("usage: wait_for_http.py URL TIMEOUT_SECONDS")

    url = sys.argv[1]
    deadline = time.monotonic() + float(sys.argv[2])
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as response:  # noqa: S310
                if response.status < 500:
                    print(f"Ready: {url}")
                    return 0
        except (OSError, urllib.error.URLError) as exc:
            last_error = exc
        time.sleep(0.25)

    detail = f": {last_error}" if last_error else ""
    print(f"Timed out waiting for {url}{detail}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
