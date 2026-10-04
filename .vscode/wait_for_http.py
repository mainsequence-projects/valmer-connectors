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
        status: int | None = None
        try:
            with urllib.request.urlopen(url, timeout=1) as response:  # noqa: S310
                status = response.status
        except urllib.error.HTTPError as exc:
            # Any answer below 500 means the server is up, including the 401 of a
            # route that needs an authenticated caller.
            status = exc.code
            last_error = exc
            exc.close()
        except (OSError, urllib.error.URLError) as exc:
            last_error = exc
        if status is not None and status < 500:
            print(f"Ready: {url} (HTTP {status})")
            return 0
        time.sleep(0.25)

    detail = f": {last_error}" if last_error else ""
    print(f"Timed out waiting for {url}{detail}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
