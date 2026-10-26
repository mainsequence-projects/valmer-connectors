from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _runtime_backend_after_each_test():
    """Undo a Markets API selection that a composition root made during a test.

    Job scripts, the CLI and the API entry module select ms-markets' remote row
    backend process-wide (``valmer_connectors.runtime.use_markets_api``); tests
    that run them must not leak that choice into the next test.
    """

    yield
    from msm.api.remote import use_runtime_backend

    use_runtime_backend()
