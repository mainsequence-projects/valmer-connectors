"""Backend selection for Valmer's own processes (ms-markets ADR 0047).

Valmer's processes reach Markets registries (assets, indexes, curves, pricing
market-data bindings, current pricing details) and the Markets tables they
publish into (asset snapshots, timestamped pricing details) through the Markets
API: ``use_markets_api()`` selects ms-markets' remote row backend, so
``Asset.upsert_many(...)``, ``Index.upsert_many(...)``, ``Curve.upsert(...)``,
``AssetSnapshots.publish(...)`` and the other ``msm.api`` row calls run on the
Markets API instead of holding MetaTables grants on ``mainsequence.markets``.
Valmer's own tables (namespace ``valmer_connectors``) keep the local MetaTables
runtime that ``bootstrap_runtime()`` attaches.

Only the composition roots call it: the Job scripts under ``scripts/``, the
``valmer-connectors`` CLI and the control-plane API. Library code never selects
a backend, because applications import ``valmer_connectors`` and choose their
own at their own composition root. ``bootstrap_runtime()`` attaches only
Valmer's own tables under either backend; an importing process that keeps
ms-markets' runtime backend attaches the Markets tables it uses itself.
"""

from __future__ import annotations

__all__ = ["markets_api_selected", "use_markets_api"]


def use_markets_api() -> None:
    """Send this process's Markets row calls to the Markets API (once per process).

    The Markets API is found in the caller's Environment on the first row call
    (``LazyRemoteBackend``), so a process that never touches a Markets registry
    never needs to see it. A workload reaches it with ``view`` on the
    ``MainSequenceMarkets`` branch; ``MARKETS_API_URL`` (and ``MARKETS_API_TOKEN``)
    select a local Markets API instead.
    """

    from msm.api.remote import active_remote_backend, use_remote_backend
    from msm.client import LazyRemoteBackend

    if active_remote_backend() is None:
        use_remote_backend(LazyRemoteBackend())


def markets_api_selected() -> bool:
    """Whether this process reaches Markets registries through the Markets API.

    True after ``use_markets_api()`` or with ``MSM_API_BACKEND=remote``; Valmer's
    asset helpers then skip their check for an attached ``AssetTable``.
    """

    from msm.api.remote import active_remote_backend

    return active_remote_backend() is not None
