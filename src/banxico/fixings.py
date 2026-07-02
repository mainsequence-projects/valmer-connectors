"""Banxico-backed TIIE and CETE fixing builders."""

from __future__ import annotations

import datetime as dt
import os
import unicodedata
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import pandas as pd
from msm_pricing.data_nodes import FixingRatesNode, IndexFixingConfiguration

from banxico.settings import (
    BANXICO_FIXING_INDEX_IDENTIFIERS,
    BANXICO_TOKEN_SECRET_NAME,
    CETE_28_INDEX_IDENTIFIER,
    CETE_91_INDEX_IDENTIFIER,
    CETE_182_INDEX_IDENTIFIER,
    TIIE_28_INDEX_IDENTIFIER,
    TIIE_91_INDEX_IDENTIFIER,
    TIIE_182_INDEX_IDENTIFIER,
    TIIE_OVERNIGHT_INDEX_IDENTIFIER,
)
from banxico.sie import BanxicoSieClient

BANXICO_FIXING_OFFSET_START = dt.datetime(2010, 1, 1, tzinfo=dt.UTC)


class BanxicoFixingError(RuntimeError):
    """Raised when Banxico fixing configuration or payloads are invalid."""


class BanxicoFixingsNode(FixingRatesNode):
    """Fixing node for Banxico-published Mexican reference-rate observations."""

    OFFSET_START = BANXICO_FIXING_OFFSET_START


@dataclass(frozen=True)
class BanxicoSeriesDefinition:
    """Mapping from a pricing index identifier to a Banxico SIE series."""

    index_identifier: str
    series_id: str
    required_title_terms: tuple[str, ...]


@dataclass(frozen=True)
class BanxicoSeriesMetadata:
    """Validated Banxico metadata for one accepted fixing series."""

    index_identifier: str
    series_id: str
    title: str
    unit: str | None = None


@dataclass(frozen=True)
class BanxicoUpdateWindow:
    """Inclusive Banxico SIE date-range request window."""

    start_date: dt.date
    end_date: dt.date


DEFAULT_SERIES_DEFINITIONS: tuple[BanxicoSeriesDefinition, ...] = (
    BanxicoSeriesDefinition(TIIE_OVERNIGHT_INDEX_IDENTIFIER, "SF331451", ("FONDEO",)),
    BanxicoSeriesDefinition(TIIE_28_INDEX_IDENTIFIER, "SF43783", ("TIIE", "28")),
    BanxicoSeriesDefinition(TIIE_91_INDEX_IDENTIFIER, "SF43878", ("TIIE", "91")),
    BanxicoSeriesDefinition(TIIE_182_INDEX_IDENTIFIER, "SF111916", ("TIIE", "182")),
    BanxicoSeriesDefinition(CETE_28_INDEX_IDENTIFIER, "SF45470", ("CETE", "28")),
    BanxicoSeriesDefinition(CETE_91_INDEX_IDENTIFIER, "SF45471", ("CETE", "91")),
    BanxicoSeriesDefinition(CETE_182_INDEX_IDENTIFIER, "SF45472", ("CETE", "182")),
)


def resolve_banxico_token(
    *,
    secret_name: str = BANXICO_TOKEN_SECRET_NAME,
    environ: Mapping[str, str] | None = None,
) -> str:
    """Resolve the Banxico API token from env first, then Main Sequence Secret."""

    env = environ or os.environ
    token = (env.get(secret_name) or "").strip()
    if token:
        return token

    try:
        from mainsequence.client import Secret

        secret = Secret.get(name=secret_name)
        secret_value = secret.value
        secret_uid = getattr(secret, "uid", None)
        if secret_value is None and secret_uid:
            secret_value = Secret.get_by_uid(secret_uid).value
        token = (
            secret_value.get_secret_value()
            if hasattr(secret_value, "get_secret_value")
            else str(secret_value or "")
        ).strip()
    except Exception as exc:
        raise BanxicoFixingError(
            f"Main Sequence Secret {secret_name!r} is required for Banxico SIE access."
        ) from exc

    if not token:
        raise BanxicoFixingError(f"Main Sequence Secret {secret_name!r} is empty.")
    return token


def definitions_by_index_identifier(
    definitions: Iterable[BanxicoSeriesDefinition] = DEFAULT_SERIES_DEFINITIONS,
) -> dict[str, BanxicoSeriesDefinition]:
    """Return Banxico series definitions keyed by pricing index identifier."""

    items = tuple(definitions)
    resolved = {item.index_identifier: item for item in items}
    if len(resolved) != len(items):
        raise BanxicoFixingError("Banxico series definitions contain duplicate index identifiers.")
    return resolved


def select_series_definitions(
    index_identifiers: Iterable[str] | None = None,
    *,
    definitions: Iterable[BanxicoSeriesDefinition] = DEFAULT_SERIES_DEFINITIONS,
) -> tuple[BanxicoSeriesDefinition, ...]:
    """Resolve the requested fixing indexes to Banxico series definitions."""

    by_index = definitions_by_index_identifier(definitions)
    if index_identifiers is None:
        return tuple(by_index.values())

    selected = []
    for index_identifier in index_identifiers:
        try:
            selected.append(by_index[index_identifier])
        except KeyError as exc:
            raise BanxicoFixingError(
                f"Unsupported Banxico fixing index identifier {index_identifier!r}."
            ) from exc
    return tuple(selected)


def validate_series_metadata(
    metadata_payloads: Iterable[Mapping[str, Any]],
    definitions: Iterable[BanxicoSeriesDefinition],
) -> dict[str, BanxicoSeriesMetadata]:
    """Validate Banxico SIE metadata against accepted pricing index coverage."""

    payload_by_series_id = {
        str(item.get("idSerie") or item.get("idserie") or "").strip(): item
        for item in metadata_payloads
    }
    validated: dict[str, BanxicoSeriesMetadata] = {}
    for definition in definitions:
        payload = payload_by_series_id.get(definition.series_id)
        if payload is None:
            raise BanxicoFixingError(
                f"Banxico SIE metadata did not include series {definition.series_id!r}."
            )
        title = str(payload.get("titulo") or "").strip()
        if not title:
            raise BanxicoFixingError(
                f"Banxico SIE metadata for {definition.series_id!r} has no title."
            )
        normalized_title = _normalize_text(title)
        missing_terms = [
            term
            for term in definition.required_title_terms
            if _normalize_text(term) not in normalized_title
        ]
        if missing_terms:
            raise BanxicoFixingError(
                f"Banxico SIE series {definition.series_id!r} title {title!r} "
                f"does not match {definition.index_identifier!r}; missing terms "
                f"{missing_terms!r}."
            )
        unit = payload.get("unidad") or payload.get("unidadMedida")
        validated[definition.index_identifier] = BanxicoSeriesMetadata(
            index_identifier=definition.index_identifier,
            series_id=definition.series_id,
            title=title,
            unit=str(unit).strip() if unit else None,
        )
    return validated


def build_banxico_fixing_frame(
    *,
    update_statistics,
    index_identifier: str,
    client: BanxicoSieClient,
    definitions: Iterable[BanxicoSeriesDefinition] = DEFAULT_SERIES_DEFINITIONS,
    end_date: dt.date | str | pd.Timestamp | None = None,
    offset_start: dt.date | dt.datetime | str | pd.Timestamp = BANXICO_FIXING_OFFSET_START,
) -> pd.DataFrame:
    """Build one Banxico fixing frame for the requested pricing index identifier."""

    definition = definitions_by_index_identifier(definitions).get(index_identifier)
    if definition is None:
        raise BanxicoFixingError(
            f"Unsupported Banxico fixing index identifier {index_identifier!r}."
        )
    window = resolve_update_window(
        update_statistics=update_statistics,
        index_identifier=index_identifier,
        end_date=end_date,
        offset_start=offset_start,
    )
    if window is None:
        return empty_fixing_frame()

    payloads = client.fetch_series_data(
        [definition.series_id],
        start_date=window.start_date,
        end_date=window.end_date,
    )
    return normalize_banxico_observations(
        payloads,
        {definition.series_id: index_identifier},
    )


def resolve_update_window(
    *,
    update_statistics,
    index_identifier: str,
    end_date: dt.date | str | pd.Timestamp | None = None,
    offset_start: dt.date | dt.datetime | str | pd.Timestamp = BANXICO_FIXING_OFFSET_START,
) -> BanxicoUpdateWindow | None:
    """Resolve the inclusive SIE request window for one fixing index."""

    resolved_end = _normalize_date(
        end_date
        if end_date is not None
        else pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=1)
    )
    last_update = _last_update_for_identity(update_statistics, index_identifier)
    if last_update is None:
        resolved_start = _normalize_date(offset_start)
    else:
        resolved_start = _normalize_date(last_update) + dt.timedelta(days=1)
    if resolved_start > resolved_end:
        return None
    return BanxicoUpdateWindow(start_date=resolved_start, end_date=resolved_end)


def normalize_banxico_observations(
    series_payloads: Iterable[Mapping[str, Any]],
    series_id_to_index_identifier: Mapping[str, str],
) -> pd.DataFrame:
    """Normalize Banxico SIE observations to `IndexFixingsStorage` frame shape."""

    rows: list[dict[str, Any]] = []
    for series_payload in series_payloads:
        series_id = str(series_payload.get("idSerie") or series_payload.get("idserie") or "")
        series_id = series_id.strip()
        try:
            index_identifier = series_id_to_index_identifier[series_id]
        except KeyError as exc:
            raise BanxicoFixingError(
                f"Unexpected Banxico SIE series id {series_id!r} in observation payload."
            ) from exc

        data = series_payload.get("datos") or []
        if not isinstance(data, list):
            raise BanxicoFixingError(f"Banxico SIE series {series_id!r} datos must be a list.")
        for item in data:
            if not isinstance(item, Mapping):
                continue
            rows.append(
                {
                    "time_index": item.get("fecha"),
                    "index_identifier": index_identifier,
                    "source_value": item.get("dato"),
                }
            )

    if not rows:
        return empty_fixing_frame()

    frame = pd.DataFrame(rows)
    frame["time_index"] = pd.to_datetime(
        frame["time_index"],
        format="%d/%m/%Y",
        utc=True,
        errors="raise",
    )
    frame["source_value"] = pd.to_numeric(frame["source_value"], errors="coerce")
    frame = frame.dropna(subset=["source_value"])
    if frame.empty:
        return empty_fixing_frame()

    frame["rate"] = frame["source_value"] / 100.0
    time_index = pd.DatetimeIndex(
        frame["time_index"],
        name="time_index",
        dtype="datetime64[ns, UTC]",
    )
    normalized = pd.DataFrame(
        {
            "index_identifier": frame["index_identifier"].astype(str).to_numpy(),
            "rate": frame["rate"].astype(float).to_numpy(),
        },
        index=time_index,
    )
    return normalized.set_index("index_identifier", append=True).sort_index()


def empty_fixing_frame() -> pd.DataFrame:
    """Return an empty frame with the Banxico fixing builder contract columns."""

    return pd.DataFrame(columns=["time_index", "index_identifier", "rate"])


def make_banxico_fixing_builders(
    *,
    client: BanxicoSieClient,
    definitions: Iterable[BanxicoSeriesDefinition] = DEFAULT_SERIES_DEFINITIONS,
    index_identifiers: Iterable[str] | None = None,
    validate_metadata: bool = True,
    end_date: dt.date | str | pd.Timestamp | None = None,
    offset_start: dt.date | dt.datetime | str | pd.Timestamp = BANXICO_FIXING_OFFSET_START,
):
    """Create `FixingRatesNode` builders keyed by pricing index identifier."""

    selected = select_series_definitions(index_identifiers, definitions=definitions)
    if validate_metadata:
        metadata_payloads = client.fetch_series_metadata(item.series_id for item in selected)
        validate_series_metadata(metadata_payloads, selected)

    builders = {}
    for definition in selected:

        def build_frame(*, update_statistics, index_identifier: str, _definition=definition):
            if index_identifier != _definition.index_identifier:
                raise BanxicoFixingError(
                    f"Builder for {_definition.index_identifier!r} received "
                    f"{index_identifier!r}."
                )
            return build_banxico_fixing_frame(
                update_statistics=update_statistics,
                index_identifier=index_identifier,
                client=client,
                definitions=selected,
                end_date=end_date,
                offset_start=offset_start,
            )

        builders[definition.index_identifier] = build_frame
    return builders


def run_banxico_fixings_update(
    *,
    index_identifiers: Iterable[str] | None = None,
    token: str | None = None,
    token_secret_name: str = BANXICO_TOKEN_SECRET_NAME,
    validate_metadata: bool = True,
    end_date: dt.date | str | pd.Timestamp | None = None,
    offset_start: dt.date | dt.datetime | str | pd.Timestamp = BANXICO_FIXING_OFFSET_START,
    hash_namespace: str | None = None,
    force_update: bool = True,
) -> None:
    """Run Banxico TIIE/CETE fixings through the current pricing fixing node."""

    from valmer_connectors.instruments.bootstrap import bootstrap_runtime

    selected = select_series_definitions(index_identifiers)
    bootstrap_runtime()
    client = BanxicoSieClient(token=token or resolve_banxico_token(secret_name=token_secret_name))
    builders = make_banxico_fixing_builders(
        client=client,
        definitions=selected,
        validate_metadata=validate_metadata,
        end_date=end_date,
        offset_start=offset_start,
    )
    node_kwargs = {}
    if hash_namespace:
        node_kwargs["hash_namespace"] = hash_namespace
    node = BanxicoFixingsNode(
        fixing_config=IndexFixingConfiguration(
            index_unique_identifiers=[item.index_identifier for item in selected],
        ),
        **node_kwargs,
    ).set_fixing_builders(builders)
    node.run(force_update=force_update)


def _last_update_for_identity(update_statistics, index_identifier: str) -> Any | None:
    getter = getattr(update_statistics, "get_last_update_for_identity", None)
    if callable(getter):
        return getter(index_identifier)
    asset_time_statistics = getattr(update_statistics, "asset_time_statistics", None) or {}
    return asset_time_statistics.get(index_identifier)


def _normalize_date(value: dt.date | dt.datetime | str | pd.Timestamp) -> dt.date:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.normalize().date()


def _normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    without_marks = "".join(char for char in normalized if not unicodedata.combining(char))
    return without_marks.upper()


__all__ = [
    "BANXICO_FIXING_INDEX_IDENTIFIERS",
    "DEFAULT_SERIES_DEFINITIONS",
    "BanxicoFixingError",
    "BanxicoFixingsNode",
    "BanxicoSeriesDefinition",
    "BanxicoSeriesMetadata",
    "BanxicoUpdateWindow",
    "build_banxico_fixing_frame",
    "empty_fixing_frame",
    "make_banxico_fixing_builders",
    "normalize_banxico_observations",
    "resolve_banxico_token",
    "resolve_update_window",
    "run_banxico_fixings_update",
    "select_series_definitions",
    "validate_series_metadata",
]
