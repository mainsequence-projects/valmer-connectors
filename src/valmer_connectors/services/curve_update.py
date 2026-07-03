from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from typing import Any

import pandas as pd
import structlog
from msm_pricing.data_nodes import CurveConfig, DiscountCurvesNode

from mainsequence.client import UpdateStatistics
from valmer_connectors.instruments.bootstrap import bootstrap_runtime
from valmer_connectors.instruments.curve_bootstrap import (
    VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_UNIQUE_IDENTIFIER,
    VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    configure_valmer_discount_curves_cadence,
)
from valmer_connectors.instruments.curve_key_nodes import (
    validate_mxn_government_key_nodes,
    validate_tiie_ois_key_nodes,
    validate_usd_mxn_xccy_key_nodes,
    validate_usd_sofr_key_nodes,
)
from valmer_connectors.instruments.mexican_government_bond_curve import (
    MexicanGovernmentBondCurveError,
    build_mxn_government_curve_frame,
    select_mxn_government_bootstrap_instruments,
)
from valmer_connectors.instruments.rates_curves import (
    build_tiie_irs_mxn_valmer,
    build_usd_mxn_xccy_valmer,
    build_usd_sofr_valmer,
)

CurveBuilder = Callable[..., pd.DataFrame]
KeyNodesValidator = Callable[..., Any]
LOGGER = structlog.get_logger(__name__)
VALMER_MXN_GOVERNMENT_BOND_OFFSET_START = dt.datetime(2026, 6, 1, tzinfo=dt.UTC)
_VECTOR_CURVE_SOURCE_COLUMNS = [
    "time_index",
    "unique_identifier",
    "fecha",
    "preciolimpio",
    "preciosucio",
    "interesesacumulados",
    "diastransccpn",
    "valornominalactualizado",
    "tipovalor",
    "emisora",
    "serie",
    "sector",
    "fechaemision",
    "fechavcto",
    "valornominal",
    "monedaemision",
    "freccpn",
    "tasacupon",
    "yield_rate",
]
_DISCOUNT_CURVE_REQUIRED_PAYLOAD_COLUMNS = frozenset({"curve", "key_nodes"})


class ValmerMxnGovernmentBondDiscountCurvesNode(DiscountCurvesNode):
    """Government curve DataNode with the Valmer vector availability boundary."""

    OFFSET_START = VALMER_MXN_GOVERNMENT_BOND_OFFSET_START


def _run_valmer_discount_curve_update(
    *,
    curve_identifier: str,
    curve_builder: CurveBuilder,
    logger=None,
    node_class: type[DiscountCurvesNode] | None = None,
    key_nodes_validator: KeyNodesValidator | None = None,
    hash_namespace: str | None = None,
    rebuild_current: bool = False,
) -> None:
    """Run a Valmer discount curve builder through the canonical pricing node."""

    bootstrap_runtime()
    configure_valmer_discount_curves_cadence()
    resolved_node_class = node_class or DiscountCurvesNode
    node_kwargs = {}
    if hash_namespace:
        node_kwargs["hash_namespace"] = hash_namespace
    node = resolved_node_class(
        curve_config=CurveConfig(curve_unique_identifier=curve_identifier),
        **node_kwargs,
    ).set_curve_builder(
        _with_curve_summary_logging(
            curve_builder,
            logger=logger or LOGGER,
        )
    )
    if key_nodes_validator is not None:
        node = node.set_key_nodes_validator(key_nodes_validator)
    run_kwargs = {"force_update": True}
    if rebuild_current:
        run_kwargs["override_update_stats"] = UpdateStatistics.return_empty()
    node.run(**run_kwargs)


def _with_curve_summary_logging(
    curve_builder: CurveBuilder,
    *,
    logger,
) -> CurveBuilder:
    def build_curve(**kwargs: Any) -> pd.DataFrame:
        frame = curve_builder(**kwargs)
        frame = _ensure_discount_curve_storage_contract(frame)
        _log_curve_frame_summary(frame, logger=logger)
        return frame

    return build_curve


def _ensure_discount_curve_storage_contract(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a frame compatible with the backend DiscountCurvesStorage contract."""

    normalized = frame.copy()
    columns = set(normalized.reset_index().columns)
    missing_required = sorted(_DISCOUNT_CURVE_REQUIRED_PAYLOAD_COLUMNS - columns)
    if missing_required:
        raise ValueError(
            "Valmer discount curve builders must include storage contract columns: "
            f"{missing_required}."
        )
    return normalized


def _log_curve_frame_summary(frame: pd.DataFrame, *, logger) -> None:
    if frame.empty:
        logger.warning("Valmer curve builder returned an empty frame.")
        return

    for row in frame.reset_index().itertuples(index=False):
        curve_points = getattr(row, "curve")
        logger.info(
            "Built Valmer discount curve "
            f"{getattr(row, 'curve_identifier')} for {getattr(row, 'time_index')} "
            f"with {len(curve_points)} pillars."
        )


def run_tiie_irs_mxn_curve_update(
    *,
    curve_identifier: str = VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
) -> None:
    """Publish the Valmer TIIE overnight OIS curve from IRS_MXN_CURVE."""

    _run_valmer_discount_curve_update(
        curve_identifier=curve_identifier,
        curve_builder=build_tiie_irs_mxn_valmer,
        key_nodes_validator=validate_tiie_ois_key_nodes,
    )


def run_usd_sofr_curve_update(
    *,
    curve_identifier: str = VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
) -> None:
    """Publish the Valmer USD SOFR overnight OIS curve from IRS_USD_CURVE."""

    _run_valmer_discount_curve_update(
        curve_identifier=curve_identifier,
        curve_builder=build_usd_sofr_valmer,
        key_nodes_validator=validate_usd_sofr_key_nodes,
    )


def run_usd_mxn_xccy_curve_update(
    *,
    curve_identifier: str = VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_UNIQUE_IDENTIFIER,
    hash_namespace: str | None = None,
    rebuild_current: bool = False,
) -> None:
    """Publish the Valmer USD/MXN F-TIIE/SOFR cross-currency curve."""

    _run_valmer_discount_curve_update(
        curve_identifier=curve_identifier,
        curve_builder=build_usd_mxn_xccy_valmer,
        key_nodes_validator=validate_usd_mxn_xccy_key_nodes,
        hash_namespace=hash_namespace,
        rebuild_current=rebuild_current,
    )


def run_mxn_government_curve_update(
    *,
    curve_identifier: str = VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    bucket_name: str | None = None,
    debug_artifact_path: str | None = None,
) -> None:
    """Publish the Valmer MXN government bond curve through DiscountCurvesNode."""

    if bucket_name is not None or debug_artifact_path is not None:
        LOGGER.warning(
            "Ignoring MXN government curve raw-source options; curve input is loaded "
            "from ValmerVectorPricesStorage joined to ValmerAssetDetailsTable."
        )

    def build_curve(**kwargs):
        return build_mxn_government_curve_from_vector_storage(
            **kwargs,
            logger=LOGGER,
        )

    _run_valmer_discount_curve_update(
        curve_identifier=curve_identifier,
        curve_builder=build_curve,
        logger=LOGGER,
        node_class=ValmerMxnGovernmentBondDiscountCurvesNode,
        key_nodes_validator=validate_mxn_government_key_nodes,
    )


def build_mxn_government_curve_from_vector_storage(
    *,
    update_statistics,
    curve_identifier: str,
    base_node_curve_points=None,
    logger=None,
) -> pd.DataFrame:
    """Build MXN government curve rows from persisted vector storage snapshots."""

    _ = base_node_curve_points
    last_update = _last_curve_update_time(update_statistics, curve_identifier)
    if last_update is None:
        source_data = load_mxn_government_curve_source_from_vector_storage(
            start_time_index=ValmerMxnGovernmentBondDiscountCurvesNode.OFFSET_START,
            logger=logger,
        )
    else:
        source_data = load_mxn_government_curve_source_from_vector_storage(
            after_time_index=last_update,
            logger=logger,
        )
    return build_mxn_government_curve_frame_from_source_snapshots(
        source_data,
        curve_identifier=curve_identifier,
        logger=logger,
    )


def build_mxn_government_curve_frame_from_source_snapshots(
    source_df: pd.DataFrame,
    *,
    curve_identifier: str,
    logger=None,
) -> pd.DataFrame:
    """Build one government curve row per vector-storage snapshot."""

    if source_df.empty:
        if logger is not None:
            logger.warning(
                "No Valmer vector storage snapshots available for curve build."
            )
        return _empty_curve_frame()
    if "time_index" not in source_df.columns:
        raise MexicanGovernmentBondCurveError(
            "Vector storage curve source is missing required time_index column."
        )

    working = source_df.copy()
    working["time_index"] = pd.to_datetime(working["time_index"], utc=True)
    frames: list[pd.DataFrame] = []
    for time_index, group in working.sort_values("time_index").groupby(
        "time_index",
        sort=True,
    ):
        selected = select_mxn_government_bootstrap_instruments(group)
        selected_counts = (
            selected.groupby(["tipovalor", "emisora"], dropna=False).size().to_dict()
        )
        if logger is not None:
            logger.info(
                "Selected "
                f"{len(selected)} Valmer MXN government curve rows from "
                f"{len(group)} vector storage rows for {time_index}. "
                f"Selected families: {selected_counts}."
            )
        frames.append(
            build_mxn_government_curve_frame(
                group,
                curve_identifier=curve_identifier,
            )
        )
    if not frames:
        return _empty_curve_frame()
    return pd.concat(frames).sort_index()


def load_mxn_government_curve_source_from_vector_storage(
    *,
    time_index: Any | None = None,
    start_time_index: Any | None = None,
    after_time_index: Any | None = None,
    timeout: int | float | tuple[float, float] | None = None,
    logger=None,
) -> pd.DataFrame:
    """Load available CETES and M Bonos from the published Valmer vector snapshot."""

    supplied_bounds = [
        value is not None for value in (time_index, start_time_index, after_time_index)
    ]
    if sum(supplied_bounds) > 1:
        raise ValueError(
            "Use only one of time_index, start_time_index, or after_time_index."
        )

    from msm.api.base import operation_result_rows
    from msm.repositories.base import compile_markets_statement, execute_markets_operation
    from sqlalchemy import and_, func, or_, select

    from valmer_connectors.data_nodes.valmer_vector_storage import (
        ValmerVectorPricesStorage,
    )
    from valmer_connectors.meta_tables.valmer_asset_details import (
        ValmerAssetDetailsTable,
        ensure_valmer_asset_detail_runtime,
    )

    context = ensure_valmer_asset_detail_runtime(timeout=timeout)
    vector_table = ValmerVectorPricesStorage.__table__
    details_table = ValmerAssetDetailsTable.__table__
    joined = vector_table.join(
        details_table,
        details_table.c.valmer_unique_identifier == vector_table.c.asset_identifier,
    )
    instrument_filter = or_(
        and_(
            details_table.c.security_type == "BI",
            details_table.c.issuer == "CETES",
        ),
        and_(
            details_table.c.security_type == "M",
            details_table.c.issuer == "BONOS",
        ),
    )
    base_filter = and_(
        instrument_filter,
        details_table.c.issue_currency == "MPS",
    )
    filters = [base_filter]
    selected_time_index = None
    query_label = "latest"
    allow_empty = start_time_index is not None or after_time_index is not None
    if time_index is not None:
        selected_time_index = _normalize_query_time_index(time_index)
        filters.append(vector_table.c.time_index == selected_time_index)
        query_label = f"at {selected_time_index}"
    elif after_time_index is not None:
        selected_after_time_index = _normalize_query_time_index(after_time_index)
        filters.append(vector_table.c.time_index > selected_after_time_index)
        query_label = f"after {selected_after_time_index}"
    elif start_time_index is not None:
        selected_start_time_index = _normalize_query_time_index(start_time_index)
        filters.append(vector_table.c.time_index >= selected_start_time_index)
        query_label = f"from {selected_start_time_index}"

    if selected_time_index is None:
        if start_time_index is None and after_time_index is None:
            latest_statement = (
                select(func.max(vector_table.c.time_index).label("time_index"))
                .select_from(joined)
                .where(base_filter)
            )
            latest_operation = compile_markets_statement(
                latest_statement,
                context=context,
                operation="select",
                models=[ValmerVectorPricesStorage, ValmerAssetDetailsTable],
                access="read",
            )
            latest_result = execute_markets_operation(latest_operation, context=context)
            latest_rows = list(operation_result_rows(latest_result))
            selected_time_index = (
                latest_rows[0].get("time_index") if latest_rows else None
            )
            if selected_time_index is None:
                raise MexicanGovernmentBondCurveError(
                    "No Valmer vector storage rows are available for CETES/M Bonos "
                    "government curve bootstrap."
                )
            selected_time_index = _normalize_query_time_index(selected_time_index)
            filters.append(vector_table.c.time_index == selected_time_index)
            query_label = f"at {selected_time_index}"

    statement = (
        select(
            vector_table.c.time_index.label("time_index"),
            vector_table.c.asset_identifier.label("unique_identifier"),
            vector_table.c.valuation_date.label("fecha"),
            vector_table.c.clean_price.label("preciolimpio"),
            vector_table.c.dirty_price.label("preciosucio"),
            vector_table.c.accrued_interest.label("interesesacumulados"),
            vector_table.c.days_since_coupon.label("diastransccpn"),
            vector_table.c.adjusted_face_value.label("valornominalactualizado"),
            details_table.c.security_type.label("tipovalor"),
            details_table.c.issuer.label("emisora"),
            details_table.c.series.label("serie"),
            details_table.c.sector.label("sector"),
            details_table.c.issue_date.label("fechaemision"),
            details_table.c.maturity_date.label("fechavcto"),
            details_table.c.face_value.label("valornominal"),
            details_table.c.issue_currency.label("monedaemision"),
            details_table.c.coupon_frequency.label("freccpn"),
            details_table.c.coupon_rate.label("tasacupon"),
            vector_table.c.yield_rate.label("yield_rate"),
        )
        .select_from(joined)
        .where(*filters)
        .order_by(
            vector_table.c.time_index,
            details_table.c.security_type,
            details_table.c.issuer,
            details_table.c.maturity_date,
            vector_table.c.asset_identifier,
        )
    )
    operation = compile_markets_statement(
        statement,
        context=context,
        operation="select",
        models=[ValmerVectorPricesStorage, ValmerAssetDetailsTable],
        access="read",
    )
    result = execute_markets_operation(operation, context=context)
    rows = list(operation_result_rows(result))
    frame = _vector_storage_rows_to_curve_source_frame(
        rows,
        time_index=selected_time_index,
        allow_empty=allow_empty,
    )
    if logger is not None:
        logger.info(
            "Loaded "
            f"{len(frame)} Valmer vector storage rows for MXN government curve "
            f"bootstrap {query_label}."
        )
    return frame


def _vector_storage_rows_to_curve_source_frame(
    rows: list[dict[str, Any]],
    *,
    time_index: Any | None,
    allow_empty: bool = False,
) -> pd.DataFrame:
    if not rows:
        if allow_empty:
            return pd.DataFrame(columns=_VECTOR_CURVE_SOURCE_COLUMNS)
        raise MexicanGovernmentBondCurveError(
            f"No CETES or M Bonos vector storage rows found for time_index {time_index!r}."
        )
    frame = pd.DataFrame(rows, columns=_VECTOR_CURVE_SOURCE_COLUMNS)
    if "fecha" not in frame:
        frame["fecha"] = pd.NaT
    missing_fecha = frame["fecha"].isna()
    if missing_fecha.any():
        frame.loc[missing_fecha, "fecha"] = frame.loc[missing_fecha, "time_index"]
    return frame


def _last_curve_update_time(
    update_statistics,
    curve_identifier: str,
) -> dt.datetime | None:
    getter = getattr(update_statistics, "get_last_update_for_identity", None)
    if not callable(getter):
        return None
    last_update = getter(curve_identifier)
    if last_update is None:
        return None
    return _normalize_query_time_index(last_update)


def _normalize_query_time_index(value: Any) -> dt.datetime:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.to_pydatetime()


def _empty_curve_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "time_index",
            "curve_identifier",
            "curve",
            "key_nodes",
            "metadata_json",
        ]
    ).set_index(["time_index", "curve_identifier"])
