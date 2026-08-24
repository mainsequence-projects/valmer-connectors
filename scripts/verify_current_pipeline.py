from __future__ import annotations

import json
import math
from collections import Counter
from typing import Any

import pandas as pd
from msm.api.base import operation_result_rows
from msm.repositories.base import (
    MarketsRepositoryContext,
    compile_markets_statement,
    execute_markets_operation,
)
from msm_pricing.data_nodes.curves.key_nodes import decompress_key_nodes_from_string
from msm_pricing.data_nodes.curves.storage import DiscountCurvesStorage
from msm_pricing.data_nodes.index_fixings.storage import IndexFixingsStorage
from sqlalchemy import func, select

from valmer_connectors.data_nodes.canonical_index_values import DailyIndexValuesStorage
from valmer_connectors.instruments.bootstrap import bootstrap_runtime
from valmer_connectors.instruments.curve_bootstrap import (
    VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_UNIQUE_IDENTIFIER,
    VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
)
from valmer_connectors.instruments.curve_key_nodes import (
    validate_mxn_government_key_nodes,
    validate_tiie_ois_key_nodes,
    validate_usd_mxn_xccy_key_nodes,
    validate_usd_sofr_key_nodes,
)
from valmer_connectors.services.curve_update import (
    load_mxn_government_curve_source_from_vector_storage,
)

CURVE_IDENTIFIERS = (
    VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_UNIQUE_IDENTIFIER,
    VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
)
EXPECTED_CURVE_ROWS = {
    VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER: 1,
    VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER: 1,
    VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_UNIQUE_IDENTIFIER: 1,
    VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER: 248,
}
KEY_NODE_VALIDATORS = {
    VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER: validate_tiie_ois_key_nodes,
    VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER: validate_usd_sofr_key_nodes,
    VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_UNIQUE_IDENTIFIER: (
        validate_usd_mxn_xccy_key_nodes
    ),
    VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER: (
        validate_mxn_government_key_nodes
    ),
}


def _rows(statement: Any, *, models: list[type[Any]]) -> list[dict[str, Any]]:
    context = MarketsRepositoryContext(
        limits={"max_rows": 100_000, "statement_timeout_ms": 120_000},
        timeout=180,
    )
    operation = compile_markets_statement(
        statement,
        context=context,
        operation="select",
        models=models,
        access="read",
    )
    result = execute_markets_operation(operation, context=context)
    if result.get("truncated"):
        raise RuntimeError("Pipeline verification query was truncated.")
    return list(operation_result_rows(result))


def _utc(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    return (
        timestamp.tz_localize("UTC")
        if timestamp.tzinfo is None
        else timestamp.tz_convert("UTC")
    )


def main() -> None:
    bootstrap_runtime(seed_static_rows=False)

    daily_table = DailyIndexValuesStorage.__table__
    daily_stats = _rows(
        select(
            daily_table.c.index_identifier,
            func.count().label("row_count"),
            func.min(daily_table.c.time_index).label("minimum_time_index"),
            func.max(daily_table.c.time_index).label("maximum_time_index"),
        )
        .group_by(daily_table.c.index_identifier)
        .order_by(daily_table.c.index_identifier),
        models=[DailyIndexValuesStorage],
    )
    quote_rows = _rows(
        select(
            daily_table.c.time_index,
            daily_table.c.index_identifier,
            daily_table.c.value,
        )
        .where(daily_table.c.index_identifier.like("VALMER_CURVE_QUOTE.%"))
        .order_by(daily_table.c.time_index, daily_table.c.index_identifier),
        models=[DailyIndexValuesStorage],
    )
    quote_lookup = {
        (_utc(row["time_index"]), str(row["index_identifier"])): float(row["value"])
        for row in quote_rows
    }
    quote_identifiers = {str(row["index_identifier"]) for row in quote_rows}
    if len(quote_rows) != 81 or len(quote_identifiers) != 81:
        raise RuntimeError(
            "Expected 81 current Valmer curve-quote Index observations and identities."
        )

    non_quote_observations = sum(
        int(row["row_count"])
        for row in daily_stats
        if not str(row["index_identifier"]).startswith("VALMER_CURVE_QUOTE.")
    )
    if non_quote_observations != 8_633:
        raise RuntimeError(
            f"Expected 8,633 reference-rate observations; got {non_quote_observations}."
        )

    fixing_table = IndexFixingsStorage.__table__
    fixing_stats = _rows(
        select(
            fixing_table.c.index_identifier,
            func.count().label("row_count"),
            func.min(fixing_table.c.time_index).label("minimum_time_index"),
            func.max(fixing_table.c.time_index).label("maximum_time_index"),
        )
        .group_by(fixing_table.c.index_identifier)
        .order_by(fixing_table.c.index_identifier),
        models=[IndexFixingsStorage],
    )
    fixing_observations = sum(int(row["row_count"]) for row in fixing_stats)
    if fixing_observations != 26_430:
        raise RuntimeError(
            f"Expected 26,430 Banxico fixing observations; got {fixing_observations}."
        )

    government_source = load_mxn_government_curve_source_from_vector_storage(
        start_time_index="2024-08-30T23:59:59Z",
        timeout=180,
    )
    cetes = government_source.loc[
        (government_source["tipovalor"] == "BI")
        & (government_source["emisora"] == "CETES")
    ]
    bonos = government_source.loc[
        (government_source["tipovalor"] == "M")
        & (government_source["emisora"] == "BONOS")
    ]
    if (
        len(government_source) != 13_083
        or government_source["time_index"].nunique() != 248
        or len(cetes) != 9_029
        or cetes["unique_identifier"].nunique() != 87
        or len(bonos) != 4_054
    ):
        raise RuntimeError("Government vector history does not match the rebuilt source.")
    government_observations = {
        (_utc(row.time_index), str(row.unique_identifier))
        for row in government_source.itertuples(index=False)
    }

    curve_table = DiscountCurvesStorage.__table__
    curve_rows = _rows(
        select(
            curve_table.c.time_index,
            curve_table.c.curve_identifier,
            curve_table.c.key_nodes,
        )
        .where(curve_table.c.curve_identifier.in_(CURVE_IDENTIFIERS))
        .order_by(curve_table.c.time_index, curve_table.c.curve_identifier),
        models=[DiscountCurvesStorage],
    )
    curve_counts = Counter(str(row["curve_identifier"]) for row in curve_rows)
    if dict(curve_counts) != EXPECTED_CURVE_ROWS:
        raise RuntimeError(
            f"Unexpected persisted curve counts: {dict(curve_counts)!r}."
        )

    source_reference_counts: Counter[str] = Counter()
    key_node_counts: Counter[str] = Counter()
    resolved_index_references = 0
    resolved_asset_references = 0
    for row in curve_rows:
        curve_identifier = str(row["curve_identifier"])
        key_nodes = decompress_key_nodes_from_string(str(row["key_nodes"]))
        KEY_NODE_VALIDATORS[curve_identifier](
            key_nodes,
            row=row,
            curve_identifier=curve_identifier,
        )
        key_node_counts[curve_identifier] += len(key_nodes)
        for key_node in key_nodes:
            reference = key_node["source_reference"]
            reference_type = str(reference["type"])
            reference_identifier = str(reference["identifier"])
            source_reference_counts[reference_type] += 1
            if reference_type == "index":
                source_time = _utc(key_node["source_observation_time"])
                stored_value = quote_lookup.get((source_time, reference_identifier))
                if stored_value is None or not math.isclose(
                    stored_value,
                    float(key_node["quote"]),
                    rel_tol=0.0,
                    abs_tol=1e-12,
                ):
                    raise RuntimeError(
                        "Curve Index key node does not resolve to its exact stored "
                        f"observation: {reference_identifier!r} at {source_time}."
                    )
                resolved_index_references += 1
            elif reference_type == "asset":
                curve_time = _utc(row["time_index"])
                if (curve_time, reference_identifier) not in government_observations:
                    raise RuntimeError(
                        "Government curve Asset key node does not resolve to its exact "
                        f"stored vector observation: {reference_identifier!r} at "
                        f"{curve_time}."
                    )
                resolved_asset_references += 1
            else:
                raise RuntimeError(f"Unexpected key-node reference type {reference_type!r}.")

    summary = {
        "daily_index_values": {
            "reference_rate_observations": non_quote_observations,
            "quote_observations": len(quote_rows),
            "quote_identities": len(quote_identifiers),
            "index_identities": len(daily_stats),
        },
        "index_fixings": {
            "observations": fixing_observations,
            "index_identities": len(fixing_stats),
        },
        "government_vector": {
            "rows": len(government_source),
            "dates": int(government_source["time_index"].nunique()),
            "minimum_time_index": str(government_source["time_index"].min()),
            "maximum_time_index": str(government_source["time_index"].max()),
            "cetes_rows": len(cetes),
            "cetes_identities": int(cetes["unique_identifier"].nunique()),
            "m_bonos_rows": len(bonos),
        },
        "curves": {
            "rows_by_identifier": dict(curve_counts),
            "key_nodes_by_identifier": dict(key_node_counts),
            "source_reference_types": dict(source_reference_counts),
            "resolved_index_references": resolved_index_references,
            "resolved_asset_references": resolved_asset_references,
        },
    }
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
