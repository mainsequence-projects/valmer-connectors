"""Dependency-backed Valmer curve DataNodes consuming persisted Index quotes."""

from __future__ import annotations

from typing import Any

import pandas as pd
from msm_pricing.data_nodes import CurveConfig, DiscountCurvesNode
from msm_pricing.data_nodes.curves.storage import DiscountCurvesStorage
from pydantic import Field

from mainsequence.meta_tables import PlatformTimeIndexMetaTable
from valmer_connectors.data_nodes.canonical_index_values import (
    DailyIndexValuesStorage,
)
from valmer_connectors.data_nodes.curve_quote_indices import (
    MXN_QUOTE_FAMILIES,
    USD_QUOTE_FAMILIES,
    ValmerIrsMxnIndexValuesNode,
    ValmerIrsUsdIndexValuesNode,
    mxn_quote_config,
    usd_quote_config,
)
from valmer_connectors.instruments.curve_bootstrap import (
    VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_UNIQUE_IDENTIFIER,
    VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
)
from valmer_connectors.instruments.curve_key_nodes import (
    validate_tiie_ois_key_nodes,
    validate_usd_mxn_xccy_key_nodes,
    validate_usd_sofr_key_nodes,
)
from valmer_connectors.instruments.rates_curves import (
    build_tiie_curve_frame_from_quote_snapshot,
    build_usd_mxn_xccy_curve_frame_from_quote_snapshot,
    build_usd_sofr_curve_frame_from_quote_snapshot,
)
from valmer_connectors.queries.curve_quote_indices import (
    load_discount_curve_key_nodes,
    load_valmer_curve_quote_snapshots,
)


class ValmerQuoteBackedCurveConfig(CurveConfig):
    """Curve updater identity including its selected canonical quote storage."""

    quote_storage_table: type[PlatformTimeIndexMetaTable] = Field(
        default=DailyIndexValuesStorage,
        description="Registered canonical Index-values storage selected as quote input.",
    )
    source_families: tuple[str, ...] = Field(
        ...,
        min_length=1,
        description="Persisted Valmer source families required by this curve build.",
    )


class ValmerXccyCurveConfig(ValmerQuoteBackedCurveConfig):
    """XCCY updater identity including exact upstream curve selections."""

    curve_storage_table: type[PlatformTimeIndexMetaTable] = Field(
        default=DiscountCurvesStorage,
        description="Registered curve storage selected for exact-date upstream curves.",
    )
    tiie_curve_identifier: str = Field(
        default=VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
        description="Exact upstream TIIE curve identity used by XCCY helpers.",
    )
    sofr_curve_identifier: str = Field(
        default=VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
        description="Exact upstream SOFR curve identity used by XCCY helpers.",
    )


class ValmerTiieDiscountCurveNode(DiscountCurvesNode):
    """Build TIIE only after its complete IRS_MXN quote snapshot is persisted."""

    def __init__(self, curve_config: ValmerQuoteBackedCurveConfig, **kwargs: Any):
        self.quote_dependency = ValmerIrsMxnIndexValuesNode(mxn_quote_config())
        super().__init__(curve_config=curve_config, **kwargs)
        self.set_key_nodes_validator(validate_tiie_ois_key_nodes)

    def dependencies(self) -> dict[str, Any]:
        return {"irs_mxn_quotes": self.quote_dependency}

    def build_curve_frame(self, *, update_statistics, curve_identifier, **kwargs):
        _ = kwargs
        snapshots = _new_quote_snapshots(
            update_statistics=update_statistics,
            curve_identifier=curve_identifier,
            storage_table=self.curve_config.quote_storage_table,
            source_families=self.curve_config.source_families,
        )
        frames = [
            build_tiie_curve_frame_from_quote_snapshot(
                snapshot,
                curve_identifier=curve_identifier,
                valuation_date=time_index,
            )
            for time_index, snapshot in snapshots.groupby("time_index", sort=True)
        ]
        return _concat_curve_frames(frames)


class ValmerUsdSofrDiscountCurveNode(DiscountCurvesNode):
    """Build SOFR only after every IRS_USD quote, including Fed Funds, persists."""

    def __init__(self, curve_config: ValmerQuoteBackedCurveConfig, **kwargs: Any):
        self.quote_dependency = ValmerIrsUsdIndexValuesNode(usd_quote_config())
        super().__init__(curve_config=curve_config, **kwargs)
        self.set_key_nodes_validator(validate_usd_sofr_key_nodes)

    def dependencies(self) -> dict[str, Any]:
        return {"irs_usd_quotes": self.quote_dependency}

    def build_curve_frame(self, *, update_statistics, curve_identifier, **kwargs):
        _ = kwargs
        snapshots = _new_quote_snapshots(
            update_statistics=update_statistics,
            curve_identifier=curve_identifier,
            storage_table=self.curve_config.quote_storage_table,
            source_families=self.curve_config.source_families,
        )
        frames = [
            build_usd_sofr_curve_frame_from_quote_snapshot(
                snapshot,
                curve_identifier=curve_identifier,
                valuation_date=time_index,
            )
            for time_index, snapshot in snapshots.groupby("time_index", sort=True)
        ]
        return _concat_curve_frames(frames)


class ValmerUsdMxnCollateralDiscountCurveNode(DiscountCurvesNode):
    """Build XCCY from persisted MXN quotes and exact-date TIIE/SOFR curves."""

    def __init__(self, curve_config: ValmerXccyCurveConfig, **kwargs: Any):
        self.quote_dependency = ValmerIrsMxnIndexValuesNode(mxn_quote_config())
        self.tiie_curve_dependency = ValmerTiieDiscountCurveNode(tiie_curve_config())
        self.sofr_curve_dependency = ValmerUsdSofrDiscountCurveNode(sofr_curve_config())
        super().__init__(curve_config=curve_config, **kwargs)
        self.set_key_nodes_validator(validate_usd_mxn_xccy_key_nodes)

    def dependencies(self) -> dict[str, Any]:
        return {
            "irs_mxn_quotes": self.quote_dependency,
            "tiie_curve": self.tiie_curve_dependency,
            "sofr_curve": self.sofr_curve_dependency,
        }

    def build_curve_frame(self, *, update_statistics, curve_identifier, **kwargs):
        _ = kwargs
        snapshots = _new_quote_snapshots(
            update_statistics=update_statistics,
            curve_identifier=curve_identifier,
            storage_table=self.curve_config.quote_storage_table,
            source_families=self.curve_config.source_families,
        )
        frames = []
        for time_index, snapshot in snapshots.groupby("time_index", sort=True):
            upstream = load_discount_curve_key_nodes(
                storage_table=self.curve_config.curve_storage_table,
                time_index=time_index,
                curve_identifiers=(
                    self.curve_config.tiie_curve_identifier,
                    self.curve_config.sofr_curve_identifier,
                ),
            )
            frames.append(
                build_usd_mxn_xccy_curve_frame_from_quote_snapshot(
                    snapshot,
                    tiie_curve_key_nodes=upstream[
                        self.curve_config.tiie_curve_identifier
                    ],
                    usd_sofr_curve_key_nodes=upstream[
                        self.curve_config.sofr_curve_identifier
                    ],
                    curve_identifier=curve_identifier,
                    valuation_date=time_index,
                )
            )
        return _concat_curve_frames(frames)


def tiie_curve_config() -> ValmerQuoteBackedCurveConfig:
    return ValmerQuoteBackedCurveConfig(
        curve_unique_identifier=VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
        quote_storage_table=DailyIndexValuesStorage,
        source_families=("tiie_ois",),
    )


def sofr_curve_config() -> ValmerQuoteBackedCurveConfig:
    return ValmerQuoteBackedCurveConfig(
        curve_unique_identifier=VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
        quote_storage_table=DailyIndexValuesStorage,
        source_families=("sofr_future", "sofr_ois"),
    )


def xccy_curve_config() -> ValmerXccyCurveConfig:
    return ValmerXccyCurveConfig(
        curve_unique_identifier=(
            VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_UNIQUE_IDENTIFIER
        ),
        quote_storage_table=DailyIndexValuesStorage,
        curve_storage_table=DiscountCurvesStorage,
        source_families=("fx_spot", "fx_forward", "tiie_sofr_xccy_basis"),
    )


def _new_quote_snapshots(
    *,
    update_statistics,
    curve_identifier: str,
    storage_table,
    source_families: tuple[str, ...],
) -> pd.DataFrame:
    getter = getattr(update_statistics, "get_last_update_for_identity", None)
    last_update = getter(curve_identifier) if callable(getter) else None
    return load_valmer_curve_quote_snapshots(
        storage_table=storage_table,
        source_families=source_families,
        after_time_index=last_update,
    )


def _concat_curve_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame(
            columns=["time_index", "curve_identifier", "curve", "key_nodes"]
        ).set_index(["time_index", "curve_identifier"])
    return pd.concat(frames).sort_index()


__all__ = [
    "ValmerQuoteBackedCurveConfig",
    "ValmerTiieDiscountCurveNode",
    "ValmerUsdMxnCollateralDiscountCurveNode",
    "ValmerUsdSofrDiscountCurveNode",
    "ValmerXccyCurveConfig",
    "sofr_curve_config",
    "tiie_curve_config",
    "xccy_curve_config",
]
