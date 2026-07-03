from __future__ import annotations

import datetime as dt
import math
from collections.abc import Mapping
from typing import Any

from msm_pricing.data_nodes import CurveKeyNode
from msm_pricing.pricing_engine.curves import (
    parse_bond_helper_key_node,
    parse_cross_currency_key_node,
)

from valmer_connectors.instruments.curve_bootstrap import (
    TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
    USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
)
from valmer_connectors.instruments.rates_curves import (
    VALMER_TIIE_CALENDAR_CODE,
    VALMER_TIIE_IRS_SOURCE_FILE,
    VALMER_TIIE_PAYMENT_FREQUENCY,
    VALMER_USD_MXN_XCCY_JOINT_CALENDAR_CODE,
    VALMER_USD_SOFR_CALENDAR_CODE,
    VALMER_USD_SOFR_IRS_SOURCE_FILE,
    VALMER_USD_SOFR_PAYMENT_FREQUENCY,
)


class ValmerCurveKeyNodeError(ValueError):
    """Raised when Valmer curve construction provenance is semantically invalid."""


def validate_tiie_ois_key_nodes(
    value: Any,
    *,
    row: Mapping[str, Any],
    curve_identifier: str,
) -> list[dict[str, Any]]:
    """Validate Valmer IRS MXN key nodes before DiscountCurvesNode compression."""

    nodes = _curve_key_node_list(value, curve_identifier=curve_identifier)
    for node in nodes:
        _require_source_family(
            node,
            curve_identifier=curve_identifier,
            required_fragment=".MXN.FTIIE.1D/28D.BANXICO",
        )
        _require_fields(
            node,
            curve_identifier=curve_identifier,
            fields=("maturity_date", "asset_identifier", "tenor", "earliest_date", "pillar_date"),
        )
        _require_date_fields(
            node,
            curve_identifier=curve_identifier,
            fields=("maturity_date", "earliest_date", "pillar_date"),
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="instrument_type",
            expected="overnight_indexed_swap",
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="helper_type",
            expected="ois_rate_helper",
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="quote_type",
            expected="par_swap_rate",
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="quote_unit",
            expected="decimal",
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="quote_side",
            expected="mid",
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="quote_source",
            expected=VALMER_TIIE_IRS_SOURCE_FILE,
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="source_quote_unit",
            expected="percent",
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="floating_index",
            expected=TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="settlement_days",
            expected=1,
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="payment_convention",
            expected="ModifiedFollowing",
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="payment_frequency",
            expected=VALMER_TIIE_PAYMENT_FREQUENCY,
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="payment_calendar_code",
            expected=VALMER_TIIE_CALENDAR_CODE,
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="averaging_method",
            expected="Compound",
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="end_of_month",
            expected=False,
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="fixed_payment_frequency",
            expected=VALMER_TIIE_PAYMENT_FREQUENCY,
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="fixed_calendar_code",
            expected=VALMER_TIIE_CALENDAR_CODE,
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="day_counter",
            expected="Actual360",
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="day_counter_code",
            expected="Actual360",
        )
        _require_number(node, curve_identifier=curve_identifier, field="quote")
        _require_number(node, curve_identifier=curve_identifier, field="source_quote")
    return nodes


def validate_usd_sofr_key_nodes(
    value: Any,
    *,
    row: Mapping[str, Any],
    curve_identifier: str,
) -> list[dict[str, Any]]:
    """Validate Valmer IRS USD SOFR key nodes before DiscountCurvesNode compression."""

    nodes = _curve_key_node_list(value, curve_identifier=curve_identifier)
    has_future = False
    has_ois = False
    for node in nodes:
        asset_identifier = _require_string(
            node,
            curve_identifier=curve_identifier,
            field="asset_identifier",
        )
        if "FEDFUNDS" in asset_identifier.upper():
            raise ValmerCurveKeyNodeError(
                f"{curve_identifier} key_nodes must not include Fed Funds rows: "
                f"{asset_identifier!r}."
            )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="quote_source",
            expected=VALMER_USD_SOFR_IRS_SOURCE_FILE,
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="quote_side",
            expected="mid",
        )
        _require_date_fields(
            node,
            curve_identifier=curve_identifier,
            fields=("maturity_date", "earliest_date", "pillar_date"),
        )
        _require_number(node, curve_identifier=curve_identifier, field="quote")

        instrument_type = node.get("instrument_type")
        if instrument_type == "sofr_future":
            has_future = True
            _validate_usd_sofr_future_node(
                node,
                curve_identifier=curve_identifier,
                asset_identifier=asset_identifier,
            )
        elif instrument_type == "overnight_indexed_swap":
            has_ois = True
            _validate_usd_sofr_ois_node(
                node,
                curve_identifier=curve_identifier,
                asset_identifier=asset_identifier,
            )
        else:
            raise ValmerCurveKeyNodeError(
                f"{curve_identifier} key_nodes contains unsupported USD SOFR "
                f"instrument_type {instrument_type!r}."
            )
    if not has_future:
        raise ValmerCurveKeyNodeError(
            f"{curve_identifier} key_nodes must include at least one SOFR future."
        )
    if not has_ois:
        raise ValmerCurveKeyNodeError(
            f"{curve_identifier} key_nodes must include at least one SOFR OIS swap."
        )
    return nodes


def validate_mxn_government_key_nodes(
    value: Any,
    *,
    row: Mapping[str, Any],
    curve_identifier: str,
) -> list[dict[str, Any]]:
    """Validate Valmer CETES and M Bonos key nodes before compression."""

    nodes = _curve_key_node_list(value, curve_identifier=curve_identifier)
    if len(nodes) < 2:
        raise ValmerCurveKeyNodeError(
            f"{curve_identifier} key_nodes must include at least two government "
            "bond bootstrap instruments."
        )
    has_cetes = False
    has_m_bonos = False
    for node in nodes:
        try:
            parse_bond_helper_key_node(node)
        except Exception as exc:
            raise ValmerCurveKeyNodeError(
                f"{curve_identifier} key_nodes must satisfy the generic bond-helper "
                f"contract: {exc}"
            ) from exc
        _require_fields(
            node,
            curve_identifier=curve_identifier,
            fields=("maturity_date", "asset_identifier", "quote_source"),
        )
        _require_date_fields(node, curve_identifier=curve_identifier, fields=("maturity_date",))
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="quote_type",
            expected="clean_price",
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="quote_side",
            expected="mid",
        )
        _require_number(node, curve_identifier=curve_identifier, field="quote")
        _require_number(node, curve_identifier=curve_identifier, field="yield")
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="yield_type",
            expected="yield_to_maturity",
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="yield_unit",
            expected="decimal",
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="yield_source",
            expected="tasaderendimiento",
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="day_counter",
            expected="Actual360",
        )

        instrument_type = node.get("instrument_type")
        if instrument_type == "zero_coupon_bond":
            has_cetes = True
            _validate_cetes_key_node(node, curve_identifier=curve_identifier)
        elif instrument_type == "fixed_rate_bond":
            has_m_bonos = True
            _validate_m_bonos_key_node(node, curve_identifier=curve_identifier)
        else:
            raise ValmerCurveKeyNodeError(
                f"{curve_identifier} key_nodes contains unsupported government "
                f"instrument_type {instrument_type!r}."
            )
    if not has_cetes:
        raise ValmerCurveKeyNodeError(
            f"{curve_identifier} key_nodes must include at least one CETES input."
        )
    if not has_m_bonos:
        raise ValmerCurveKeyNodeError(
            f"{curve_identifier} key_nodes must include at least one M Bonos input."
        )
    return nodes


def validate_usd_mxn_xccy_key_nodes(
    value: Any,
    *,
    row: Mapping[str, Any],
    curve_identifier: str,
) -> list[dict[str, Any]]:
    """Validate Valmer USD/MXN FX and CCS key nodes before compression."""

    nodes = _curve_key_node_list(value, curve_identifier=curve_identifier)
    has_spot = False
    has_fx_swap = False
    has_ccs = False
    for node in nodes:
        parse_cross_currency_key_node(node)
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="quote_source",
            expected=VALMER_TIIE_IRS_SOURCE_FILE,
        )
        _require_equal(
            node,
            curve_identifier=curve_identifier,
            field="quote_side",
            expected="mid",
        )
        _require_number(node, curve_identifier=curve_identifier, field="quote")
        instrument_type = node.get("instrument_type")
        if instrument_type == "fx_spot":
            has_spot = True
            _validate_usd_mxn_fx_spot_node(node, curve_identifier=curve_identifier)
        elif instrument_type == "fx_swap":
            has_fx_swap = True
            _validate_usd_mxn_fx_swap_node(node, curve_identifier=curve_identifier)
        elif instrument_type == "cross_currency_basis_swap":
            has_ccs = True
            _validate_usd_mxn_xccy_basis_node(node, curve_identifier=curve_identifier)
        else:
            raise ValmerCurveKeyNodeError(
                f"{curve_identifier} key_nodes contains unsupported USD/MXN "
                f"instrument_type {instrument_type!r}."
            )
    if not has_spot:
        raise ValmerCurveKeyNodeError(
            f"{curve_identifier} key_nodes must include USD/MXN spot."
        )
    if not has_fx_swap:
        raise ValmerCurveKeyNodeError(
            f"{curve_identifier} key_nodes must include USD/MXN FX swap helpers."
        )
    if not has_ccs:
        raise ValmerCurveKeyNodeError(
            f"{curve_identifier} key_nodes must include USD/MXN CCS helpers."
        )
    return nodes


def _validate_usd_mxn_fx_spot_node(
    node: Mapping[str, Any],
    *,
    curve_identifier: str,
) -> None:
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="asset_identifier",
        expected="FX.USD.MXN",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="helper_type",
        expected="fx_spot",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="quote_type",
        expected="fx_spot",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="quote_unit",
        expected="mxn_per_usd",
    )
    _validate_usd_mxn_fx_identity(node, curve_identifier=curve_identifier)


def _validate_usd_mxn_fx_swap_node(
    node: Mapping[str, Any],
    *,
    curve_identifier: str,
) -> None:
    _require_source_family(
        node,
        curve_identifier=curve_identifier,
        required_fragment="FX.USD.MXN.",
    )
    _require_date_fields(
        node,
        curve_identifier=curve_identifier,
        fields=("maturity_date", "earliest_date", "pillar_date"),
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="helper_type",
        expected="fx_swap_rate_helper",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="quote_type",
        expected="fx_forward_points",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="quote_unit",
        expected="mxn_per_usd",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="source_quote_unit",
        expected="raw_points",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="point_scale",
        expected=10000,
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="fixing_days",
        expected=2,
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="calendar_code",
        expected=VALMER_USD_MXN_XCCY_JOINT_CALENDAR_CODE,
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="business_day_convention",
        expected="ModifiedFollowing",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="end_of_month",
        expected=False,
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="is_fx_base_currency_collateral_currency",
        expected=True,
    )
    _validate_usd_mxn_fx_identity(node, curve_identifier=curve_identifier)
    _require_number(node, curve_identifier=curve_identifier, field="source_quote")
    _require_number(node, curve_identifier=curve_identifier, field="spot")
    _require_number(node, curve_identifier=curve_identifier, field="market_forward")
    _require_number(node, curve_identifier=curve_identifier, field="quote_error")


def _validate_usd_mxn_xccy_basis_node(
    node: Mapping[str, Any],
    *,
    curve_identifier: str,
) -> None:
    _require_source_family(
        node,
        curve_identifier=curve_identifier,
        required_fragment=".MXN.FTIIE.1D/USD.SOFR.1D.SOFR",
    )
    _require_date_fields(
        node,
        curve_identifier=curve_identifier,
        fields=("maturity_date", "earliest_date", "pillar_date"),
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="helper_type",
        expected="const_notional_cross_currency_basis_swap_rate_helper",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="quote_type",
        expected="basis_spread",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="quote_unit",
        expected="decimal",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="source_quote_unit",
        expected="percent",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="basis_side",
        expected="USD_SOFR",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="basis_sign",
        expected="positive_quote_means_sofr_plus_spread",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="notional_style",
        expected="constant_notional",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="base_currency_index",
        expected=USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="quote_currency_index",
        expected=TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="fixing_days",
        expected=0,
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="calendar_code",
        expected=VALMER_USD_MXN_XCCY_JOINT_CALENDAR_CODE,
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="business_day_convention",
        expected="ModifiedFollowing",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="end_of_month",
        expected=False,
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="is_fx_base_currency_collateral_currency",
        expected=True,
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="is_basis_on_fx_base_currency_leg",
        expected=True,
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="payment_frequency",
        expected=VALMER_TIIE_PAYMENT_FREQUENCY,
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="payment_lag",
        expected=0,
    )
    _require_fields(
        node,
        curve_identifier=curve_identifier,
        fields=("source_tenor", "tenor"),
    )
    _require_number(node, curve_identifier=curve_identifier, field="source_quote")
    _require_number(node, curve_identifier=curve_identifier, field="quote_error")


def _validate_usd_mxn_fx_identity(
    node: Mapping[str, Any],
    *,
    curve_identifier: str,
) -> None:
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="fx_pair",
        expected="USD/MXN",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="fx_base_currency",
        expected="USD",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="fx_quote_currency",
        expected="MXN",
    )


def _validate_usd_sofr_future_node(
    node: Mapping[str, Any],
    *,
    curve_identifier: str,
    asset_identifier: str,
) -> None:
    if not (
        asset_identifier.startswith("Future.USD.CME.CME SR1 EOM.")
        or asset_identifier.startswith("Future.USD.CME.CME SR3 IMM.")
    ):
        raise ValmerCurveKeyNodeError(
            f"{curve_identifier} SOFR future key node has unsupported source row "
            f"{asset_identifier!r}."
        )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="helper_type",
        expected="sofr_future_rate_helper",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="quote_type",
        expected="futures_price",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="quote_unit",
        expected="price",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="implied_rate_unit",
        expected="decimal",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="future_family",
        expected="sofr",
    )
    _require_number(node, curve_identifier=curve_identifier, field="implied_rate")
    _require_number(node, curve_identifier=curve_identifier, field="convexity_adjustment")
    if float(node["convexity_adjustment"]) != 0.0:
        raise ValmerCurveKeyNodeError(
            f"{curve_identifier} SOFR future convexity_adjustment must be 0.0."
        )
    _require_fields(
        node,
        curve_identifier=curve_identifier,
        fields=("contract_code", "reference_month", "reference_frequency"),
    )
    _require_number(node, curve_identifier=curve_identifier, field="reference_year")


def _validate_usd_sofr_ois_node(
    node: Mapping[str, Any],
    *,
    curve_identifier: str,
    asset_identifier: str,
) -> None:
    _require_source_family(
        node,
        curve_identifier=curve_identifier,
        required_fragment=".USD.SOFR.1D/1Y.SOFR",
    )
    if "FEDFUNDS" in asset_identifier.upper():
        raise ValmerCurveKeyNodeError(
            f"{curve_identifier} SOFR OIS key node has unsupported Fed Funds row "
            f"{asset_identifier!r}."
        )
    _require_fields(node, curve_identifier=curve_identifier, fields=("tenor",))
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="helper_type",
        expected="ois_rate_helper",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="quote_type",
        expected="par_swap_rate",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="quote_unit",
        expected="decimal",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="source_quote_unit",
        expected="percent",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="floating_index",
        expected=USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="settlement_days",
        expected=2,
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="payment_convention",
        expected="ModifiedFollowing",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="payment_frequency",
        expected=VALMER_USD_SOFR_PAYMENT_FREQUENCY,
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="payment_calendar_code",
        expected=VALMER_USD_SOFR_CALENDAR_CODE,
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="averaging_method",
        expected="Compound",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="end_of_month",
        expected=False,
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="fixed_payment_frequency",
        expected=VALMER_USD_SOFR_PAYMENT_FREQUENCY,
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="fixed_calendar_code",
        expected=VALMER_USD_SOFR_CALENDAR_CODE,
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="day_counter",
        expected="Actual360",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="day_counter_code",
        expected="Actual360",
    )
    _require_number(node, curve_identifier=curve_identifier, field="source_quote")


def _validate_cetes_key_node(node: Mapping[str, Any], *, curve_identifier: str) -> None:
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="helper_type",
        expected="zero_coupon_bond_helper",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="quote_unit",
        expected="price_per_face",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="quote_source",
        expected="preciosucio",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="source_quote_type",
        expected="dirty_price",
    )
    _require_number(node, curve_identifier=curve_identifier, field="face_value")
    if float(node["face_value"]) != 10.0:
        raise ValmerCurveKeyNodeError(
            f"{curve_identifier} CETES key node face_value must be 10.0."
        )


def _validate_m_bonos_key_node(node: Mapping[str, Any], *, curve_identifier: str) -> None:
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="helper_type",
        expected="fixed_rate_bond_helper",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="quote_unit",
        expected="price_per_100",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="quote_source",
        expected="preciolimpio",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="source_quote_type",
        expected="clean_price",
    )
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="dirty_price_source",
        expected="preciosucio",
    )
    _require_number(node, curve_identifier=curve_identifier, field="dirty_price")
    _require_number(node, curve_identifier=curve_identifier, field="accrued_interest")
    _require_number(node, curve_identifier=curve_identifier, field="coupon_rate")
    _require_number(node, curve_identifier=curve_identifier, field="face_value")
    _require_equal(
        node,
        curve_identifier=curve_identifier,
        field="coupon_period_days",
        expected=182,
    )
    if float(node["face_value"]) != 100.0:
        raise ValmerCurveKeyNodeError(
            f"{curve_identifier} M Bonos key node face_value must be 100.0."
        )


def _curve_key_node_list(value: Any, *, curve_identifier: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ValmerCurveKeyNodeError(
            f"{curve_identifier} key_nodes must be a non-empty list."
        )
    return [_curve_key_node_payload(node, curve_identifier=curve_identifier) for node in value]


def _curve_key_node_payload(value: Any, *, curve_identifier: str) -> dict[str, Any]:
    try:
        return CurveKeyNode.model_validate(value).model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        )
    except ValueError as exc:
        raise ValmerCurveKeyNodeError(
            f"{curve_identifier} key_nodes must satisfy the CurveKeyNode contract."
        ) from exc


def _require_source_family(
    node: Mapping[str, Any],
    *,
    curve_identifier: str,
    required_fragment: str,
) -> None:
    asset_identifier = _require_string(
        node,
        curve_identifier=curve_identifier,
        field="asset_identifier",
    )
    if required_fragment not in asset_identifier:
        raise ValmerCurveKeyNodeError(
            f"{curve_identifier} key node asset_identifier {asset_identifier!r} "
            f"must contain {required_fragment!r}."
        )


def _require_fields(
    node: Mapping[str, Any],
    *,
    curve_identifier: str,
    fields: tuple[str, ...],
) -> None:
    for field in fields:
        _require_string(node, curve_identifier=curve_identifier, field=field)


def _require_date_fields(
    node: Mapping[str, Any],
    *,
    curve_identifier: str,
    fields: tuple[str, ...],
) -> None:
    for field in fields:
        value = _require_string(node, curve_identifier=curve_identifier, field=field)
        try:
            dt.date.fromisoformat(value)
        except ValueError as exc:
            raise ValmerCurveKeyNodeError(
                f"{curve_identifier} key node field {field!r} must be an ISO date."
            ) from exc


def _require_string(
    node: Mapping[str, Any],
    *,
    curve_identifier: str,
    field: str,
) -> str:
    value = node.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValmerCurveKeyNodeError(
            f"{curve_identifier} key node field {field!r} must be a non-empty string."
        )
    return value


def _require_equal(
    node: Mapping[str, Any],
    *,
    curve_identifier: str,
    field: str,
    expected: Any,
) -> None:
    if node.get(field) != expected:
        raise ValmerCurveKeyNodeError(
            f"{curve_identifier} key node field {field!r} must be {expected!r}; "
            f"got {node.get(field)!r}."
        )


def _require_number(
    node: Mapping[str, Any],
    *,
    curve_identifier: str,
    field: str,
) -> None:
    value = node.get(field)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValmerCurveKeyNodeError(
            f"{curve_identifier} key node field {field!r} must be a finite number."
        )
    if not math.isfinite(float(value)):
        raise ValmerCurveKeyNodeError(
            f"{curve_identifier} key node field {field!r} must be finite."
        )


__all__ = [
    "ValmerCurveKeyNodeError",
    "validate_mxn_government_key_nodes",
    "validate_tiie_ois_key_nodes",
    "validate_usd_sofr_key_nodes",
]
