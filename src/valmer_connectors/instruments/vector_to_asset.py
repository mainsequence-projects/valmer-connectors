# -*- coding: utf-8 -*-
import datetime as dt
import re
import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import msm_pricing.instruments as msi
import numpy as np
import pandas as pd

# noinspection PyPackageRequirements
import pytz
import QuantLib as ql
from msm_pricing.pricing_engine.bond_analytics import compare_bond_to_market_quote
from msm_pricing.pricing_engine.coupon_schedules import (
    compute_coupon_schedule_force_match,
)
from msm_pricing.valuation import ValuationLine, ValuationPosition
from tqdm import tqdm

from mainsequence.client.models_foundry import Artifact
from valmer_connectors.instruments.asset_identity import resolve_valmer_asset_uids
from valmer_connectors.settings import SUBYACENTE_TO_INDEX_MAP

# =============================================================================
# Configuration toggles for “coupon count” to match the sheet convention
# =============================================================================
COUNT_FROM_SETTLEMENT: bool = True  # vendor sheets often count from settlement (T+1/T+2)
INCLUDE_REF_DATE_EVENTS: bool = False  # treat flows ON ref date as already occurred (QL default)


# =============================================================================
# Small dataclass for the built instrument
# =============================================================================
@dataclass
class BuiltBond:
    row_ix: int
    emisora: str
    serie: str
    bond: msi.FloatingRateBond  # your model
    eval_date: dt.date


@dataclass(frozen=True)
class CoreBondPricingPayload:
    instrument_type: str
    valuation_date: dt.date
    issue_date: dt.date
    maturity_date: dt.date
    face_value: float
    day_count: ql.DayCounter
    calendar: ql.Calendar
    business_day_convention: int
    settlement_days: int
    benchmark_rate_index_uid: uuid.UUID
    coupon_frequency: ql.Period | None = None
    coupon_rate: float | None = None
    floating_rate_index_uid: uuid.UUID | None = None
    spread: float | None = None
    schedule: ql.Schedule | None = None


@dataclass
class ValmerValuationPositionBuildResult:
    valuation_position: ValuationPosition
    price_check_frame: pd.DataFrame
    instrument_map: Dict[str, Dict[str, Any]]
    report_artifact_uid: str | None = None
    report_csv_path: str | None = None


_REFERENCE_INDEX_UID_CACHE: dict[str, uuid.UUID] | None = None


def resolve_reference_index_uid(index_unique_identifier: str) -> uuid.UUID:
    """Resolve a core Index.unique_identifier to its backend IndexTable.uid."""

    uid_map = _reference_index_uid_map()
    try:
        return uid_map[index_unique_identifier]
    except KeyError as exc:
        raise KeyError(
            "Valmer pricing requires a bootstrapped Index row for "
            f"{index_unique_identifier!r}. Run bootstrap_runtime() before pricing "
            "hydration and ensure SUBYACENTE_TO_INDEX_MAP points to a supported "
            "Index.unique_identifier."
        ) from exc


def _reference_index_uid_map() -> dict[str, uuid.UUID]:
    global _REFERENCE_INDEX_UID_CACHE
    if _REFERENCE_INDEX_UID_CACHE is None:
        from valmer_connectors.instruments.curve_bootstrap import (
            upsert_mexican_reference_indexes,
        )

        indexes = upsert_mexican_reference_indexes(attach_runtime=False)
        _REFERENCE_INDEX_UID_CACHE = {
            unique_identifier: uuid.UUID(str(index.uid))
            for unique_identifier, index in indexes.items()
        }
    return _REFERENCE_INDEX_UID_CACHE


# =============================================================================
# Basic date & schedule helpers
# =============================================================================
def qld(d: dt.date) -> ql.Date:
    return ql.Date(d.day, d.month, d.year)


def pyd(d: ql.Date) -> dt.date:
    return dt.date(d.year(), int(d.month()), d.dayOfMonth())


def parse_val_date(v) -> dt.date:
    """Handle integer 20240903, '2024-09-03', pandas Timestamp, etc."""
    if pd.isna(v):
        raise ValueError("FECHA is required")
    s = str(int(v)) if isinstance(v, (int, np.integer)) else str(v)
    try:
        if len(s) == 8 and s.isdigit():
            return dt.date(int(s[:4]), int(s[4:6]), int(s[6:8]))
        return pd.to_datetime(s).date()
    except Exception:
        return pd.to_datetime(v).date()


def parse_iso_date(v) -> dt.date:
    if pd.isna(v):
        raise ValueError("Missing date")
    return pd.to_datetime(v).date()


def parse_coupon_period(
    freq_val,
) -> ql.Period:
    """
    Parse strings like: '28Dias', '30 dias', '91DÍAS', '184Dias'. Fallback to default.
    """
    if pd.isna(freq_val):
        raise Exception("Invalid freq_val")
    s = str(freq_val).strip().lower()
    m = re.search(r"(\d+)", s)
    days = int(m.group(1))
    days = days
    return ql.Period(days, ql.Days)


def parse_coupon_days(
    freq_val,
) -> int:
    """Integer version (days)."""

    m = re.search(r"(\d+)", str(freq_val).lower())
    return int(m.group(1))


def sch_len(s: ql.Schedule) -> int:
    try:
        return int(s.size())
    except Exception:
        try:
            return len(list(s.dates()))
        except Exception:
            i = 0
            while True:
                try:
                    _ = s.date(i)
                    i += 1
                except Exception:
                    break
            return i


def sch_date(s: ql.Schedule, i: int) -> ql.Date:
    try:
        return s.date(i)
    except Exception:
        return list(s.dates())[i]


def sch_dates(s: ql.Schedule) -> List[ql.Date]:
    try:
        return list(s.dates())
    except Exception:
        return [sch_date(s, j) for j in range(sch_len(s))]


@contextmanager
def ql_include_ref_events(include: bool):
    """Temporarily set Settings.includeReferenceDateEvents, then restore."""
    s = ql.Settings.instance()
    prev = s.includeReferenceDateEvents
    s.includeReferenceDateEvents = include
    try:
        yield
    finally:
        s.includeReferenceDateEvents = prev


# =============================================================================
# Build a schedule that *forces* the sheet's coupon count to match
# =============================================================================
def compute_sheet_schedule_force_match(
    row: pd.Series,
    *,
    calendar: ql.Calendar = ql.Mexico(),
    bdc: int = ql.Following,
    freq_days: ql.Period,
    adjust_maturity_date: bool = False,
    # vendor sometimes uses these alt columns
    settlement_days: int = 2,
    count_from_settlement: bool = True,
    include_boundary_for_count: bool = True,  # vendor counts the on‑settlement payment
    dc: ql.DayCounter = ql.Actual360(),  # to force DIAS TRANSC. CPN
) -> ql.Schedule:
    """
    Valmer row adapter for provider-neutral coupon schedule reconciliation.
    """
    if freq_days.units() != ql.Days:
        raise ValueError("freq_days must be a QuantLib Period in days")
    _ = dc  # kept for the existing public signature; reconciliation uses actual days.
    coupons_left = (
        int(row["cuponesxcobrar"])
        if "cuponesxcobrar" in row and pd.notna(row["cuponesxcobrar"])
        else None
    )
    dias_trans = int(row["diastransccpn"]) if pd.notna(row.get("diastransccpn")) else None

    return compute_coupon_schedule_force_match(
        valuation_date=parse_val_date(row["fecha"]),
        maturity_date=parse_iso_date(row["fechavcto"]),
        coupon_frequency_days=freq_days.length(),
        remaining_coupon_count=coupons_left,
        elapsed_coupon_days=dias_trans,
        calendar=calendar,
        business_day_convention=bdc,
        adjust_maturity_date=adjust_maturity_date,
        settlement_days=settlement_days,
        count_from_settlement=count_from_settlement,
        include_boundary_for_count=include_boundary_for_count,
    )


def _count_future_coupons(b: ql.Bond, ref_py: dt.date, include_ref: bool) -> int:
    n = 0
    with ql_include_ref_events(include_ref):
        ref = qld(ref_py)
        for cf in b.cashflows():
            if ql.as_floating_rate_coupon(cf) is None:
                continue
            if not cf.hasOccurred(ref):  # 1-arg overload only
                n += 1
    return n


def _flow_table(b: ql.Bond, *, eval_date: dt.date, settle_date: dt.date) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for cf in b.cashflows():
        is_cpn = ql.as_floating_rate_coupon(cf) is not None
        pay = pyd(cf.date())
        status_eval = "future" if pay > eval_date else ("on_ref" if pay == eval_date else "past")
        status_sett = (
            "future" if pay > settle_date else ("on_ref" if pay == settle_date else "past")
        )
        rec: Dict[str, Any] = {
            "type": "coupon" if is_cpn else "redemption",
            "pay": pay,
            "Δdays_FECHA": (pay - eval_date).days,
            "Δdays_settle": (pay - settle_date).days,
            "vs_FECHA": status_eval,
            "vs_settle": status_sett,
        }
        cpn = ql.as_floating_rate_coupon(cf)
        if cpn:
            rec.update(
                {
                    "accrual_start": pyd(cpn.accrualStartDate()),
                    "accrual_end": pyd(cpn.accrualEndDate()),
                    "fixing": pyd(cpn.fixingDate()),
                    "accrual_days": int(cpn.accrualEndDate() - cpn.accrualStartDate()),
                }
            )
        rows.append(rec)
    return pd.DataFrame(rows).sort_values(["pay", "type"]).reset_index(drop=True)


# =============================================================================
# Debug assertion with forensic report + optional auto-fix
# =============================================================================
def _snap_first_future(
    schedule: ql.Schedule,
    *,
    calendar: ql.Calendar,
    bdc: int,
    min_first_date: dt.date,
    allow_equal: bool,
) -> ql.Schedule:
    """
    Keep maturity anchored and coupon count unchanged, but ensure the first *future*
    date is >= (or >) min_first_date. (Creates a longer front stub if needed.)
    """
    dts = [pyd(d) for d in sch_dates(schedule)]
    if len(dts) <= 1:
        return schedule  # redemption-only
    prev = dts[0]
    first = dts[1]
    second = dts[2] if len(dts) >= 3 else None

    # Compute the minimum allowed first date
    snap = pyd(calendar.adjust(qld(min_first_date), bdc))
    if not allow_equal:
        if snap <= min_first_date:
            snap = pyd(calendar.advance(qld(min_first_date), 1, ql.Days, bdc))

    # If the original first is already compliant, keep it
    new_first = first
    if (first < snap) or (not allow_equal and first == snap):
        new_first = snap

    # Guard: first must be strictly before second (if present)
    if second and not (new_first < second):
        # Pull back to the previous business day before 'second'
        new_first = pyd(calendar.advance(qld(second), -1, ql.Days, ql.Preceding))
        while not (new_first < second):
            new_first = pyd(calendar.advance(qld(new_first), -1, ql.Days, ql.Preceding))

    # Rebuild schedule
    dv = ql.DateVector()
    dv.push_back(qld(prev))
    dv.push_back(qld(new_first))
    for x in dts[2:]:
        dv.push_back(qld(x))
    return ql.Schedule(dv, calendar, bdc)


def assert_schedule_matches_sheet_debug(
    row: pd.Series,
    schedule: ql.Schedule,
    *,
    calendar: ql.Calendar = ql.Mexico(),
    bdc: int = ql.Following,
    day_count: ql.DayCounter = ql.Actual360(),
    settlement_days: int = 1,
    count_from_settlement: bool = True,
    include_ref_date_events: bool = False,
    auto_fix: bool = False,
    allow_equal_on_first: bool = False,
    probe_bond: Optional[ql.Bond] = None,
) -> ql.Schedule:
    """
    Verifies that future coupon count matches the sheet AND prints a forensic
    report when it doesn't. Optionally returns a *fixed* schedule that snaps the
    first future date forward so the count matches 100%.
    """
    # 0) Core dates — use the SAME robust parser as everywhere else
    eval_date = parse_val_date(row["fecha"])
    issue_date = (
        parse_iso_date(row["fechaemision"]) if "" in row and pd.notna(row["fechaemision"]) else None
    )
    maturity = parse_iso_date(row["fechavto"])
    expected = (
        int(row["cuponesxcobrar"])
        if "cuponesxcobrar" in row and pd.notna(row["cuponesxcobrar"])
        else None
    )

    # If the sheet says 0 coupons left, nothing to count/fix: keep schedule as-is.
    if expected is not None and expected == 0:
        return schedule

    # 1) Probe bond & settlement date (use provided probe_bond or build a tiny probe)
    if probe_bond is None:
        probe_bond = _build_probe_bond(
            schedule,
            eval_date=eval_date,
            day_count=day_count,
            calendar=calendar,
            bdc=bdc,
            settlement_days=settlement_days,
            issue_date=issue_date,
        )
    settle_date = pyd(probe_bond.settlementDate())

    # 2) Counts under standard variants
    cnt_eval_excl = _count_future_coupons(probe_bond, eval_date, include_ref=False)
    cnt_eval_incl = _count_future_coupons(probe_bond, eval_date, include_ref=True)
    cnt_sett_excl = _count_future_coupons(probe_bond, settle_date, include_ref=False)
    cnt_sett_incl = _count_future_coupons(probe_bond, settle_date, include_ref=True)

    # 3) Choose the model count according to your config
    ref_date = settle_date if count_from_settlement else eval_date
    chosen_cnt = _count_future_coupons(probe_bond, ref_date, include_ref_date_events)

    # 4) Forensic table
    cf_table = _flow_table(probe_bond, eval_date=eval_date, settle_date=settle_date)

    # 5) Quick schedule digest
    sched_py = [pyd(d) for d in sch_dates(schedule)]
    first_future_idx = 1 if len(sched_py) >= 2 else None
    first_future = sched_py[first_future_idx] if first_future_idx is not None else None
    last_date = sched_py[-1] if sched_py else None

    # 6) If mismatch, print a precise “why”
    if expected is not None and chosen_cnt != expected:
        print("──────────── Coupon Count Assertion (DIAGNOSTIC) ────────────")
        print(f"EMISORA={row.get('EMISORA', '')}  SERIE={row.get('SERIE', '')}")
        print(f"FECHA={eval_date}   Settlement(T+{settlement_days})={settle_date}")
        print(f"Schedule dates ({len(sched_py)}): {sched_py}")
        print(f"Maturity (sheet)={maturity}  Schedule.last={last_date}\n")
        print("Counts:")
        print(f"  vs FECHA  include_ref=False : {cnt_eval_excl}")
        print(f"  vs FECHA  include_ref=True  : {cnt_eval_incl}")
        print(f"  vs SETTLE include_ref=False : {cnt_sett_excl}")
        print(f"  vs SETTLE include_ref=True  : {cnt_sett_incl}")
        print(
            f"Chosen (cfg: from_settlement={count_from_settlement}, "
            f"include_ref_date_events={include_ref_date_events}) -> {chosen_cnt}"
        )
        print(f"Sheet 'CUPONES X COBRAR' : {expected}\n")
        if first_future:
            ref_label = "SETTLE" if count_from_settlement else "FECHA"
            ref_val = settle_date if count_from_settlement else eval_date
            cmp = "≤" if (first_future <= ref_val) else ">"
            print(
                f"First future date = {first_future}  |  {ref_label} = {ref_val}  "
                f"→  first_future {cmp} {ref_label}"
            )
            if (include_ref_date_events is False and first_future <= ref_val) or (
                include_ref_date_events is True and first_future < ref_val
            ):
                print("⚠ This boundary is the reason you are off by exactly 1.")
                print("   Under your counting rule, that payment is considered 'past'.")
        if last_date != maturity:
            print(
                "⚠ Schedule.last differs from FECHA VCTO. If the vendor adjusts maturity, "
                "set adjust_maturity_date=True."
            )
        print("────────────────────────────────────────────────────────────")
        print(cf_table.head(10).to_string(index=False))

        if not auto_fix:
            raise AssertionError(
                f"Coupon count mismatch. sheet={expected} model={chosen_cnt} "
                f"(from_settlement={count_from_settlement}, "
                f"include_ref_date_events={include_ref_date_events})."
            )

    # 7) Auto-fix: snap the first future date forward so the count matches the sheet
    if expected is not None and chosen_cnt != expected and auto_fix:
        ref_val = settle_date if count_from_settlement else eval_date
        fixed = _snap_first_future(
            schedule,
            calendar=calendar,
            bdc=bdc,
            min_first_date=ref_val,
            allow_equal=include_ref_date_events,
        )
        # Recount after fix
        probe2 = _build_probe_bond(
            fixed,
            eval_date=eval_date,
            day_count=day_count,
            calendar=calendar,
            bdc=bdc,
            settlement_days=settlement_days,
            issue_date=issue_date,
        )
        chosen_cnt2 = _count_future_coupons(probe2, ref_val, include_ref_date_events)
        if chosen_cnt2 != expected:
            print("Auto-fix attempted but counts still differ.")
            print("Fixed schedule:", [pyd(d) for d in sch_dates(fixed)])
            # raise AssertionError(f"After auto-fix, sheet={expected} model={chosen_cnt2}")
        return fixed

    return schedule


# =============================================================================
# Coupon counters for built instruments
# =============================================================================


def count_future_coupons(
    b: ql.Bond,
    *,
    from_settlement: bool = COUNT_FROM_SETTLEMENT,
    include_ref_date_events: bool = INCLUDE_REF_DATE_EVENTS,
) -> int:
    """
    Count future coupons the way QL does it:
    - reference date = settlementDate() (if from_settlement) else Settings.evaluationDate
    - includeRefDateEvents comes from Settings at call time (Python wheel exposes only 0/1 arg)
    """
    ref = b.settlementDate() if from_settlement else ql.Settings.instance().evaluationDate
    n = 0
    with ql_include_ref_events(include_ref_date_events):
        for cf in b.cashflows():
            if isinstance(b, ql.FloatingRateBond):
                cpn = ql.as_floating_rate_coupon(cf)
            else:
                cpn = ql.as_fixed_rate_coupon(cf)

            if cpn is None:
                continue
            if not cf.hasOccurred(ref):  # one-arg form only
                n += 1
    return n


# =============================================================================
# Build a QL floater from a sheet row + curve
# =============================================================================


def valmer_row_to_core_bond_pricing_payload(
    row: pd.Series,
    *,
    calendar: ql.Calendar,
    dc: ql.DayCounter,
    bdc: int,
    settlement_days: int,
    SPREAD_IS_PERCENT: bool = True,
) -> CoreBondPricingPayload:
    eval_date = parse_val_date(row["fecha"])
    issue_date = parse_iso_date(row["fechaemision"])
    maturity_date = parse_iso_date(row["fechavcto"])
    face_value = float(row["valornominalactualizado"])
    raw_spread = 0.0 if pd.isna(row["sobretasa"]) else float(row["sobretasa"])
    spread_decimal = (raw_spread / 100.0) if SPREAD_IS_PERCENT else raw_spread

    coupon_rule = row["reglacupon"]
    emisora = row["emisora"]
    tipo_valor = row["tipovalor"]
    cuponesemision = row["cuponesemision"]

    zero_corps_tipo_valor = ["I", "93", "92"]

    is_zero_coupon = tipo_valor in zero_corps_tipo_valor or emisora in ["CETES"]
    is_zero_coupon = is_zero_coupon if cuponesemision == 0 else False
    coupon_frequency = None
    explicit_schedule = None

    if not is_zero_coupon:
        try:
            if f"{tipo_valor}_{emisora}" in ["MC_BONOS", "MP_BONOS"]:
                coupon_frequency = parse_coupon_period(182)
                is_zero_coupon = True
            elif tipo_valor in ["JI"]:
                # Bonos internacionales o multilaterales en pesos, tasa fija
                coupon_frequency = parse_coupon_period(182)
            elif tipo_valor in ["D2", "D8"]:
                coupon_frequency = parse_coupon_period(182)
            else:
                coupon_frequency = parse_coupon_period(row.get("freccpn"))
        except Exception as e:
            raise e
        explicit_schedule = compute_sheet_schedule_force_match(
            row,
            calendar=calendar,
            bdc=bdc,
            freq_days=coupon_frequency,
            settlement_days=settlement_days,  # ← add this
            count_from_settlement=COUNT_FROM_SETTLEMENT,  # ← and this (keeps your toggle)
            include_boundary_for_count=True,  # ← ven
        )
    if is_zero_coupon:
        assert cuponesemision == 0, "No Zero coupon bond review log"
        if "BONDES" in emisora:
            benchmark_rate_index_identifier = SUBYACENTE_TO_INDEX_MAP["CETE_28"]
        elif tipo_valor in ["MC", "MP"]:
            benchmark_rate_index_identifier = SUBYACENTE_TO_INDEX_MAP["CETE182"]
        elif tipo_valor in zero_corps_tipo_valor:
            try:
                benchmark_rate_index_identifier = SUBYACENTE_TO_INDEX_MAP[row["subyacente"]]
            except Exception as e:
                raise e
        elif emisora in ["CETES"]:
            benchmark_rate_index_identifier = SUBYACENTE_TO_INDEX_MAP["CETE_28"]
        else:
            raise NotImplementedError
        benchmark_rate_index_uid = resolve_reference_index_uid(
            benchmark_rate_index_identifier
        )

        return CoreBondPricingPayload(
            instrument_type="zero_coupon_bond",
            valuation_date=eval_date,
            issue_date=issue_date,
            maturity_date=maturity_date,
            face_value=face_value,
            day_count=dc,
            calendar=calendar,
            business_day_convention=bdc,
            settlement_days=settlement_days,
            benchmark_rate_index_uid=benchmark_rate_index_uid,
        )

    elif coupon_rule == "Tasa Fija":  # Fixed Rate Bond
        benchmark_rate_index_identifier = SUBYACENTE_TO_INDEX_MAP[row["subyacente"]]
        benchmark_rate_index_uid = resolve_reference_index_uid(
            benchmark_rate_index_identifier
        )

        return CoreBondPricingPayload(
            instrument_type="fixed_rate_bond",
            valuation_date=eval_date,
            issue_date=issue_date,
            maturity_date=maturity_date,
            face_value=face_value,
            day_count=dc,
            calendar=calendar,
            business_day_convention=bdc,
            settlement_days=settlement_days,
            benchmark_rate_index_uid=benchmark_rate_index_uid,
            coupon_rate=float(row["tasacupon"]) / 100,
            coupon_frequency=coupon_frequency,
            schedule=explicit_schedule,
        )

    else:  # floating rate bond
        try:
            floating_rate_index_identifier = SUBYACENTE_TO_INDEX_MAP[row["subyacente"]]
        except KeyError as e:
            raise e
        floating_rate_index_uid = resolve_reference_index_uid(
            floating_rate_index_identifier
        )

        return CoreBondPricingPayload(
            instrument_type="floating_rate_bond",
            valuation_date=eval_date,
            issue_date=issue_date,
            maturity_date=maturity_date,
            face_value=face_value,
            day_count=dc,
            calendar=calendar,
            business_day_convention=bdc,
            settlement_days=settlement_days,
            benchmark_rate_index_uid=floating_rate_index_uid,
            floating_rate_index_uid=floating_rate_index_uid,
            spread=spread_decimal,
            coupon_frequency=coupon_frequency,
            schedule=explicit_schedule,
        )


def build_instrument_from_core_bond_pricing_payload(
    payload: CoreBondPricingPayload,
) -> msi.Instrument:
    ql.Settings.instance().evaluationDate = qld(payload.valuation_date)
    ql.Settings.instance().includeReferenceDateEvents = INCLUDE_REF_DATE_EVENTS
    ql.Settings.instance().enforceTodaysHistoricFixings = False

    if payload.instrument_type == "zero_coupon_bond":
        instrument = msi.ZeroCouponBond(
            face_value=payload.face_value,
            benchmark_rate_index_uid=payload.benchmark_rate_index_uid,
            issue_date=payload.issue_date,
            maturity_date=payload.maturity_date,
            day_count=payload.day_count,
            calendar=payload.calendar,
            business_day_convention=payload.business_day_convention,
            settlement_days=payload.settlement_days,
        )
    elif payload.instrument_type == "fixed_rate_bond":
        instrument = msi.FixedRateBond(
            face_value=payload.face_value,
            coupon_rate=payload.coupon_rate,
            benchmark_rate_index_uid=payload.benchmark_rate_index_uid,
            issue_date=payload.issue_date,
            maturity_date=payload.maturity_date,
            coupon_frequency=payload.coupon_frequency,
            day_count=payload.day_count,
            calendar=payload.calendar,
            business_day_convention=payload.business_day_convention,
            settlement_days=payload.settlement_days,
            schedule=payload.schedule,
        )
    elif payload.instrument_type == "floating_rate_bond":
        instrument = msi.FloatingRateBond(
            face_value=payload.face_value,
            floating_rate_index_uid=payload.floating_rate_index_uid,
            spread=payload.spread,
            issue_date=payload.issue_date,
            maturity_date=payload.maturity_date,
            coupon_frequency=payload.coupon_frequency,
            day_count=payload.day_count,
            calendar=payload.calendar,
            business_day_convention=payload.business_day_convention,
            settlement_days=payload.settlement_days,
            benchmark_rate_index_uid=payload.benchmark_rate_index_uid,
            schedule=payload.schedule,
        )
    else:
        raise ValueError(f"Unsupported core bond pricing payload type: {payload.instrument_type}")

    instrument.set_valuation_date(payload.valuation_date)
    return instrument


def build_qll_bond_from_row(
    row: pd.Series,
    *,
    calendar: ql.Calendar,
    dc: ql.DayCounter,
    bdc: int,
    settlement_days: int,
    SPREAD_IS_PERCENT: bool = True,
) -> msi.Instrument:
    """
    Convert a Valmer row to a normalized pricing payload, then build an instrument.
    """
    payload = valmer_row_to_core_bond_pricing_payload(
        row,
        calendar=calendar,
        dc=dc,
        bdc=bdc,
        settlement_days=settlement_days,
        SPREAD_IS_PERCENT=SPREAD_IS_PERCENT,
    )
    frb = build_instrument_from_core_bond_pricing_payload(payload)

    # --- assert/diagnose + (optionally) auto-fix the front boundary ---
    # with_yield = float(row["TASA DE RENDIMIENTO"]) / 100
    # try:
    #     frb._setup_pricer(with_yield=with_yield)
    # except Exception as e:
    #     raise e
    # fixed_schedule = assert_schedule_matches_sheet_debug(
    #     row,
    #     explicit_schedule,
    #     calendar=calendar,
    #     bdc=bdc,                    # ✅ correct type (int)
    #     day_count=dc,               # ✅ correct type (ql.DayCounter)
    #     settlement_days=settlement_days,
    #     count_from_settlement=COUNT_FROM_SETTLEMENT,
    #     include_ref_date_events=INCLUDE_REF_DATE_EVENTS,
    #     auto_fix=True,                                   # snap first future if needed
    #     allow_equal_on_first=INCLUDE_REF_DATE_EVENTS,    # align with chosen include-ref rule
    #     probe_bond=frb._bond
    # )
    # def _dates(s: ql.Schedule) -> List[dt.date]:
    #     return [pyd(d) for d in sch_dates(s)]
    # if _dates(fixed_schedule) != _dates(explicit_schedule):
    #     frb = FloatingRateBond(
    #         face_value=face_adj,
    #         floating_rate_index=tiie_index,
    #         spread=spread_decimal,
    #         issue_date=issue_date,
    #         maturity_date=maturity_date,
    #         coupon_frequency=parse_coupon_period(row.get("FREC. CPN"), 28),
    #         day_count=dc,
    #         calendar=calendar,
    #         business_day_convention=bdc,
    #         settlement_days=settlement_days,
    #         valuation_date=eval_date,
    #         schedule=fixed_schedule,  # <— IMPORTANT
    #     )
    return frb


# =============================================================================
# Cashflow extraction (future only)
# =============================================================================f
def extract_future_cashflows(
    built: BuiltBond,
    *,
    from_settlement: bool = COUNT_FROM_SETTLEMENT,
    include_ref_date_events: bool = INCLUDE_REF_DATE_EVENTS,
) -> Dict[str, List[Dict[str, Any]]]:
    ql_bond = built.bond.bond
    ql.Settings.instance().evaluationDate = qld(built.eval_date)
    ref = ql_bond.settlementDate() if from_settlement else ql.Settings.instance().evaluationDate

    out: Dict[str, List[Dict[str, Any]]] = {"floating": [], "redemption": []}
    with ql_include_ref_events(include_ref_date_events):
        for cf in ql_bond.cashflows():
            if cf.hasOccurred(ref):  # one-arg form only
                continue
            cpn = ql.as_floating_rate_coupon(cf)
            if cpn is not None:
                out["floating"].append(
                    {
                        "payment_date": pyd(cpn.date()),
                        "fixing_date": pyd(cpn.fixingDate()),
                        "rate": float(cpn.rate()),
                        "spread": float(cpn.spread()),
                        "amount": float(cpn.amount()),
                    }
                )
            else:
                out["redemption"].append(
                    {
                        "payment_date": pyd(cf.date()),
                        "amount": float(cf.amount()),
                    }
                )
    return out


# =============================================================================
# Main pricing loop
# =============================================================================


def get_instrument_conventions(row: pd.Series) -> Tuple[ql.Calendar, int, int, ql.DayCounter]:
    """
    Determines the correct QuantLib market conventions for a given instrument.

    This function inspects the currency and underlying type of an instrument
    to return the appropriate calendar, business day convention, settlement days,
    and day count convention.

    Args:
        row: A pandas Series representing a single instrument, requiring at least
             'monedaemision' and 'subyacente' fields.

    Returns:
        A tuple containing:
        (ql.Calendar, business_day_convention, settlement_days, ql.DayCounter)

    Raises:
        NotImplementedError: If the currency is not supported.
        ValueError: If the underlying type is not supported for a given currency.
    """
    currency = row["monedaemision"]
    subyacente = row["subyacente"]
    emisora = row["emisora"]
    tipo_valor = row["tipovalor"]
    zero_typo_valor_cors = ["I", "93", "92"]
    if currency == "MPS":  # Mexican Peso
        calendar = ql.Mexico(ql.Mexico.BMV)

        is_bondes = emisora in ["BONDESD", "BONDESF", "BONDESG"]
        # Check for standard Mexican money market and short-term instruments
        try:
            if tipo_valor in zero_typo_valor_cors:
                calendar = ql.Mexico(ql.Mexico.BMV)
                day_count = ql.Actual360()
                business_day_convention = ql.Following
                settlement_days = 1
            elif (
                any(
                    keyword in subyacente
                    for keyword in ["Bonos M", "CETE", "Cetes", "IRMXP-FGub-28", "IRMXP-FGub-91"]
                )
                or is_bondes
            ):
                business_day_convention = ql.Following
                settlement_days = 1  # T+1 is standard for these
                day_count = ql.Actual360()

            elif any(keyword in subyacente for keyword in ["TIIE"]):
                calendar = ql.Mexico(ql.Mexico.BMV)
                day_count = ql.Actual360()
                business_day_convention = ql.Following
                settlement_days = 1
            else:
                # If the currency is supported but the instrument type is not, raise a specific error
                raise ValueError(
                    f"Unsupported 'subyacente' for currency '{currency}': {subyacente}"
                )
        except Exception as e:
            raise e

        return calendar, business_day_convention, settlement_days, day_count
    else:
        # If the currency is not recognized, raise an error
        raise NotImplementedError(f"Conventions for currency '{currency}' are not implemented.")


def run_price_check(
    bonos_df: pd.DataFrame,
    *,
    SPREAD_IS_PERCENT: bool = True,
    price_tol_bp: float = 2.0,
) -> Tuple[pd.DataFrame, Dict[str, Dict[str, Any]]]:
    results: List[Dict[str, Any]] = []
    instrument_map: Dict[str, Dict[str, Any]] = {}

    for ix, row in tqdm(bonos_df.iterrows(), desc="building instruments"):
        eval_date = parse_val_date(row["fecha"])
        eval_date = dt.datetime(eval_date.year, eval_date.month, eval_date.day, tzinfo=pytz.utc)

        if row["cuponactual"] == 0.0 or row["cuponesxcobrar"] == 0:
            continue

        icalendar, business_day_convention, settlement_days, day_count = get_instrument_conventions(
            row
        )

        # Build bond (explicit schedule)
        try:
            bond = build_qll_bond_from_row(
                row,
                calendar=icalendar,
                dc=day_count,
                bdc=business_day_convention,
                settlement_days=settlement_days,
            )
        except Exception as e:
            print(row)
            raise e

        face = float(row["valornominalactualizado"])
        mkt_dirty = float(row["preciosucio"])
        mkt_clean = float(row["preciolimpio"])
        if mkt_dirty == 0:
            continue

        expected_count = (
            int(row["cuponesxcobrar"]) if not pd.isna(row.get("cuponesxcobrar")) else None
        )
        market_current_coupon = (
            float(row["cuponactual"]) if not pd.isna(row.get("cuponactual")) else None
        )
        comparison = compare_bond_to_market_quote(
            bond,
            market_dirty_price=mkt_dirty,
            market_clean_price=mkt_clean,
            market_current_coupon=market_current_coupon,
            expected_future_coupon_count=expected_count,
            yield_to_maturity=float(row["tasaderendimiento"]) / 100.0,
            face_value=face,
            valuation_date=eval_date,
            price_tolerance_bp=price_tol_bp,
            from_settlement=COUNT_FROM_SETTLEMENT,
            include_ref_date_events=INCLUDE_REF_DATE_EVENTS,
        )

        instrument_hash = bond.content_hash()
        results.append(
            {
                "instrument_hash": instrument_hash,
                "FECHA": eval_date,
                "UID": f"{row['tipovalor']}_{row['emisora']}_{row['serie']}",
                "SUBYACENTE": row["subyacente"],
                "VALOR NOMINAL": float(row["valornominal"]),
                "SOBRETASA_in": float(row["sobretasa"]),
                "SOBRETASA_decimal": (float(row["sobretasa"]) / 100.0)
                if SPREAD_IS_PERCENT and not pd.isna(row["sobretasa"])
                else float(row["sobretasa"] or 0.0),
                "CUPON ACTUAL (sheet) %": float(row["cuponactual"]),
                "CUPON ACTUAL (model) %": comparison["model_current_coupon"],
                "coupon_diff_bp": comparison["coupon_diff_bp"],
                "PRECIO SUCIO (sheet)": mkt_dirty,
                "PRECIO SUCIO (model)": comparison["model_dirty_price"],
                "price_diff_bp": comparison["price_diff_bp"],
                "PRECIO LIMPIO (sheet)": mkt_clean,
                "PRECIO LIMPIO (model)": comparison["model_clean_price"],
                "accrued_per_100 (model)": comparison["model_accrued_per_100"],
                "CUPONES X COBRAR (sheet)": expected_count
                if expected_count is not None
                else np.nan,
                "CUPONES FUTUROS (model)": comparison["future_coupon_count"],
                "pass_price": comparison["pass_price"],
                "pass_coupon_count": comparison["pass_coupon_count"],
                "DIAS TRANSC. CPN (sheet)": int(row["diastransccpn"])
                if pd.notna(row.get("diastransccpn"))
                else np.nan,
                "DIAS TRANSC. CPN (model)": comparison["elapsed_coupon_days"],
            }
        )

        instrument_map[instrument_hash] = {
            "instrument": bond,
            "extra_market_info": {"yield": row["tasaderendimiento"] / 100},
        }

    return pd.DataFrame(results), instrument_map


def normalize_column_name(col_name: str) -> str:
    """
    Removes special characters and newlines from a string and converts it to lowercase.
    """
    # Replace newlines and then remove all non-alphanumeric characters
    cleaned_name = str(col_name).replace("\n", " ")
    return re.sub(r"[^a-z0-9]", "", cleaned_name.lower())


def build_valuation_position_from_sheet(
    sheet_path: str | Path,
    *,
    notional_per_line: float = 100_000_000.0,
    out_path: str | Path | None = None,
    market_data_set: Any = None,
    publish_report_artifact: bool = True,
) -> ValmerValuationPositionBuildResult:
    """
    Build a transient ValuationPosition from a Valmer vendor sheet.

    The generated basket is in-memory pricing input. It is not persisted as
    account holdings, target positions, or any durable pricing position row.
    """
    sheet_path = str(sheet_path)
    df = pd.read_excel(sheet_path)
    df.columns = [normalize_column_name(col) for col in df.columns]

    floating_tiie = df[df["subyacente"].astype(str).str.contains("TIIE", na=False)]
    floating_cetes = df[df["subyacente"].astype(str).str.contains("CETE", na=False)]
    m_bono_fixed_0 = df[df["subyacente"].astype(str).str.contains("Bonos M", na=False)]
    m_bono_fixed_0 = m_bono_fixed_0[m_bono_fixed_0.monedaemision == "MPS"]
    all_floating = pd.concat(
        [floating_tiie, floating_cetes, m_bono_fixed_0], axis=0, ignore_index=True
    )

    df_out, instrument_map = run_price_check(all_floating)
    pd.set_option("display.float_format", lambda x: f"{x:,.6f}")

    ms_assets_map = {
        uid: str(asset_uid)
        for uid, asset_uid in resolve_valmer_asset_uids(df_out["UID"].to_list()).items()
    }

    df_out["asset_uid"] = df_out["UID"].map(ms_assets_map)
    df_out["asset_id"] = df_out["asset_uid"]

    valuation_position = _build_valuation_position_from_price_check(
        df_out,
        instrument_map,
        notional_per_line=notional_per_line,
        market_data_set=market_data_set,
    )
    report_csv_path, report_artifact_uid = _publish_price_check_report(
        df_out,
        out_path=out_path,
        publish_report_artifact=publish_report_artifact,
    )

    return ValmerValuationPositionBuildResult(
        valuation_position=valuation_position,
        price_check_frame=df_out,
        instrument_map=instrument_map,
        report_artifact_uid=report_artifact_uid,
        report_csv_path=report_csv_path,
    )


def _build_valuation_position_from_price_check(
    price_check_frame: pd.DataFrame,
    instrument_map: Dict[str, Dict[str, Any]],
    *,
    notional_per_line: float,
    market_data_set: Any = None,
) -> ValuationPosition:
    if price_check_frame.empty:
        raise ValueError("Cannot build a ValuationPosition from an empty price-check frame.")

    valuation_dates = pd.to_datetime(
        price_check_frame["FECHA"],
        utc=True,
        errors="coerce",
    ).dropna()
    unique_dates = valuation_dates.dt.normalize().unique()
    if len(unique_dates) != 1:
        raise ValueError(
            "ValuationPosition requires exactly one valuation date per sheet; "
            f"found {len(unique_dates)}."
        )

    lines: list[ValuationLine] = []
    for row in price_check_frame.to_dict("records"):
        instrument_hash = str(row["instrument_hash"])
        try:
            instrument_entry = instrument_map[instrument_hash]
        except KeyError as exc:
            raise KeyError(
                f"Missing instrument for price-check row hash {instrument_hash!r}."
            ) from exc

        instrument = instrument_entry["instrument"]
        face_value = float(getattr(instrument, "face_value"))
        if face_value <= 0:
            raise ValueError(
                f"Instrument {instrument_hash!r} has non-positive face_value {face_value}."
            )

        asset_uid = _optional_uuid(row.get("asset_uid"))
        extra_market_info = dict(instrument_entry.get("extra_market_info", {}))
        lines.append(
            ValuationLine(
                instrument=instrument,
                units=float(notional_per_line) / face_value,
                asset_uid=asset_uid,
                metadata_json={
                    "valmer_unique_identifier": row.get("UID"),
                    "instrument_hash": instrument_hash,
                    "subyacente": row.get("SUBYACENTE"),
                    "extra_market_info": extra_market_info,
                },
            )
        )

    valuation_date = pd.Timestamp(unique_dates[0]).to_pydatetime()
    return ValuationPosition(
        valuation_date=valuation_date,
        market_data_set=market_data_set,
        lines=lines,
    )


def _publish_price_check_report(
    price_check_frame: pd.DataFrame,
    *,
    out_path: str | Path | None,
    publish_report_artifact: bool,
) -> tuple[str | None, str | None]:
    report_csv_path: str | None = None

    if out_path is not None:
        path = Path(out_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        price_check_frame.to_csv(path, index=False)
        report_csv_path = str(path)

    if not publish_report_artifact:
        return report_csv_path, None

    from valmer_connectors.settings import PROJECT_BUCKET_NAME

    if report_csv_path is not None:
        artifact = Artifact.get_or_create(
            bucket_name=PROJECT_BUCKET_NAME,
            name="instrument_pricing_match",
            created_by_resource_name=__file__,
            filepath=report_csv_path,
        )
        return report_csv_path, _artifact_uid(artifact)

    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv", delete=True) as temp_csv:
        price_check_frame.to_csv(temp_csv.name, index=False)
        artifact = Artifact.get_or_create(
            bucket_name=PROJECT_BUCKET_NAME,
            name="instrument_pricing_match",
            created_by_resource_name=__file__,
            filepath=temp_csv.name,
        )
        return None, _artifact_uid(artifact)


def _optional_uuid(value: Any) -> uuid.UUID | None:
    if value is None or pd.isna(value):
        return None
    return uuid.UUID(str(value))


def _artifact_uid(artifact: Any) -> str | None:
    for attribute in ("uid", "artifact_uid", "id"):
        value = getattr(artifact, attribute, None)
        if value is not None:
            return str(value)
    return None
