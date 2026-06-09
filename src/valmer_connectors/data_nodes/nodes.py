import os
import re
from dataclasses import dataclass
from datetime import timedelta
from typing import List, Union

import pandas as pd
import structlog
from mainsequence.client.models_foundry import Artifact
from msm.api.assets import Asset as MarketsAsset
from msm.api.base import operation_result_rows
from msm.constants import ASSET_TYPE_BOND
from msm.data_nodes import AssetIndexedDataNode, AssetIndexedDataNodeConfiguration
from msm.repositories.base import compile_markets_statement
from msm.repositories.base import execute_markets_operation
from msm.settings import ASSET_IDENTIFIER_DIMENSION
from msm_pricing.api.instruments import persist_current_pricing_details
from msm_pricing.api.pricing_details import AssetCurrentPricingDetails
from pydantic import Field
from sqlalchemy import select
from tqdm import tqdm

from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
from valmer_connectors.instruments.asset_identity import (
    add_valmer_unique_identifier,
    resolve_valmer_asset_refs,
    upsert_valmer_assets,
)
from valmer_connectors.instruments.vector_to_asset import (
    build_qll_bond_from_row,
    get_instrument_conventions,
    normalize_column_name,
)
from valmer_connectors.meta_tables.valmer_asset_details import (
    VALMER_ASSET_DETAIL_SOURCE_COLUMNS,
    upsert_valmer_asset_details,
)
from valmer_connectors.settings import resolve_valmer_meta_operation_batch_size


@dataclass(frozen=True)
class ValmerColumnSpec:
    source_name: str | None
    column_name: str
    dtype: str
    transform: str
    label: str
    description: str


def _coerce_valmer_series(series: pd.Series, transform: str) -> pd.Series:
    if transform == "string":
        return series.astype("string")

    if transform == "float":
        return pd.to_numeric(series, errors="coerce").astype("float64")

    if transform == "int":
        return pd.to_numeric(series, errors="coerce").astype("Int64")

    if transform == "percent":
        cleaned = (
            series.astype("string")
            .str.replace("%", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        return pd.to_numeric(cleaned, errors="coerce").astype("float64")

    if transform == "datetime":
        return _as_utc_ns(pd.to_datetime(series, errors="coerce", utc=True))

    if transform == "date_ymd":
        return _as_utc_ns(
            pd.to_datetime(series.astype("string"), format="%Y%m%d", errors="coerce", utc=True)
        )

    raise ValueError(f"Unsupported Valmer transform: {transform}")


def _as_utc_ns(series: pd.Series) -> pd.Series:
    return pd.Series(series, index=series.index).astype("datetime64[ns, UTC]")


def _pricing_detail_face_value(instrument_dump: dict) -> object:
    if not isinstance(instrument_dump, dict):
        return None
    if "face_value" in instrument_dump:
        return instrument_dump.get("face_value")
    wrapped_instrument = instrument_dump.get("instrument")
    if isinstance(wrapped_instrument, dict):
        return wrapped_instrument.get("face_value")
    return None


def _build_open_time_series(time_index: pd.Series) -> pd.Series:
    open_time = pd.Series(pd.NA, index=time_index.index, dtype="Int64")
    valid = time_index.notna()
    if valid.any():
        open_time.loc[valid] = (time_index.loc[valid].astype("int64") // 10**9).astype("Int64")
    return open_time


def _coerce_nullable_integer_for_storage(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def _normalize_nullable_integer_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    normalized = frame.copy()
    for column in columns:
        if column in normalized.columns:
            normalized[column] = _coerce_nullable_integer_for_storage(normalized[column])
    return normalized


VALMER_DERIVED_COLUMN_SPECS = (
    ValmerColumnSpec(
        source_name=None,
        column_name="open",
        dtype="float",
        transform="float",
        label="Open",
        description="Synthetic OHLC open copied from the Valmer dirty price.",
    ),
    ValmerColumnSpec(
        source_name=None,
        column_name="high",
        dtype="float",
        transform="float",
        label="High",
        description="Synthetic OHLC high copied from the Valmer dirty price.",
    ),
    ValmerColumnSpec(
        source_name=None,
        column_name="low",
        dtype="float",
        transform="float",
        label="Low",
        description="Synthetic OHLC low copied from the Valmer dirty price.",
    ),
    ValmerColumnSpec(
        source_name=None,
        column_name="close",
        dtype="float",
        transform="float",
        label="Close",
        description="Synthetic OHLC close copied from the Valmer dirty price.",
    ),
    ValmerColumnSpec(
        source_name=None,
        column_name="volume",
        dtype="int",
        transform="int",
        label="Volume",
        description="Synthetic volume placeholder published as 0.",
    ),
    ValmerColumnSpec(
        source_name=None,
        column_name="open_time",
        dtype="int",
        transform="int",
        label="Open Time",
        description="Unix timestamp in seconds for the published end-of-day bar.",
    ),
)


VALMER_SOURCE_COLUMN_SPECS = (
    ValmerColumnSpec(
        source_name="fecha",
        column_name="valuation_date",
        dtype="datetime",
        transform="date_ymd",
        label="Valuation Date",
        description="Source FECHA value normalized to a UTC date.",
    ),
    ValmerColumnSpec(
        source_name="preciolimpio",
        column_name="clean_price",
        dtype="float",
        transform="float",
        label="Clean Price",
        description="Clean price from PRECIO LIMPIO.",
    ),
    ValmerColumnSpec(
        source_name="preciosucio",
        column_name="dirty_price",
        dtype="float",
        transform="float",
        label="Dirty Price",
        description="Dirty price from PRECIO SUCIO.",
    ),
    ValmerColumnSpec(
        source_name="interesesacumulados",
        column_name="accrued_interest",
        dtype="float",
        transform="float",
        label="Accrued Interest",
        description="Accrued interest from INTERESES ACUMULADOS.",
    ),
    ValmerColumnSpec(
        source_name="cuponactual",
        column_name="current_coupon",
        dtype="float",
        transform="float",
        label="Current Coupon",
        description="Current coupon from CUPON ACTUAL.",
    ),
    ValmerColumnSpec(
        source_name="sobretasa",
        column_name="spread",
        dtype="float",
        transform="float",
        label="Spread",
        description="Spread from SOBRETASA.",
    ),
    ValmerColumnSpec(
        source_name="montoencirculacion",
        column_name="amount_outstanding",
        dtype="float",
        transform="float",
        label="Amount Outstanding",
        description="Outstanding amount from MONTO EN CIRCULACION.",
    ),
    ValmerColumnSpec(
        source_name="diastransccpn",
        column_name="days_since_coupon",
        dtype="float",
        transform="float",
        label="Days Since Coupon",
        description="Days since coupon from DIAS TRANSC. CPN.",
    ),
    ValmerColumnSpec(
        source_name="cuponesxcobrar",
        column_name="coupons_remaining",
        dtype="float",
        transform="float",
        label="Coupons Remaining",
        description="Remaining coupon count from CUPONES X COBRAR.",
    ),
    ValmerColumnSpec(
        source_name="hechodemkt",
        column_name="market_event",
        dtype="string",
        transform="string",
        label="Market Event",
        description="Market event marker from HECHO DE MKT.",
    ),
    ValmerColumnSpec(
        source_name="fechauh",
        column_name="uh_date",
        dtype="datetime",
        transform="datetime",
        label="UH Date",
        description="Vendor FECHA U.H. date field preserved as provided by Valmer.",
    ),
    ValmerColumnSpec(
        source_name="precioteorico",
        column_name="theoretical_price",
        dtype="float",
        transform="float",
        label="Theoretical Price",
        description="Theoretical price from PRECIO TEORICO.",
    ),
    ValmerColumnSpec(
        source_name="postcompra",
        column_name="posted_bid",
        dtype="float",
        transform="float",
        label="Posted Bid",
        description="Posted bid from POST COMPRA.",
    ),
    ValmerColumnSpec(
        source_name="postventa",
        column_name="posted_ask",
        dtype="float",
        transform="float",
        label="Posted Ask",
        description="Posted ask from POST VENTA.",
    ),
    ValmerColumnSpec(
        source_name="yieldcompra",
        column_name="bid_yield",
        dtype="float",
        transform="float",
        label="Bid Yield",
        description="Bid yield from YIELD COMPRA.",
    ),
    ValmerColumnSpec(
        source_name="yieldventa",
        column_name="ask_yield",
        dtype="float",
        transform="float",
        label="Ask Yield",
        description="Ask yield from YIELD VENTA.",
    ),
    ValmerColumnSpec(
        source_name="spreadcompra",
        column_name="bid_spread",
        dtype="float",
        transform="float",
        label="Bid Spread",
        description="Bid spread from SPREAD COMPRA.",
    ),
    ValmerColumnSpec(
        source_name="spreadventa",
        column_name="ask_spread",
        dtype="float",
        transform="float",
        label="Ask Spread",
        description="Ask spread from SPREAD VENTA.",
    ),
    ValmerColumnSpec(
        source_name="mdys",
        column_name="moodys_rating",
        dtype="string",
        transform="string",
        label="Moody's Rating",
        description="Moody's rating from MDYS.",
    ),
    ValmerColumnSpec(
        source_name="sp",
        column_name="sp_rating",
        dtype="string",
        transform="string",
        label="S&P Rating",
        description="S&P rating from S&P.",
    ),
    ValmerColumnSpec(
        source_name="bursatilidad",
        column_name="marketability",
        dtype="string",
        transform="string",
        label="Marketability",
        description="Marketability label from BURSATILIDAD.",
    ),
    ValmerColumnSpec(
        source_name="liquidez",
        column_name="liquidity",
        dtype="float",
        transform="float",
        label="Liquidity",
        description="Liquidity value from LIQUIDEZ.",
    ),
    ValmerColumnSpec(
        source_name="cambiodiario",
        column_name="daily_change_pct",
        dtype="float",
        transform="percent",
        label="Daily Change Pct",
        description="Daily change from CAMBIO DIARIO, stored as a numeric percentage value.",
    ),
    ValmerColumnSpec(
        source_name="cambiosemanal",
        column_name="weekly_change_pct",
        dtype="float",
        transform="percent",
        label="Weekly Change Pct",
        description="Weekly change from CAMBIO SEMANAL, stored as a numeric percentage value.",
    ),
    ValmerColumnSpec(
        source_name="preciomax12m",
        column_name="max_price_12m",
        dtype="float",
        transform="float",
        label="Max Price 12M",
        description="Twelve-month high price from PRECIO MAX 12M.",
    ),
    ValmerColumnSpec(
        source_name="preciomin12m",
        column_name="min_price_12m",
        dtype="float",
        transform="float",
        label="Min Price 12M",
        description="Twelve-month low price from PRECIO MIN 12M.",
    ),
    ValmerColumnSpec(
        source_name="suspension",
        column_name="suspension_status",
        dtype="string",
        transform="string",
        label="Suspension Status",
        description="Suspension status from SUSPENSION.",
    ),
    ValmerColumnSpec(
        source_name="volatilidad",
        column_name="volatility",
        dtype="float",
        transform="float",
        label="Volatility",
        description="Volatility from VOLATILIDAD.",
    ),
    ValmerColumnSpec(
        source_name="volatilidad2",
        column_name="volatility_secondary",
        dtype="float",
        transform="float",
        label="Secondary Volatility",
        description="Secondary volatility from VOLATILIDAD 2.",
    ),
    ValmerColumnSpec(
        source_name="duracion",
        column_name="duration",
        dtype="float",
        transform="float",
        label="Duration",
        description="Duration from DURACION.",
    ),
    ValmerColumnSpec(
        source_name="duracionmonet",
        column_name="monetary_duration",
        dtype="float",
        transform="float",
        label="Monetary Duration",
        description="Monetary duration from DURACION MONET.",
    ),
    ValmerColumnSpec(
        source_name="convexidad",
        column_name="convexity",
        dtype="float",
        transform="float",
        label="Convexity",
        description="Convexity from CONVEXIDAD.",
    ),
    ValmerColumnSpec(
        source_name="var",
        column_name="value_at_risk",
        dtype="float",
        transform="float",
        label="Value At Risk",
        description="Value at risk from VAR.",
    ),
    ValmerColumnSpec(
        source_name="desviacionstand",
        column_name="standard_deviation",
        dtype="float",
        transform="float",
        label="Standard Deviation",
        description="Standard deviation from DESVIACION STAND.",
    ),
    ValmerColumnSpec(
        source_name="valornominalactualizado",
        column_name="adjusted_face_value",
        dtype="float",
        transform="float",
        label="Adjusted Face Value",
        description="Adjusted face value from VALOR NOMINAL ACTUALIZADO.",
    ),
    ValmerColumnSpec(
        source_name="calificacionfitch",
        column_name="fitch_rating",
        dtype="string",
        transform="string",
        label="Fitch Rating",
        description="Fitch rating from CALIFICACION FITCH.",
    ),
    ValmerColumnSpec(
        source_name="fechapreciomaximo",
        column_name="max_price_date",
        dtype="datetime",
        transform="datetime",
        label="Max Price Date",
        description="Date of the maximum price from FECHA PRECIO MAXIMO.",
    ),
    ValmerColumnSpec(
        source_name="fechapreciominimo",
        column_name="min_price_date",
        dtype="datetime",
        transform="datetime",
        label="Min Price Date",
        description="Date of the minimum price from FECHA PRECIO MINIMO.",
    ),
    ValmerColumnSpec(
        source_name="sensibilidad",
        column_name="sensitivity",
        dtype="float",
        transform="float",
        label="Sensitivity",
        description="Sensitivity from SENSIBILIDAD.",
    ),
    ValmerColumnSpec(
        source_name="duracionmacaulay",
        column_name="macaulay_duration",
        dtype="float",
        transform="float",
        label="Macaulay Duration",
        description="Macaulay duration from DURACION MACAULAY.",
    ),
    ValmerColumnSpec(
        source_name="tasaderendimiento",
        column_name="yield_rate",
        dtype="float",
        transform="float",
        label="Yield Rate",
        description="Yield rate from TASA DE RENDIMIENTO.",
    ),
    ValmerColumnSpec(
        source_name="hrratings",
        column_name="hr_rating",
        dtype="string",
        transform="string",
        label="HR Rating",
        description="HR rating from HR RATINGS.",
    ),
)


VALMER_TIMESERIES_SOURCE_COLUMN_SPECS = VALMER_SOURCE_COLUMN_SPECS
VALMER_VECTOR_COLUMN_SPECS = VALMER_DERIVED_COLUMN_SPECS + VALMER_TIMESERIES_SOURCE_COLUMN_SPECS

VALMER_REQUIRED_SOURCE_COLUMNS = tuple(
    dict.fromkeys(
        [
            *[
                spec.source_name
                for spec in VALMER_SOURCE_COLUMN_SPECS
                if spec.source_name is not None
            ],
            *sorted(VALMER_ASSET_DETAIL_SOURCE_COLUMNS),
        ]
    )
)

PERSISTED_VECTOR_TO_SOURCE_COLUMNS = {
    "security_type": "tipovalor",
    "issuer": "emisora",
    "series": "serie",
    "issue_currency": "monedaemision",
    "underlying": "subyacente",
    "issue_date": "fechaemision",
    "adjusted_face_value": "valornominalactualizado",
}


def _prepare_frame_for_target_bond_rules(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        persisted: source
        for persisted, source in PERSISTED_VECTOR_TO_SOURCE_COLUMNS.items()
        if persisted in df.columns and source not in df.columns
    }
    if not rename_map:
        return df.copy()
    return df.rename(columns=rename_map).copy()


class _ProgressLogger:
    def __init__(
        self,
        logger,
        label: str,
        total: int,
        *,
        milestones: tuple[int, ...] = (1, 5, 20, 40, 60, 80, 100),
    ):
        self.logger = logger
        self.label = label
        self.total = total
        self.thresholds = {
            max(1, (total * milestone + 99) // 100)
            for milestone in milestones
        }
        self.completed = 0
        self.logged: set[int] = set()
        if total:
            self.logger.info(f"{label}: starting {total} items.")

    def advance(self, count: int = 1) -> None:
        if not self.total:
            return
        self.completed += count
        for threshold in sorted(self.thresholds):
            if threshold in self.logged or self.completed < threshold:
                continue
            self.logged.add(threshold)
            percent = min(100, round((self.completed / self.total) * 100))
            self.logger.info(
                f"{self.label}: {percent}% complete ({self.completed}/{self.total})."
            )


class ImportValmerConfig(AssetIndexedDataNodeConfiguration):
    bucket_name: str = Field(
        ...,
        description="Valmer artifact bucket used by this updater.",
        examples=["Hitorical Valmer Vector Analytico"],
    )


class ImportValmer(AssetIndexedDataNode):
    def __init__(self, config: ImportValmerConfig, **kwargs):
        """
        Initializes the ImportValmer DataNode.

        Args:
            config: DataNode configuration including the source artifact bucket.
        """
        self.bucket_name = config.bucket_name
        self.artifact_data = None
        self.source_data = None
        self.asset_list = None
        super().__init__(config=config, **kwargs)

    @classmethod
    def _required_storage_table(cls):
        return ValmerVectorPricesStorage

    def maximum_forward_fill(self):
        return timedelta(days=1) - pd.Timedelta("5ms")

    def get_explanation(self):
        explanation = (
            "### Data From Valmer\n\n"
            "This node reads all files from the specified Valmer bucket, "
            "combines them, and processes them in a single operation. "
            "It normalizes all column headers by lowercasing them and removing special characters."
        )
        return explanation

    def _set_artifact_data(self):
        """
        Reads all artifacts from the bucket, normalizes columns, and concatenates them into a single DataFrame.
        Optionally filters for new artifacts based on the 'process_all_files' flag.
        """
        from pathlib import Path

        debug_artifact_path = os.environ.get("DEBUG_ARTIFACT_PATH", None)
        if debug_artifact_path:
            def read_artifact(p: Path) -> pd.DataFrame:
                engine = "xlrd" if p.suffix.lower() == ".xls" else "openpyxl"

                # 1) normal read: keep default NA behavior for ALL other columns
                df = pd.read_excel(
                    p,
                    engine=engine,
                    dtype={"TIPO VALOR": "string", "SERIE": "string"}  # don't include emisora here
                )

                # 2) re-read ONLY emisora, without interpreting "NA" as missing
                emisora = pd.read_excel(
                    p,
                    engine=engine,
                    usecols=["EMISORA"],
                    dtype={"EMISORA": "string"},
                    keep_default_na=False
                )["EMISORA"]

                # If you want blank Excel cells to still be missing (not empty string):
                emisora = emisora.replace("", pd.NA)

                df["EMISORA"] = emisora
                return df

            base = Path(debug_artifact_path)
            if not base.exists():
                raise FileNotFoundError(f"DEBUG_ARTIFACT_PATH does not exist: {base}")
            if base.is_file():
                sorted_artifacts = [read_artifact(base)]
            else:
                sorted_artifacts = [read_artifact(p) for p in sorted(base.rglob("*.xls*"))]
            latest_date = (
                self.local_persist_manager.get_update_statistics_for_table()
                .get_max_time_in_update_statistics()
            )
            source_label = f"local DEBUG_ARTIFACT_PATH '{debug_artifact_path}'"
        else:
            if self.artifact_data is not None:
                return None

            artifacts, artifact_dates = self._get_artifacts(self.logger, self.bucket_name)
            latest_date = (
                self.local_persist_manager.get_update_statistics_for_table()
                .get_max_time_in_update_statistics()
            )
            sorted_artifacts = artifacts
            if latest_date:
                self.logger.info(f"Filtering artifacts newer than {latest_date}.")
                sorted_artifacts = [
                    a for a, a_date in zip(artifacts, artifact_dates) if a_date > latest_date
                ]

            sorted_artifacts = sorted_artifacts[:5]

            self.logger.info(f"Processing {len(sorted_artifacts)} artifacts...")
            if not sorted_artifacts:
                self.logger.info("No new artifacts to process. Task finished.")
                self.artifact_data = pd.DataFrame()
                return None
            source_label = f"Artifact bucket '{self.bucket_name}'"

        frames = self._concatenate_artifacts_content(
            sorted_artifacts,
            self.logger,
            source_label=source_label,
        )

        try:
            self.artifact_data = pd.concat(frames, ignore_index=True, sort=False, copy=False)
        except TypeError:
            self.artifact_data = pd.concat(frames, ignore_index=True, sort=False)

        self.logger.info(
            f"Combined all artifacts into a single DataFrame with {len(self.artifact_data)} rows."
        )

    def dependencies(self) -> dict:
        return {}

    # ------- Helpers for bond and vector filter -------#
    @staticmethod
    def _get_target_bonds(df_latest: pd.DataFrame):
        working = _prepare_frame_for_target_bond_rules(df_latest)
        required_cols = {"tipovalor", "subyacente", "monedaemision", "emisora", "fechaemision"}
        missing = sorted(required_cols - set(working.columns))
        if missing:
            raise KeyError(
                f"ImportValmer target-bond selection requires columns {required_cols}. Missing: {missing}"
            )

        working["tipovalor"] = working["tipovalor"].astype("string")
        working["subyacente"] = working["subyacente"].astype("string")
        working["monedaemision"] = working["monedaemision"].astype("string")
        working["emisora"] = working["emisora"].astype("string")

        floating_tiie = working["subyacente"].str.contains("TIIE", na=False)
        floating_cetes = working["subyacente"].str.contains("CETE", na=False)
        cetes = working["subyacente"].str.contains("Cete", na=False)
        m_bono_fixed_0 = working["subyacente"].str.contains("Bonos M", na=False)
        m_bono_fixed_0 = m_bono_fixed_0 & (working["monedaemision"] == "MPS")

        bondes_d = working["subyacente"].str.contains("Fondeo Bancario", na=False)
        bondes_f_g = working["subyacente"].str.contains("Tasa TIIE Fondeo 1D", na=False)

        zero_corps = working["tipovalor"].isin(["I", "93", "92"])
        zero_corps = zero_corps & working["monedaemision"].isin(["MPS"])

        bpas = working["emisora"].isin(["BPAG91", "BPAG28", "BPA182"])
        bpas = bpas & working["monedaemision"].isin(["MPS"])

        target_mask = (
            floating_tiie
            | floating_cetes
            | cetes
            | m_bono_fixed_0
            | bondes_d
            | bondes_f_g
            | zero_corps
            | bpas
        )

        all_target_bonds = df_latest.loc[target_mask].copy()
        all_target_bonds = all_target_bonds.loc[working.loc[target_mask, "fechaemision"].notna()]

        log = structlog.get_logger("mainsequence")
        log.info("Only adding price details for")

        return all_target_bonds

    @staticmethod
    def _get_missing_asset_uids(
        unique_identifiers: List[str],
        existing_assets: dict[str, MarketsAsset],
    ) -> List[str]:
        return [u for u in unique_identifiers if u not in existing_assets]

    @staticmethod
    def _get_current_pricing_face_values_by_uid(
        existing_assets: dict[str, MarketsAsset],
        *,
        batch_size: int,
        logger,
    ) -> dict[str, object]:
        if not existing_assets:
            return {}

        asset_uid_to_valmer_uid = {str(asset.uid): uid for uid, asset in existing_assets.items()}
        asset_uids = [asset.uid for asset in existing_assets.values()]
        context = AssetCurrentPricingDetails._active_context()
        batches = [
            asset_uids[start : start + batch_size]
            for start in range(0, len(asset_uids), batch_size)
        ]
        logger.info(
            "Resolving current pricing face values for "
            f"{len(asset_uids)} target assets in {len(batches)} batches "
            f"of up to {batch_size}."
        )
        progress = _ProgressLogger(
            logger,
            "Resolving current pricing face values",
            len(asset_uids),
        )

        face_values: dict[str, object] = {}
        rows_found = 0
        for batch in batches:
            statement = (
                select(
                    AssetCurrentPricingDetails.__table__.asset_uid.label("asset_uid"),
                    AssetCurrentPricingDetails.__table__.instrument_dump.label(
                        "instrument_dump"
                    ),
                )
                .where(AssetCurrentPricingDetails.__table__.asset_uid.in_(batch))
                .limit(len(batch))
            )
            operation = compile_markets_statement(
                statement,
                context=context,
                operation="select",
                models=[AssetCurrentPricingDetails.__table__],
                access="read",
            )
            result = execute_markets_operation(operation, context=context)
            for row in operation_result_rows(result):
                asset_uid = row.get("asset_uid")
                valmer_uid = asset_uid_to_valmer_uid.get(str(asset_uid))
                if valmer_uid is not None:
                    face_values[valmer_uid] = _pricing_detail_face_value(
                        row.get("instrument_dump")
                    )
                    rows_found += 1
            progress.advance(len(batch))
        logger.info(
            "Resolved current pricing face values: "
            f"{rows_found} existing pricing-detail rows, "
            f"{len(asset_uids) - rows_found} missing rows."
        )
        return face_values

    @staticmethod
    def _get_pricing_refresh_uids(
        unique_identifiers: List[str],
        existing_assets: dict[str, MarketsAsset],
        current_pricing_face_values: dict[str, object],
        all_target_bonds: pd.DataFrame,
        *,
        force_update: bool = False,
        logger=None,
    ) -> List[str]:
        """
        Decide which target UIDs need a pricing-detail refresh.
        """
        if all_target_bonds.empty:
            return []

        target_rows = all_target_bonds.drop_duplicates("unique_identifier", keep="last").set_index(
            "unique_identifier"
        )
        target_uids = set(target_rows.index)

        pricing_updates: List[str] = []
        missing_assets = 0
        missing_pricing_details = 0
        missing_face_values = 0
        changed_face_values = 0
        forced_refreshes = 0

        for u in unique_identifiers:
            if u not in target_uids:
                continue
            asset = existing_assets.get(u)

            if asset is None:
                missing_assets += 1
                pricing_updates.append(u)
                continue

            if force_update:
                forced_refreshes += 1
                pricing_updates.append(u)
                continue

            if u not in current_pricing_face_values:
                missing_pricing_details += 1
                pricing_updates.append(u)
                continue

            old_face_value = current_pricing_face_values.get(u)
            if old_face_value is None:
                missing_face_values += 1
                pricing_updates.append(u)
                continue

            # Compare against latest nominal value in targets
            row = target_rows.loc[u]
            new_face_value = row.get("valornominalactualizado", row.get("adjusted_face_value"))
            if old_face_value is None or old_face_value != new_face_value:
                changed_face_values += 1
                pricing_updates.append(u)

        # Deduplicate while preserving order
        def _dedup(seq: List[str]) -> List[str]:
            return list(dict.fromkeys(seq))

        deduped = _dedup(pricing_updates)
        if logger is not None:
            logger.info(
                "Pricing refresh decision: "
                f"{len(target_uids)} target assets, "
                f"{missing_assets} missing assets, "
                f"{missing_pricing_details} missing pricing details, "
                f"{missing_face_values} missing face values, "
                f"{changed_face_values} changed face values, "
                f"{forced_refreshes} forced refreshes, "
                f"{len(deduped)} refreshes."
            )
        return deduped

    @staticmethod
    def _get_artifacts(logger, bucket_name):
        artifacts = Artifact.filter(
            bucket__name=bucket_name,
        )
        sorted_artifacts = sorted(artifacts, key=lambda artifact: artifact.name)

        logger.info(f"Found {len(sorted_artifacts)} artifacts in bucket '{bucket_name}'.")

        # --- Conditional processing based on process_all_files flag ---
        artifact_dates = []
        for artifact in sorted_artifacts:
            match = re.search(r"(\d{4}-\d{2}-\d{2})", artifact.name)
            if match:
                artifact_dates.append(pd.to_datetime(match.group(1), utc=True))
            else:
                continue

        return sorted_artifacts, artifact_dates

    @staticmethod
    def _concatenate_artifacts_content(sorted_artifacts, logger, *, source_label: str | None = None):
        frames = []
        for artifact in tqdm(sorted_artifacts):
            if isinstance(artifact, Artifact):
                artifact_name = artifact.name
                name_l = artifact.name.lower()
                content = artifact.content
                buf = content

                df = None
                if name_l.endswith(".xls"):
                    import xlrd  # noqa: F401

                    df = pd.read_excel(buf, engine="xlrd")
                elif name_l.endswith(".csv"):
                    try:
                        df = pd.read_csv(buf, encoding="latin1", engine="pyarrow")
                    except Exception:
                        df = pd.read_csv(buf, encoding="latin1", low_memory=False)
                else:
                    logger.info(f"Skipping unsupported file type: {artifact.name}")
                    continue

                if df is None or df.empty:
                    continue
            else:
                df = artifact
                artifact_name = source_label or "local dataframe"

            # Normalize all column names
            df.columns = [normalize_column_name(col) for col in df.columns]

            # Check for required columns for instrument identifier
            required_cols = {"tipovalor", "emisora", "serie"}
            if required_cols.issubset(df.columns):
                df = add_valmer_unique_identifier(df)
            else:
                logger.warning(
                    f"Skipping unique_identifier creation for {artifact_name} due to missing columns."
                )
                continue

            frames.append(df)

        if not frames:
            source = source_label or "provided artifacts"
            raise ValueError(f"No valid data frames could be created from {source}.")
        return frames

    @staticmethod
    def _pick_latest_artifact(artifacts, logger):
        """Pick the single latest artifact by YYYY-MM-DD in its name; fallback to last by name."""
        import re

        if not artifacts:
            return None

        def _parse_dt(a):
            m = re.search(r"(\d{4}-\d{2}-\d{2})", a.name)
            return pd.to_datetime(m.group(1), utc=True) if m else pd.NaT

        dated = [(a, _parse_dt(a)) for a in artifacts]
        dated = [(a, d) for a, d in dated if pd.notna(d)]
        if dated:
            max_date = max(d for _, d in dated)
            candidates = [a for a, d in dated if d == max_date]
            selected = sorted(candidates, key=lambda x: x.name)[-1]
            logger.info(f"Selected latest artifact: {selected.name} ({max_date.date()})")
            return selected

        logger.warning("No parsable dates in artifact names; falling back to last by name.")
        return sorted(artifacts, key=lambda a: a.name)[-1]

    def _sync_asset_registry_and_pricing(
        self,
        unique_identifiers: List[str],
        df_latest: pd.DataFrame,
        all_target_bonds: pd.DataFrame,
        *,
        force_update: bool = False,
    ) -> list:
        """Sync Valmer asset rows, Valmer asset details, and current pricing details."""
        per_page_assets = resolve_valmer_meta_operation_batch_size()
        target_uids = set(all_target_bonds["unique_identifier"].unique())
        self.logger.info(
            "Starting Valmer asset registry and pricing sync: "
            f"{len(unique_identifiers)} source assets, "
            f"{len(target_uids)} target pricing assets, "
            f"batch size {per_page_assets}."
        )
        existing_asset_refs = resolve_valmer_asset_refs(
            unique_identifiers,
            batch_size=per_page_assets,
            logger=self.logger,
        )
        existing_assets = {
            uid: asset_ref.as_asset()
            for uid, asset_ref in existing_asset_refs.items()
        }
        assets_needing_type_update = [
            uid
            for uid, asset_ref in existing_asset_refs.items()
            if asset_ref.asset_type != ASSET_TYPE_BOND
        ]
        self.logger.info(
            "Valmer asset registry decision: "
            f"{len(existing_assets)} existing, "
            f"{len(unique_identifiers) - len(existing_assets)} missing, "
            f"{len(assets_needing_type_update)} wrong asset_type."
        )
        existing_target_assets = {
            uid: asset for uid, asset in existing_assets.items() if uid in target_uids
        }
        current_pricing_face_values = self._get_current_pricing_face_values_by_uid(
            existing_target_assets,
            batch_size=per_page_assets,
            logger=self.logger,
        )
        missing_assets = self._get_missing_asset_uids(unique_identifiers, existing_assets)
        pricing_updates = self._get_pricing_refresh_uids(
            unique_identifiers,
            existing_assets,
            current_pricing_face_values,
            all_target_bonds,
            force_update=force_update,
            logger=self.logger,
        )

        df_latest_idx = df_latest.drop_duplicates("unique_identifier", keep="last").set_index(
            "unique_identifier"
        )

        assets_to_upsert = list(dict.fromkeys([*missing_assets, *assets_needing_type_update]))
        if assets_to_upsert:
            self.logger.info(
                f"Upserting {len(assets_to_upsert)} Valmer assets as {ASSET_TYPE_BOND!r}."
            )
            upserted_assets = upsert_valmer_assets(
                assets_to_upsert,
                batch_size=per_page_assets,
                logger=self.logger,
            )
            existing_assets.update(upserted_assets)

        detail_rows = upsert_valmer_asset_details(
            df_latest,
            existing_assets,
            logger=self.logger,
        )
        self.logger.info(f"Upserted {len(detail_rows)} Valmer asset detail rows.")

        # --- decide pricing recipients ---
        uids_needing_pricing = set(pricing_updates)
        uids_needing_pricing.update(u for u in missing_assets if u in target_uids)

        if uids_needing_pricing:
            instrument_pricing_detail_map: dict[str, dict] = {}
            pricing_uid_list = list(uids_needing_pricing)
            instrument_progress = _ProgressLogger(
                self.logger,
                "Building Valmer pricing instruments",
                len(pricing_uid_list),
            )
            for uid in pricing_uid_list:
                if uid not in df_latest_idx.index:
                    instrument_progress.advance()
                    continue
                row = df_latest_idx.loc[uid]

                icalendar, business_day_convention, settlement_days, day_count = (
                    get_instrument_conventions(row)
                )
                ql_bond = build_qll_bond_from_row(
                    row=row,
                    calendar=icalendar,
                    dc=day_count,
                    bdc=business_day_convention,
                    settlement_days=settlement_days,
                )
                instrument_pricing_detail_map[uid] = {
                    "instrument": ql_bond,
                    "pricing_details_date": row["fecha"],
                }
                instrument_progress.advance()

            # target the correct asset objects (newly registered + existing)
            assets_for_update: dict[str, MarketsAsset] = {
                u: existing_assets[u]
                for u in uids_needing_pricing
                if u in existing_assets and u in instrument_pricing_detail_map
            }

            persist_progress = _ProgressLogger(
                self.logger,
                "Persisting current pricing details",
                len(assets_for_update),
            )
            for uid, asset in assets_for_update.items():
                try:
                    pricing_details = instrument_pricing_detail_map[uid]
                    persist_current_pricing_details(
                        asset=asset,
                        instrument=pricing_details["instrument"],
                        pricing_details_date=pricing_details["pricing_details_date"],
                        source="valmer",
                        metadata_json={"valmer_unique_identifier": uid},
                    )
                except Exception as e:
                    self.logger.error(f"Failed to update pricing details for {uid}: {e}")
                finally:
                    persist_progress.advance()

        return list(existing_assets.values())

    def update_pricing_details_from_last_vector(self, force_update=False):
        artifacts, artifact_dates = self._get_artifacts(self.logger, self.bucket_name)
        if not artifacts:
            self.logger.info("No artifacts to process.")
            return []

        last_artifact = self._pick_latest_artifact(artifacts, self.logger)
        if last_artifact is None:
            self.logger.info("No latest artifact could be selected.")
            return []
        source_df_list = self._concatenate_artifacts_content([last_artifact], self.logger)
        if not source_df_list:
            self.logger.info("Latest artifact produced no usable rows.")
            return []
        source_df = source_df_list[0]
        df_latest, all_target_bonds, unique_identifiers = self._prepare_latest_inputs(source_df)
        self.logger.info(f"[last vector] Found {len(unique_identifiers)} unique assets to process.")
        return self._sync_asset_registry_and_pricing(
            unique_identifiers, df_latest, all_target_bonds, force_update=force_update
        )

    def _prepare_latest_inputs(self, df: pd.DataFrame):
        """Common prep: normalize, latest rows per UID, target bonds, and universe of UIDs."""
        df = df[df["unique_identifier"].notna()].copy()
        df["fecha"] = pd.to_datetime(df["fecha"], format="%Y%m%d", utc=True)

        idx = df.groupby("unique_identifier")["fecha"].idxmax()
        df_latest = df.loc[idx].reset_index(drop=True)

        all_target_bonds = self._get_target_bonds(df_latest)

        unique_identifiers = df["unique_identifier"].unique().tolist()
        return df_latest, all_target_bonds, unique_identifiers

    def prepare_source_data(self) -> pd.DataFrame:
        """Load Valmer artifact rows for the current DataNode update."""
        self._set_artifact_data()
        self.source_data = self.artifact_data
        if self.source_data is None:
            self.source_data = pd.DataFrame()
        return self.source_data

    def prepare_for_update(self, *, force_pricing_update: bool = False) -> list:
        """
        Prepare the Valmer update explicitly before the DataNode run.

        This registers AssetTable rows, upserts static Valmer asset details,
        hydrates current pricing details for target bonds, and stores the
        resulting AssetTable scope for get_asset_list().
        """
        source_data = self.prepare_source_data()
        if self.source_data.empty:
            self.asset_list = None
            return []

        df_latest, all_target_bonds, unique_identifiers = self._prepare_latest_inputs(source_data)
        self.logger.info(f"Found {len(unique_identifiers)} unique assets to process.")
        self.asset_list = self._sync_asset_registry_and_pricing(
            unique_identifiers,
            df_latest,
            all_target_bonds,
            force_update=force_pricing_update,
        )
        return self.asset_list

    def get_asset_list(self) -> Union[None, list]:
        """Return the already-prepared asset scope for this DataNode run."""
        return super().get_asset_list()

    def update(self):
        source_data = self.source_data
        assert source_data is not None, "Source data is not available"

        if source_data.empty:
            return pd.DataFrame()

        missing_columns = sorted(set(VALMER_REQUIRED_SOURCE_COLUMNS) - set(source_data.columns))
        if missing_columns:
            raise KeyError(
                "ImportValmer requires the full translated Valmer source schema. "
                f"Missing normalized columns: {missing_columns}"
            )

        vector_df = pd.DataFrame(index=source_data.index)
        for spec in VALMER_TIMESERIES_SOURCE_COLUMN_SPECS:
            assert spec.source_name is not None
            vector_df[spec.column_name] = _coerce_valmer_series(
                source_data[spec.source_name], spec.transform
            )

        valuation_date = vector_df["valuation_date"]
        dirty_price = vector_df["dirty_price"]
        time_index = _as_utc_ns(valuation_date + pd.Timedelta(days=1) - pd.Timedelta(seconds=1))

        vector_df["time_index"] = time_index
        vector_df[ASSET_IDENTIFIER_DIMENSION] = source_data["unique_identifier"].astype("string")
        vector_df["open"] = dirty_price
        vector_df["high"] = dirty_price
        vector_df["low"] = dirty_price
        vector_df["close"] = dirty_price
        vector_df["volume"] = pd.Series(0, index=source_data.index, dtype="Int64")
        vector_df["open_time"] = _build_open_time_series(time_index)

        ordered_columns = [spec.column_name for spec in VALMER_VECTOR_COLUMN_SPECS]
        vector_df = vector_df[["time_index", ASSET_IDENTIFIER_DIMENSION, *ordered_columns]]
        vector_df.set_index(["time_index", ASSET_IDENTIFIER_DIMENSION], inplace=True)
        vector_df = self.update_statistics.filter_df_by_latest_value(vector_df)
        integer_columns = [
            spec.column_name for spec in VALMER_VECTOR_COLUMN_SPECS if spec.dtype == "int"
        ]
        vector_df = _normalize_nullable_integer_columns(vector_df, integer_columns)

        return vector_df
