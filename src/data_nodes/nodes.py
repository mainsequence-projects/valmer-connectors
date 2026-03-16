import io
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Tuple, Union

import numpy as np
import pandas as pd
import pytz
import requests
import structlog
from tqdm import tqdm

import mainsequence.client as msc
from mainsequence.client.models_tdag import Artifact
from mainsequence.tdag import DataNode
from src.instruments.vector_to_asset import (
    build_qll_bond_from_row,
    get_instrument_conventions,
    normalize_column_name,
)

UTC = pytz.UTC
import base64
import gzip
import json
import os

import QuantLib as ql


@dataclass(frozen=True)
class ValmerColumnSpec:
    source_name: str | None
    column_name: str
    dtype: str
    transform: str
    label: str
    description: str


def _build_column_metadata(
    specs: Iterable[ValmerColumnSpec],
) -> list[msc.ColumnMetaData]:
    return [
        msc.ColumnMetaData(
            column_name=spec.column_name,
            dtype=spec.dtype,
            label=spec.label,
            description=spec.description,
        )
        for spec in specs
    ]


def _coerce_valmer_series(series: pd.Series, transform: str) -> pd.Series:
    if transform == "string":
        return series.astype("string")

    if transform == "float":
        return pd.to_numeric(series, errors="coerce")

    if transform == "int":
        return pd.to_numeric(series, errors="coerce").astype("Int64")

    if transform == "percent":
        cleaned = (
            series.astype("string")
            .str.replace("%", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        return pd.to_numeric(cleaned, errors="coerce")

    if transform == "datetime":
        return pd.to_datetime(series, errors="coerce", utc=True)

    if transform == "date_ymd":
        return pd.to_datetime(series.astype("string"), format="%Y%m%d", errors="coerce", utc=True)

    raise ValueError(f"Unsupported Valmer transform: {transform}")


def _build_open_time_series(time_index: pd.Series) -> pd.Series:
    open_time = pd.Series(pd.NA, index=time_index.index, dtype="Int64")
    valid = time_index.notna()
    if valid.any():
        open_time.loc[valid] = (time_index.loc[valid].astype("int64") // 10**9).astype("Int64")
    return open_time


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
        source_name="tipovalor",
        column_name="security_type",
        dtype="string",
        transform="string",
        label="Security Type",
        description="Security type code from TIPO VALOR.",
    ),
    ValmerColumnSpec(
        source_name="emisora",
        column_name="issuer",
        dtype="string",
        transform="string",
        label="Issuer",
        description="Issuer code from EMISORA.",
    ),
    ValmerColumnSpec(
        source_name="serie",
        column_name="series",
        dtype="string",
        transform="string",
        label="Series",
        description="Series code from SERIE.",
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
        source_name="nombrecompleto",
        column_name="full_name",
        dtype="string",
        transform="string",
        label="Full Name",
        description="Full instrument name from NOMBRE COMPLETO.",
    ),
    ValmerColumnSpec(
        source_name="sector",
        column_name="sector",
        dtype="string",
        transform="string",
        label="Sector",
        description="Sector from SECTOR.",
    ),
    ValmerColumnSpec(
        source_name="montoemitido",
        column_name="issued_amount",
        dtype="float",
        transform="float",
        label="Issued Amount",
        description="Issued amount from MONTO EMITIDO.",
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
        source_name="fechaemision",
        column_name="issue_date",
        dtype="datetime",
        transform="datetime",
        label="Issue Date",
        description="Issue date from FECHA EMISION.",
    ),
    ValmerColumnSpec(
        source_name="plazoemision",
        column_name="issue_term",
        dtype="float",
        transform="float",
        label="Issue Term",
        description="Issue term from PLAZO EMISION.",
    ),
    ValmerColumnSpec(
        source_name="fechavcto",
        column_name="maturity_date",
        dtype="datetime",
        transform="datetime",
        label="Maturity Date",
        description="Maturity date from FECHA VCTO.",
    ),
    ValmerColumnSpec(
        source_name="valornominal",
        column_name="face_value",
        dtype="float",
        transform="float",
        label="Face Value",
        description="Face value from VALOR NOMINAL.",
    ),
    ValmerColumnSpec(
        source_name="monedaemision",
        column_name="issue_currency",
        dtype="string",
        transform="string",
        label="Issue Currency",
        description="Issue currency from MONEDA EMISION.",
    ),
    ValmerColumnSpec(
        source_name="subyacente",
        column_name="underlying",
        dtype="string",
        transform="string",
        label="Underlying",
        description="Underlying reference from SUBYACENTE.",
    ),
    ValmerColumnSpec(
        source_name="rendcolocacion",
        column_name="placement_yield",
        dtype="float",
        transform="float",
        label="Placement Yield",
        description="Placement yield from REND. COLOCACION.",
    ),
    ValmerColumnSpec(
        source_name="stcolocacion",
        column_name="placement_spread",
        dtype="float",
        transform="float",
        label="Placement Spread",
        description="Placement spread from STCOLOCACION.",
    ),
    ValmerColumnSpec(
        source_name="freccpn",
        column_name="coupon_frequency",
        dtype="string",
        transform="string",
        label="Coupon Frequency",
        description="Coupon frequency from FREC. CPN.",
    ),
    ValmerColumnSpec(
        source_name="tasacupon",
        column_name="coupon_rate",
        dtype="float",
        transform="float",
        label="Coupon Rate",
        description="Coupon rate from TASA CUPON.",
    ),
    ValmerColumnSpec(
        source_name="diastransccpn",
        column_name="days_since_coupon",
        dtype="int",
        transform="int",
        label="Days Since Coupon",
        description="Days since coupon from DIAS TRANSC. CPN.",
    ),
    ValmerColumnSpec(
        source_name="reglacupon",
        column_name="coupon_rule",
        dtype="string",
        transform="string",
        label="Coupon Rule",
        description="Coupon rule from REGLA CUPON.",
    ),
    ValmerColumnSpec(
        source_name="cuponesemision",
        column_name="coupons_at_issue",
        dtype="int",
        transform="int",
        label="Coupons At Issue",
        description="Coupon count at issuance from CUPONES EMISION.",
    ),
    ValmerColumnSpec(
        source_name="cuponesxcobrar",
        column_name="coupons_remaining",
        dtype="int",
        transform="int",
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


VALMER_VECTOR_COLUMN_SPECS = VALMER_DERIVED_COLUMN_SPECS + VALMER_SOURCE_COLUMN_SPECS

VALMER_REQUIRED_SOURCE_COLUMNS = tuple(
    spec.source_name for spec in VALMER_SOURCE_COLUMN_SPECS if spec.source_name is not None
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


class MexDerTIIE28Zero(DataNode):
    """Download and return daily MEXDERSWAP_IRSTIIEPR swap rates from valmer.com.mx

    Output:
        - Index: DatetimeIndex named 'time_index' (UTC)
        - Columns: cleaned from the CSV (lowercase, <=63 chars, no datetime columns)
    """

    @staticmethod
    def compress_curve_to_string(curve_dict: Dict[Any, Any]) -> str:
        """
        Serializes, compresses, and encodes a curve dictionary into a single,
        transport-safe text string.

        Pipeline: Dict -> JSON -> Gzip (binary) -> Base64 (text)

        Args:
            curve_dict: The Python dictionary representing the curve.

        Returns:
            A Base64-encoded string of the Gzipped JSON.
        """
        # 1. Serialize the dictionary to a compact JSON string, then encode to bytes
        json_bytes = json.dumps(curve_dict, separators=(",", ":")).encode("utf-8")

        # 2. Compress the JSON bytes using the universal Gzip standard
        compressed_bytes = gzip.compress(json_bytes)

        # 3. Encode the compressed binary data into a text-safe Base64 string
        base64_bytes = base64.b64encode(compressed_bytes)

        # 4. Decode the Base64 bytes into a final ASCII string for storage/transport
        return base64_bytes.decode("ascii")

    @staticmethod
    def decompress_string_to_curve(b64_string: str) -> Dict[Any, Any]:
        """
        Decodes, decompresses, and deserializes a string back into a curve dictionary.

        Pipeline: Base64 (text) -> Gzip (binary) -> JSON -> Dict

        Args:
            b64_string: The Base64-encoded string from the database or API.

        Returns:
            The reconstructed Python dictionary.
        """
        # 1. Encode the ASCII string back into Base64 bytes
        base64_bytes = b64_string.encode("ascii")

        # 2. Decode the Base64 to get the compressed Gzip bytes
        compressed_bytes = base64.b64decode(base64_bytes)

        # 3. Decompress the Gzip bytes to get the original JSON bytes
        json_bytes = gzip.decompress(compressed_bytes)

        # 4. Decode the JSON bytes to a string and parse back into a dictionary
        return json.loads(json_bytes.decode("utf-8"))

    def dependencies(self):
        return {}

    def get_asset_list(self):
        tiie_asset = msc.Asset.get(unique_identifier="TIIE_28")
        self.tiie_asset = tiie_asset
        return [tiie_asset]

    def update(self):
        # Download CSV from source
        url = "https://valmer.com.mx/VAL/Web_Benchmarks/MEXDERSWAP_IRSTIIEPR.csv"
        response = requests.get(url)
        response.raise_for_status()

        # Load CSV directly from bytes, using correct encoding
        names = ["id", "curve_name", "asof_yyMMdd", "idx", "zero_rate"]
        # STRICT: comma-separated, headerless, exactly these six columns
        df = pd.read_csv(
            io.BytesIO(response.content),
            header=None,
            names=names,
            sep=",",
            engine="c",
            encoding="latin1",
            dtype=str,
        )
        # pick a rate column

        df["asof_yyMMdd"] = pd.to_datetime(df["asof_yyMMdd"], format="%y%m%d")
        df["asof_yyMMdd"] = df["asof_yyMMdd"].dt.tz_localize("UTC")

        base_dt = df["asof_yyMMdd"].iloc[0] - timedelta(days=1)

        if (
            self.update_statistics.asset_time_statistics[self.tiie_asset.unique_identifier]
            >= base_dt
        ):
            return pd.DataFrame()

        df["idx"] = df["idx"].astype(int)
        df["days_to_maturity"] = (df["asof_yyMMdd"] - base_dt).dt.days
        df["zero_rate"] = df["zero_rate"].astype(float) / 100

        df["time_index"] = base_dt
        df["unique_identifier"] = self.tiie_asset.unique_identifier

        grouped = (
            df.groupby(["time_index", "unique_identifier"])
            .apply(lambda g: g.set_index("days_to_maturity")["zero_rate"].to_dict())
            .rename("curve")
            .reset_index()
        )

        #    Apply the new compression and encoding function to the 'curve' column.
        grouped["curve"] = grouped["curve"].apply(self.compress_curve_to_string)

        # 3. Final index and structure (your original code)
        grouped = grouped.set_index(["time_index", "unique_identifier"])

        return grouped

    def get_table_metadata(self) -> msc.TableMetaData:
        return msc.TableMetaData(
            identifier="valmer_mexder_tiie28_zero_curve",
            data_frequency_id=msc.DataFrequency.one_d,
            description="Benchmark swap rates (MEXDERSWAP_IRSTIIEPR) from Valmer (valmer.com.mx)",
        )

    def get_column_metadata(self) -> list[msc.ColumnMetaData]:
        return _build_column_metadata(
            (
                ValmerColumnSpec(
                    source_name=None,
                    column_name="curve",
                    dtype="string",
                    transform="string",
                    label="Curve Payload",
                    description=(
                        "Base64-encoded gzip payload containing the zero-curve dictionary."
                    ),
                ),
            )
        )


class ImportValmer(DataNode):
    def __init__(self, bucket_name: str, *args, **kwargs):
        """
        Initializes the ImportValmer DataNode.

        Args:
            bucket_name (str): The name of the bucket containing the source files.
        """
        self.bucket_name = bucket_name
        self.artifact_data = None
        super().__init__(*args, **kwargs)

    _ARGS_IGNORE_IN_STORAGE_HASH = ["bucket_name"]

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
        import os
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
            sorted_artifacts = [read_artifact(p) for p in sorted(base.rglob("*.xls*"))]
            latest_date = self.local_persist_manager.get_update_statistics_for_table().get_max_time_in_update_statistics()
        else:
            if self.artifact_data is not None:
                return None

            artifacts, artifact_dates = self._get_artifacts(self.logger, self.bucket_name)
            latest_date = self.local_persist_manager.get_update_statistics_for_table().get_max_time_in_update_statistics()
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

        frames = self._concatenate_artifacts_content(sorted_artifacts, self.logger)

        try:
            self.artifact_data = pd.concat(frames, ignore_index=True, sort=False, copy=False)
        except TypeError:
            self.artifact_data = pd.concat(frames, ignore_index=True, sort=False)

        self.logger.info(
            f"Combined all artifacts into a single DataFrame with {len(self.artifact_data)} rows."
        )


    def dependencies(self) -> Dict[str, Union["DataNode", "APIDataNode"]]:
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
    def _get_uids_to_update(
        unique_identifiers: List[str],
        existing_assets: Dict[str, "msc.Asset"],
        all_target_bonds: pd.DataFrame,
        *,
        force_update: bool = False,
    ) -> Tuple[List[str], List[str]]:
        """
        Decide which UIDs need (a) asset registration and/or (b) pricing-detail update.

        Returns:
            missing_assets: list[str]   -> assets not in existing_assets (register )
            pricing_updates: list[str]  -> assets (existing or newly-created) that need pricing-detail update

        Behavior:
            - Pricing updates are only considered for *target bonds* (present in all_target_bonds).
            - If force_update=True, every existing target bond goes to pricing_updates.
        """
        if all_target_bonds.empty:
            return [], []

        target_rows = all_target_bonds.drop_duplicates("unique_identifier", keep="last").set_index(
            "unique_identifier"
        )
        target_uids = set(target_rows.index)

        missing_assets: List[str] = []
        pricing_updates: List[str] = []

        # If you're only updating pricing, limit the iteration to target UIDs
        candidates = unique_identifiers

        for u in candidates:
            in_targets = u in target_uids
            asset = existing_assets.get(u)

            if asset is None:
                missing_assets.append(u)
                # Newly-created assets also need pricing details *if* they are target bonds.
                if in_targets:
                    pricing_updates.append(u)

                continue

            # Existing asset
            if not in_targets:
                # Not a target bond => no pricing update requested.
                continue

            if force_update:
                pricing_updates.append(u)
                continue

            cpd = getattr(asset, "current_pricing_detail", None)
            if not cpd or getattr(cpd, "instrument_dump", None) is None:
                pricing_updates.append(u)
                continue

            old_face_value = None
            try:
                old_face_value = cpd.instrument_dump.get("instrument", {}).get("face_value")
            except Exception:
                old_face_value = None

            # Compare against latest nominal value in targets
            row = target_rows.loc[u]
            new_face_value = row.get("valornominalactualizado", row.get("adjusted_face_value"))
            if old_face_value is None or old_face_value != new_face_value:
                pricing_updates.append(u)

        # Deduplicate while preserving order
        def _dedup(seq: List[str]) -> List[str]:
            return list(dict.fromkeys(seq))

        return _dedup(missing_assets), _dedup(pricing_updates)

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
    def _concatenate_artifacts_content(sorted_artifacts, logger):
        frames = []
        for artifact in tqdm(sorted_artifacts):
            if isinstance(artifact, msc.Artifact):
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

            # Normalize all column names
            df.columns = [normalize_column_name(col) for col in df.columns]

            # Check for required columns for instrument identifier
            required_cols = {"tipovalor", "emisora", "serie"}
            if required_cols.issubset(df.columns):
                # Build unique_identifier while keeping all other columns
                df["unique_identifier"] = (
                    df["tipovalor"]
                    .astype("string")
                    .str.cat(df["emisora"].astype("string"), sep="_")
                    .str.cat(df["serie"].astype("string"), sep="_")
                )
            else:
                logger.warning(
                    f"Skipping unique_identifier creation for {artifact.name} due to missing columns."
                )
                continue

            frames.append(df)

        if not frames:
            raise ValueError(f"No valid data frames could be created from files in bucket .")
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

    def _register_and_update_pricing(
        self,
        unique_identifiers: List[str],
        df_latest: pd.DataFrame,
        all_target_bonds: pd.DataFrame,
        *,
        force_update: bool = False,
    ) -> list:
        """One orchestrator for both paths (full vector or last vector)."""
        import os

        # pull existing assets once
        per_page_assets = int(os.environ.get("VALMER_PER_PAGE", 5000))
        existing_assets_list = msc.Asset.query(
            unique_identifier__in=unique_identifiers, per_page=per_page_assets
        )
        existing_assets = {a.unique_identifier: a for a in existing_assets_list}

        missing_assets, pricing_updates = self._get_uids_to_update(
            unique_identifiers, existing_assets, all_target_bonds, force_update=force_update
        )

        df_latest_idx = df_latest.drop_duplicates("unique_identifier", keep="last").set_index(
            "unique_identifier"
        )
        target_uids = set(all_target_bonds["unique_identifier"].unique())

        # --- register missing assets ---
        registered_assets = []
        newly_registered_map: Dict[str, "msc.Asset"] = {}
        if missing_assets:
            for i in range(0, len(missing_assets), per_page_assets):
                batch = missing_assets[i : i + per_page_assets]
                assets_payload = [
                    {"unique_identifier": uid, "snapshot": {"name": uid, "ticker": uid}}
                    for uid in batch
                ]
                self.logger.info(
                    f"Getting or registering assets in batch {i // per_page_assets + 1}/"
                    f"{(len(missing_assets) + per_page_assets - 1) // per_page_assets}..."
                )
                try:
                    assets = msc.Asset.batch_get_or_register_custom_assets(assets_payload)
                    registered_assets.extend(assets)
                    newly_registered_map.update({a.unique_identifier: a for a in assets})
                except Exception as e:
                    self.logger.error(f"Failed to process asset batch: {e}")
                    raise

        # --- decide pricing recipients ---
        uids_needing_pricing = set(pricing_updates)
        uids_needing_pricing.update(u for u in missing_assets if u in target_uids)

        if uids_needing_pricing:
            instrument_pricing_detail_map: Dict[str, dict] = {}
            for uid in uids_needing_pricing:
                if uid not in df_latest_idx.index:
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

            # target the correct asset objects (newly registered + existing)
            assets_for_update: Dict[str, "msc.Asset"] = {}
            assets_for_update.update(
                {
                    u: newly_registered_map[u]
                    for u in newly_registered_map.keys()
                    if u in instrument_pricing_detail_map
                }
            )
            assets_for_update.update(
                {
                    u: existing_assets[u]
                    for u in pricing_updates
                    if u in existing_assets and u in instrument_pricing_detail_map
                }
            )

            for uid, asset in assets_for_update.items():
                try:
                    asset.add_instrument_pricing_details_from_ms_instrument(
                        **instrument_pricing_detail_map[uid]
                    )
                except Exception as e:
                    self.logger.error(f"Failed to update pricing details for {uid}: {e}")

        return registered_assets + list(existing_assets.values())

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
        return self._register_and_update_pricing(
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

    def get_asset_list(self) -> Union[None, list]:
        """
        Processes and registers each unique asset only once from the combined DataFrame.
        """
        self._set_artifact_data()
        self.source_data = self.artifact_data
        if self.source_data.empty:
            return []
        df_latest, all_target_bonds, unique_identifiers = self._prepare_latest_inputs(
            self.source_data
        )
        self.logger.info(f"Found {len(unique_identifiers)} unique assets to process.")
        return self._register_and_update_pricing(
            unique_identifiers, df_latest, all_target_bonds, force_update=False
        )

    def get_column_metadata(self) -> list[msc.ColumnMetaData]:
        return _build_column_metadata(VALMER_VECTOR_COLUMN_SPECS)

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
        for spec in VALMER_SOURCE_COLUMN_SPECS:
            assert spec.source_name is not None
            vector_df[spec.column_name] = _coerce_valmer_series(
                source_data[spec.source_name], spec.transform
            )

        valuation_date = vector_df["valuation_date"]
        dirty_price = vector_df["dirty_price"]
        time_index = valuation_date + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

        vector_df["time_index"] = time_index
        vector_df["unique_identifier"] = source_data["unique_identifier"].astype("string")
        vector_df["open"] = dirty_price
        vector_df["high"] = dirty_price
        vector_df["low"] = dirty_price
        vector_df["close"] = dirty_price
        vector_df["volume"] = pd.Series(0, index=source_data.index, dtype="Int64")
        vector_df["open_time"] = _build_open_time_series(time_index)

        ordered_columns = [spec.column_name for spec in VALMER_VECTOR_COLUMN_SPECS]
        vector_df = vector_df[["time_index", "unique_identifier", *ordered_columns]]
        vector_df.set_index(["time_index", "unique_identifier"], inplace=True)
        vector_df = self.update_statistics.filter_df_by_latest_value(vector_df)

        return vector_df

    def get_table_metadata(self) -> msc.TableMetaData:
        TS_ID = "vector_de_precios_valmer"
        meta = msc.TableMetaData(
            identifier=TS_ID,
            description="Valmer price vector with translated source columns and derived OHLC bars.",
            data_frequency_id=msc.DataFrequency.one_d,
        )
        return meta
