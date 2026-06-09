import io
from datetime import timedelta

import pandas as pd
import requests

from valmer_connectors.instruments.curve_bootstrap import VALMER_TIIE_28_CURVE_DEFINITION

VALMER_TIIE_MEXDER_URL = VALMER_TIIE_28_CURVE_DEFINITION.metadata_json["source_url"]
VALMER_TIIE_MEXDER_COLUMNS = ["id", "curve_name", "asof_yyMMdd", "idx", "zero_rate"]


def read_tiie_valmer_csv(content: bytes) -> pd.DataFrame:
    return pd.read_csv(
        io.BytesIO(content),
        header=None,
        names=VALMER_TIIE_MEXDER_COLUMNS,
        sep=",",
        engine="c",
        encoding="latin1",
        dtype=str,
    )


def build_tiie_curve_frame_from_csv(
    content: bytes,
    *,
    curve_identifier: str,
) -> pd.DataFrame:
    df = read_tiie_valmer_csv(content)

    df["asof_yyMMdd"] = pd.to_datetime(df["asof_yyMMdd"], format="%y%m%d")
    df["asof_yyMMdd"] = df["asof_yyMMdd"].dt.tz_localize("UTC")

    base_dt = df["asof_yyMMdd"].iloc[0] - timedelta(days=1)

    df["idx"] = df["idx"].astype(int)
    df["days_to_maturity"] = (df["asof_yyMMdd"] - base_dt).dt.days
    df["zero_rate"] = df["zero_rate"].astype(float) / 100

    df["time_index"] = base_dt
    df["curve_identifier"] = curve_identifier

    return (
        df.groupby(["time_index", "curve_identifier"])
        .apply(lambda g: g.set_index("days_to_maturity")["zero_rate"].to_dict())
        .rename("curve")
        .reset_index()
        .set_index(["time_index", "curve_identifier"])
    )


def build_tiie_valmer(
    *,
    update_statistics,
    curve_identifier: str,
    base_node_curve_points=None,
) -> pd.DataFrame:
    _ = update_statistics, base_node_curve_points
    response = requests.get(VALMER_TIIE_MEXDER_URL, timeout=30)
    response.raise_for_status()
    return build_tiie_curve_frame_from_csv(
        response.content,
        curve_identifier=curve_identifier,
    )
