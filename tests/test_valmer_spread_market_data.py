import datetime as dt
import unittest
from unittest.mock import patch

import pandas as pd
from msm.settings import ASSET_IDENTIFIER_DIMENSION

from valmer_connectors.analytics import (
    SPREAD_SNAPSHOT_COLUMNS,
    default_start_date,
    fetch_market_snapshot,
    fetch_yield_history,
)


class ValmerSpreadMarketDataTest(unittest.TestCase):
    def test_default_start_date_returns_utc_datetime_about_five_years_back(self):
        before = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=365 * 5 + 1)
        result = pd.Timestamp(default_start_date())
        after = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=365 * 5 - 1)

        self.assertEqual(str(result.tz), "UTC")
        self.assertGreater(result, before)
        self.assertLess(result, after)

    def test_fetch_yield_history_avoids_query_for_empty_identifiers(self):
        with patch(
            "valmer_connectors.analytics.spread_market_data.read_valmer_yield_history",
            side_effect=AssertionError("query should not be called"),
        ):
            result = fetch_yield_history([None, "", "nan"])

        self.assertTrue(result.empty)

    def test_fetch_yield_history_calls_query_and_pivots_wide(self):
        query_frame = pd.DataFrame(
            [
                {
                    "time_index": pd.Timestamp("2024-01-01T00:00:00Z"),
                    ASSET_IDENTIFIER_DIMENSION: "M_BONOS_241205",
                    "yield_rate": "9.25",
                },
                {
                    "time_index": pd.Timestamp("2024-01-01T00:00:00Z"),
                    ASSET_IDENTIFIER_DIMENSION: "BI_CETES_1",
                    "yield_rate": "10.5",
                },
                {
                    "time_index": pd.Timestamp("2024-01-02T00:00:00Z"),
                    ASSET_IDENTIFIER_DIMENSION: "M_BONOS_241205",
                    "yield_rate": "9.5",
                },
            ]
        )

        with patch(
            "valmer_connectors.analytics.spread_market_data.read_valmer_yield_history",
            return_value=query_frame,
        ) as read_history:
            result = fetch_yield_history(
                ["M_BONOS_241205", "M_BONOS_241205", "BI_CETES_1"],
                start_date=pd.Timestamp("2024-01-01"),
                end_date=pd.Timestamp("2024-01-03", tz="UTC"),
            )

        read_history.assert_called_once_with(
            ["M_BONOS_241205", "BI_CETES_1"],
            start_date=dt.datetime(2024, 1, 1, tzinfo=dt.UTC),
            end_date=dt.datetime(2024, 1, 3, tzinfo=dt.UTC),
        )
        self.assertEqual(result.columns.tolist(), ["BI_CETES_1", "M_BONOS_241205"])
        self.assertEqual(result.loc[pd.Timestamp("2024-01-01T00:00:00Z"), "BI_CETES_1"], 10.5)
        self.assertEqual(
            result.loc[pd.Timestamp("2024-01-02T00:00:00Z"), "M_BONOS_241205"],
            9.5,
        )

    def test_fetch_yield_history_returns_empty_when_yield_rate_is_missing(self):
        with patch(
            "valmer_connectors.analytics.spread_market_data.read_valmer_yield_history",
            return_value=pd.DataFrame(
                [
                    {
                        "time_index": pd.Timestamp("2024-01-01T00:00:00Z"),
                        ASSET_IDENTIFIER_DIMENSION: "M_BONOS_241205",
                    }
                ]
            ),
        ):
            result = fetch_yield_history(["M_BONOS_241205"])

        self.assertTrue(result.empty)

    def test_fetch_market_snapshot_avoids_query_for_empty_identifiers(self):
        with patch(
            "valmer_connectors.analytics.spread_market_data.read_valmer_last_observation",
            side_effect=AssertionError("query should not be called"),
        ):
            result = fetch_market_snapshot([None, "", "none"])

        self.assertTrue(result.empty)

    def test_fetch_market_snapshot_calls_query_and_coerces_numeric_columns(self):
        snapshot = pd.DataFrame(
            [
                {
                    ASSET_IDENTIFIER_DIMENSION: "M_BONOS_241205",
                    "yield_rate": "9.25",
                    "dirty_price": "101.1",
                    "clean_price": "100.9",
                    "duration": "2.5",
                    "macaulay_duration": "2.6",
                    "monetary_duration": "250.0",
                    "convexity": "0.7",
                    "spread": "0.05",
                }
            ]
        )

        with patch(
            "valmer_connectors.analytics.spread_market_data.read_valmer_last_observation",
            return_value=snapshot,
        ) as read_latest:
            result = fetch_market_snapshot(
                ["M_BONOS_241205", "M_BONOS_241205"],
                as_of=pd.Timestamp("2024-01-03"),
            )

        read_latest.assert_called_once_with(
            ["M_BONOS_241205"],
            as_of=dt.datetime(2024, 1, 3, tzinfo=dt.UTC),
            columns=SPREAD_SNAPSHOT_COLUMNS,
        )
        for column in SPREAD_SNAPSHOT_COLUMNS:
            self.assertEqual(str(result[column].dtype), "float64")
        self.assertEqual(result["dirty_price"].iloc[0], 101.1)


if __name__ == "__main__":
    unittest.main()
