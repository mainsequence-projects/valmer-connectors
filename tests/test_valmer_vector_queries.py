import datetime as dt
import inspect
import unittest
from unittest.mock import Mock, patch

import pandas as pd
from msm.settings import ASSET_IDENTIFIER_DIMENSION

from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
from valmer_connectors.queries import vector_quotes
from valmer_connectors.queries.vector_quotes import (
    clean_valmer_identifiers,
    filter_valmer_vector_columns,
    latest_dirty_price_by_identifier,
    normalize_valmer_quote_frame,
    read_valmer_history,
    read_valmer_last_observation,
    read_valmer_yield_history,
    valmer_vector_node,
    valmer_vector_node_identifier,
    valmer_vector_storage_columns,
)


class ValmerVectorQueriesTest(unittest.TestCase):
    def test_vector_node_identifier_derives_from_storage_contract(self):
        self.assertEqual(
            valmer_vector_node_identifier(),
            str(ValmerVectorPricesStorage.__metatable_identifier__),
        )

    def test_vector_queries_do_not_depend_on_fundcompetition_settings(self):
        self.assertNotIn("fundcompetition", inspect.getsource(vector_quotes))

    def test_valmer_vector_node_builds_api_node_from_bound_storage_metatable(self):
        api_node = object()
        meta_table = object()

        with patch(
            "valmer_connectors.queries.vector_quotes.ValmerVectorPricesStorage.get_meta_table",
            return_value=meta_table,
        ) as get_meta_table, patch(
            "mainsequence.meta_tables.APIDataNode.build_from_meta_table",
            return_value=api_node,
        ) as build_from_meta_table:
            result = valmer_vector_node()

        self.assertIs(result, api_node)
        get_meta_table.assert_called_once_with()
        build_from_meta_table.assert_called_once_with(
            meta_table,
        )

    def test_valmer_vector_node_requires_runtime_bound_storage_metatable(self):
        with patch(
            "valmer_connectors.queries.vector_quotes.ValmerVectorPricesStorage.get_meta_table",
            return_value=None,
        ), patch(
            "mainsequence.meta_tables.APIDataNode.build_from_identifier",
            side_effect=AssertionError("must not look up the vector by string identifier"),
        ), patch(
            "mainsequence.meta_tables.APIDataNode.build_from_meta_table",
            side_effect=AssertionError("must not build without a bound MetaTable"),
        ):
            with self.assertRaisesRegex(RuntimeError, "Valmer vector storage is not bound"):
                valmer_vector_node()

    def test_storage_columns_are_read_from_registered_storage_schema(self):
        self.assertIn("time_index", valmer_vector_storage_columns())
        self.assertIn(ASSET_IDENTIFIER_DIMENSION, valmer_vector_storage_columns())
        self.assertIn("dirty_price", valmer_vector_storage_columns())

    def test_filter_vector_columns_keeps_index_columns_and_registered_columns(self):
        self.assertEqual(
            filter_valmer_vector_columns(["dirty_price", "missing", "dirty_price"]),
            ["time_index", ASSET_IDENTIFIER_DIMENSION, "dirty_price"],
        )

    def test_clean_identifiers_preserves_order_and_deduplicates(self):
        self.assertEqual(
            clean_valmer_identifiers(
                [" M_BONOS_241205 ", None, "", "nan", "M_BONOS_241205", "BI_CETES_1"]
            ),
            ["M_BONOS_241205", "BI_CETES_1"],
        )

    def test_read_history_avoids_platform_call_for_empty_identifiers(self):
        with patch(
            "valmer_connectors.queries.vector_quotes.valmer_vector_node",
            side_effect=AssertionError("platform should not be called"),
        ):
            result = read_valmer_history([], start_date=dt.datetime(2024, 1, 1))

        self.assertTrue(result.empty)

    def test_read_history_calls_api_node_with_asset_dimension_filter(self):
        node = Mock()
        node.get_df_between_dates.return_value = pd.DataFrame(
            [
                {
                    "time_index": "2024-01-02T00:00:00Z",
                    ASSET_IDENTIFIER_DIMENSION: "M_BONOS_241205",
                    "dirty_price": "99.25",
                }
            ]
        )

        with patch(
            "valmer_connectors.queries.vector_quotes.valmer_vector_node",
            return_value=node,
        ):
            result = read_valmer_history(
                ["M_BONOS_241205", "M_BONOS_241205"],
                start_date=dt.datetime(2024, 1, 1),
                end_date=pd.Timestamp("2024-01-03", tz="UTC"),
                columns=["dirty_price"],
            )

        node.get_df_between_dates.assert_called_once()
        kwargs = node.get_df_between_dates.call_args.kwargs
        self.assertEqual(
            kwargs["dimension_filters"],
            {ASSET_IDENTIFIER_DIMENSION: ["M_BONOS_241205"]},
        )
        self.assertEqual(
            kwargs["columns"],
            ["time_index", ASSET_IDENTIFIER_DIMENSION, "dirty_price"],
        )
        self.assertEqual(kwargs["start_date"], dt.datetime(2024, 1, 1, tzinfo=dt.UTC))
        self.assertEqual(
            kwargs["end_date"],
            dt.datetime(2024, 1, 3, tzinfo=dt.UTC),
        )
        self.assertEqual(result["dirty_price"].iloc[0], 99.25)

    def test_normalize_quote_frame_resets_index_and_coerces_types(self):
        frame = pd.DataFrame(
            [{"dirty_price": "101.5", "yield_rate": "9.75"}],
            index=pd.MultiIndex.from_tuples(
                [(pd.Timestamp("2024-01-02"), "M_BONOS_241205")],
                names=["time_index", ASSET_IDENTIFIER_DIMENSION],
            ),
        )

        result = normalize_valmer_quote_frame(frame)

        self.assertIn("time_index", result.columns)
        self.assertIn(ASSET_IDENTIFIER_DIMENSION, result.columns)
        self.assertEqual(str(result["time_index"].dt.tz), "UTC")
        self.assertEqual(result["dirty_price"].iloc[0], 101.5)
        self.assertEqual(result["yield_rate"].iloc[0], 9.75)

    def test_last_observation_uses_backend_latest_per_asset_as_of(self):
        node = Mock()
        frame = pd.DataFrame(
            [
                {
                    "time_index": pd.Timestamp("2024-01-03T00:00:00Z"),
                    ASSET_IDENTIFIER_DIMENSION: "M_BONOS_241205",
                    "dirty_price": 101.0,
                    "yield_rate": 9.5,
                },
                {
                    "time_index": pd.Timestamp("2024-01-02T00:00:00Z"),
                    ASSET_IDENTIFIER_DIMENSION: "BI_CETES_1",
                    "dirty_price": 10.0,
                    "yield_rate": 8.25,
                },
            ]
        ).set_index(["time_index", ASSET_IDENTIFIER_DIMENSION])
        node.get_last_observation.return_value = frame

        as_of = dt.datetime(2024, 1, 3, tzinfo=dt.UTC)
        latest_search_start = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
        with (
            patch(
                "valmer_connectors.queries.vector_quotes.valmer_vector_node",
                return_value=node,
            ),
            patch(
                "valmer_connectors.queries.vector_quotes.read_valmer_history",
                side_effect=AssertionError("latest reads must not fetch history"),
            ),
        ):
            result = read_valmer_last_observation(
                ["M_BONOS_241205", "M_BONOS_241205", "BI_CETES_1"],
                as_of=as_of,
                columns=["dirty_price"],
                latest_search_start=latest_search_start,
            )

        node.get_last_observation.assert_called_once_with(
            dimension_range_map=[
                {
                    "coordinate": {ASSET_IDENTIFIER_DIMENSION: "M_BONOS_241205"},
                    "end_date": as_of,
                    "start_date": latest_search_start,
                },
                {
                    "coordinate": {ASSET_IDENTIFIER_DIMENSION: "BI_CETES_1"},
                    "end_date": as_of,
                    "start_date": latest_search_start,
                },
            ],
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(
            result.columns.tolist(),
            ["time_index", ASSET_IDENTIFIER_DIMENSION, "dirty_price"],
        )
        prices = dict(zip(result[ASSET_IDENTIFIER_DIMENSION], result["dirty_price"]))
        self.assertEqual(prices, {"M_BONOS_241205": 101.0, "BI_CETES_1": 10.0})

    def test_latest_dirty_price_map_omits_missing_values(self):
        frame = pd.DataFrame(
            [
                {ASSET_IDENTIFIER_DIMENSION: "M_BONOS_241205", "dirty_price": 101.0},
                {ASSET_IDENTIFIER_DIMENSION: "BI_CETES_1", "dirty_price": pd.NA},
            ]
        )

        with patch(
            "valmer_connectors.queries.vector_quotes.read_valmer_last_observation",
            return_value=frame,
        ):
            result = latest_dirty_price_by_identifier(["M_BONOS_241205", "BI_CETES_1"])

        self.assertEqual(result, {"M_BONOS_241205": 101.0})

    def test_read_yield_history_requests_yield_related_columns(self):
        with patch(
            "valmer_connectors.queries.vector_quotes.read_valmer_history",
            return_value=pd.DataFrame(),
        ) as read_history:
            read_valmer_yield_history(
                ["M_BONOS_241205"],
                start_date=dt.datetime(2024, 1, 1, tzinfo=dt.UTC),
            )

        self.assertEqual(
            read_history.call_args.kwargs["columns"],
            ["yield_rate", "duration", "monetary_duration"],
        )


if __name__ == "__main__":
    unittest.main()
