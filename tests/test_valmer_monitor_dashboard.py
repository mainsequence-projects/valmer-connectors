from __future__ import annotations

import inspect
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
from msm.settings import ASSET_IDENTIFIER_DIMENSION

APP_DIR = Path(__file__).resolve().parents[1] / "dashboards" / "valmer_monitor"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import valmer_dashboard


class ValmerMonitorDashboardTest(unittest.TestCase):
    def test_vector_snapshot_uses_backend_latest_observation(self):
        table_ref = Mock()
        table_ref.get_df_between_dates.side_effect = AssertionError("must not fetch history")
        table_ref.get_last_observation.return_value = pd.DataFrame(
            [
                {
                    "time_index": pd.Timestamp("2024-01-03T00:00:00Z"),
                    ASSET_IDENTIFIER_DIMENSION: "M_BONOS_241205",
                    "dirty_price": "101.25",
                }
            ]
        ).set_index(["time_index", ASSET_IDENTIFIER_DIMENSION])

        with patch.object(
            valmer_dashboard.TimeIndexTableRef,
            "from_identifier",
            return_value=table_ref,
        ):
            result = valmer_dashboard._query_latest_table_observations(
                valmer_dashboard.VECTOR_TABLE_IDENTIFIER,
                unique_identifier_list=["M_BONOS_241205"],
            )

        table_ref.get_last_observation.assert_called_once_with(
            dimension_filters={ASSET_IDENTIFIER_DIMENSION: ["M_BONOS_241205"]}
        )
        table_ref.get_df_between_dates.assert_not_called()
        self.assertIsNone(result.error)
        self.assertEqual(result.data["unique_identifier"].tolist(), ["M_BONOS_241205"])
        self.assertEqual(result.data["dirty_price"].iloc[0], 101.25)

    def test_curve_health_uses_backend_latest_observation(self):
        table_ref = Mock()
        table_ref.get_df_between_dates.side_effect = AssertionError("must not fetch history")
        table_ref.get_last_observation.return_value = pd.DataFrame(
            [
                {
                    "time_index": pd.Timestamp("2024-01-03T00:00:00Z"),
                    "curve_identifier": "MXN_TIIE_ON_VALMER",
                    "curve": {28: 0.1125, 91: 0.109},
                }
            ]
        ).set_index("time_index")

        with patch.object(
            valmer_dashboard.TimeIndexTableRef,
            "from_identifier",
            return_value=table_ref,
        ):
            result = valmer_dashboard._load_latest_discount_curve()

        table_ref.get_last_observation.assert_called_once()
        self.assertIn("dimension_range_map", table_ref.get_last_observation.call_args.kwargs)
        table_ref.get_df_between_dates.assert_not_called()
        self.assertIsNone(result.error)
        self.assertEqual(len(result.data.index), 1)

    def test_monitor_has_no_local_latest_reduction_helpers(self):
        source = inspect.getsource(valmer_dashboard)
        self.assertNotIn("latest_vector_snapshot", source)
        self.assertNotIn(".groupby(\"unique_identifier\")[\"time_index\"].idxmax()", source)
        self.assertNotIn(".sort_values(\"time_index\").iloc[-1]", source)


if __name__ == "__main__":
    unittest.main()
