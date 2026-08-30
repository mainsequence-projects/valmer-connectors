import datetime as dt
import unittest
from unittest.mock import Mock

import pandas as pd
from msm.data_nodes.indices import index_values_output_table_name
from msm.models.indices import IndexTable
from pydantic import ValidationError

from valmer_connectors.data_nodes.canonical_index_values import (
    DailyIndexValuesStorage,
    IndexObservationError,
    ReferenceRateObservationConfiguration,
    canonical_index_value_row,
    normalize_index_value_rows,
    resolve_reference_rate_update_window,
)


class ReferenceRateStorageTests(unittest.TestCase):
    def test_storage_contract_is_canonical_daily_and_index_keyed(self):
        self.assertEqual(
            DailyIndexValuesStorage.__metatable_identifier__,
            "IndexValuesTS.1d",
        )
        self.assertEqual(
            DailyIndexValuesStorage.__table__.name,
            index_values_output_table_name(cadence="1d"),
        )
        self.assertEqual(
            DailyIndexValuesStorage.__index_names__,
            ["time_index", "index_identifier"],
        )
        self.assertEqual(DailyIndexValuesStorage.__cadence__, "1d")
        self.assertEqual(
            DailyIndexValuesStorage.__metatable_extra_hash_components__,
            {"storage_name": "index_values", "cadence": "1d"},
        )

        columns = DailyIndexValuesStorage.__table__.c
        self.assertFalse(columns.time_index.nullable)
        self.assertFalse(columns.index_identifier.nullable)
        self.assertFalse(columns.value.nullable)
        self.assertNotIn("unit", columns)
        self.assertEqual(
            {foreign_key.target_fullname for foreign_key in columns.index_identifier.foreign_keys},
            {f"{IndexTable.__table__.fullname}.unique_identifier"},
        )

    def test_frame_normalization_uses_nanosecond_utc_multi_index(self):
        frame = normalize_index_value_rows(
            [
                canonical_index_value_row(
                    time_index="2026-07-17",
                    index_identifier="US_TREASURY_CMT_10Y",
                    value=0.041,
                )
            ]
        )

        self.assertEqual(frame.index.names, ["time_index", "index_identifier"])
        self.assertEqual(str(frame.index.levels[0].dtype), "datetime64[ns, UTC]")
        self.assertEqual(frame.iloc[0]["value"], 0.041)

    def test_frame_normalization_rejects_duplicate_keys(self):
        rows = [
            canonical_index_value_row(
                time_index="2026-07-17",
                index_identifier="US_TREASURY_CMT_10Y",
                value=0.041,
            )
        ]
        with self.assertRaisesRegex(ValueError, "duplicate"):
            normalize_index_value_rows([*rows, *rows])

    def test_frame_normalization_rejects_non_finite_rates(self):
        with self.assertRaisesRegex(IndexObservationError, "finite"):
            canonical_index_value_row(
                time_index="2026-07-17",
                index_identifier="US_TREASURY_CMT_10Y",
                value=float("inf"),
            )


class ReferenceRateConfigurationTests(unittest.TestCase):
    def test_normal_configuration_is_stable_and_has_no_dynamic_date(self):
        first = ReferenceRateObservationConfiguration(
            index_unique_identifiers=["US_TREASURY_CMT_2Y"],
        )
        second = ReferenceRateObservationConfiguration(
            index_unique_identifiers=["US_TREASURY_CMT_2Y"],
        )

        self.assertEqual(first.model_dump(mode="json"), second.model_dump(mode="json"))
        self.assertIsNone(first.offset_start)
        self.assertEqual(first.initial_history_years, 5)
        self.assertNotIn("initial_history_years", first.model_dump(mode="json"))

    def test_configuration_rejects_empty_duplicate_and_manual_history_scope(self):
        invalid_inputs = (
            {"index_unique_identifiers": [""]},
            {"index_unique_identifiers": ["A", "A"]},
            {
                "index_unique_identifiers": ["A"],
                "offset_start": "2021-01-01T00:00:00Z",
            },
        )
        for payload in invalid_inputs:
            with self.subTest(payload=payload), self.assertRaises(ValidationError):
                ReferenceRateObservationConfiguration(**payload)

    def test_initial_window_is_exactly_five_calendar_years(self):
        statistics = Mock()
        statistics.get_last_update_for_identity.return_value = None
        config = ReferenceRateObservationConfiguration(index_unique_identifiers=["A"])

        window = resolve_reference_rate_update_window(
            update_statistics=statistics,
            config=config,
            index_identifier="A",
            runtime_end="2026-07-18",
        )

        self.assertEqual(window.start_date, dt.date(2021, 7, 19))
        self.assertEqual(window.end_date, dt.date(2026, 7, 18))
        self.assertEqual((window.end_date - window.start_date).days + 1, 1826)

    def test_incremental_window_starts_after_identity_progress(self):
        statistics = Mock()
        statistics.get_last_update_for_identity.return_value = pd.Timestamp(
            "2026-07-16", tz="UTC"
        )
        config = ReferenceRateObservationConfiguration(index_unique_identifiers=["A"])

        window = resolve_reference_rate_update_window(
            update_statistics=statistics,
            config=config,
            index_identifier="A",
            runtime_end="2026-07-18",
        )

        self.assertEqual(window.start_date, dt.date(2026, 7, 17))
        self.assertEqual(window.end_date, dt.date(2026, 7, 18))

if __name__ == "__main__":
    unittest.main()
