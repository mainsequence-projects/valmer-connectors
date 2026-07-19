import datetime as dt
import unittest
from unittest.mock import Mock

import pandas as pd
from msm.base import markets_table_name
from msm.models.indices import IndexTable
from pydantic import ValidationError

from valmer_connectors.data_nodes.reference_rate_observations import (
    ReferenceRateObservationConfiguration,
    ReferenceRateObservationError,
    ReferenceRateObservationsStorage,
    normalize_reference_rate_rows,
    resolve_reference_rate_update_window,
)
from valmer_connectors.markets import (
    VALMER_MARKETS_NAMESPACE,
    VALMER_MARKETS_STORAGE_APP,
)


class ReferenceRateStorageTests(unittest.TestCase):
    def test_storage_contract_is_project_owned_and_index_keyed(self):
        self.assertEqual(
            ReferenceRateObservationsStorage.__metatable_identifier__,
            "valmer_connectors.reference_rate_observations",
        )
        self.assertEqual(
            ReferenceRateObservationsStorage.__table__.name,
            markets_table_name(
                VALMER_MARKETS_STORAGE_APP,
                "reference_rate_observations",
            ),
        )
        self.assertEqual(
            ReferenceRateObservationsStorage.__table__.info["namespace"],
            VALMER_MARKETS_NAMESPACE,
        )
        self.assertEqual(
            ReferenceRateObservationsStorage.__index_names__,
            ["time_index", "index_identifier"],
        )
        self.assertEqual(ReferenceRateObservationsStorage.__cadence__, "1d")
        self.assertEqual(
            ReferenceRateObservationsStorage.__metatable_extra_hash_components__,
            {"storage_name": "reference_rate_observations"},
        )

        columns = ReferenceRateObservationsStorage.__table__.c
        self.assertFalse(columns.time_index.nullable)
        self.assertFalse(columns.index_identifier.nullable)
        self.assertFalse(columns.rate.nullable)
        self.assertIn("normalized", columns.time_index.info["description"])
        self.assertIn("IndexTable.unique_identifier", columns.index_identifier.info["description"])
        self.assertIn("exactly once", columns.rate.info["description"])
        self.assertEqual(
            {foreign_key.target_fullname for foreign_key in columns.index_identifier.foreign_keys},
            {f"{IndexTable.__table__.fullname}.unique_identifier"},
        )

    def test_frame_normalization_uses_nanosecond_utc_multi_index(self):
        frame = normalize_reference_rate_rows(
            [
                {
                    "time_index": "2026-07-17",
                    "index_identifier": "US_TREASURY_CMT_10Y",
                    "rate": 0.041,
                }
            ]
        )

        self.assertEqual(frame.index.names, ["time_index", "index_identifier"])
        self.assertEqual(str(frame.index.levels[0].dtype), "datetime64[ns, UTC]")
        self.assertEqual(frame.iloc[0]["rate"], 0.041)

    def test_frame_normalization_rejects_duplicate_keys(self):
        rows = [
            {
                "time_index": "2026-07-17",
                "index_identifier": "US_TREASURY_CMT_10Y",
                "rate": 0.041,
            }
        ]
        with self.assertRaisesRegex(ValueError, "duplicate"):
            normalize_reference_rate_rows([*rows, *rows])

    def test_frame_normalization_rejects_non_finite_rates(self):
        with self.assertRaisesRegex(ReferenceRateObservationError, "finite"):
            normalize_reference_rate_rows(
                [
                    {
                        "time_index": "2026-07-17",
                        "index_identifier": "US_TREASURY_CMT_10Y",
                        "rate": float("inf"),
                    }
                ]
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
        self.assertIsNone(first.backfill_end)
        self.assertEqual(first.bootstrap_lookback_days, 90)

    def test_configuration_rejects_empty_duplicate_and_partial_backfill_scope(self):
        invalid_inputs = (
            {"index_unique_identifiers": [""]},
            {"index_unique_identifiers": ["A", "A"]},
            {
                "index_unique_identifiers": ["A"],
                "offset_start": "2021-01-01T00:00:00Z",
            },
            {
                "index_unique_identifiers": ["A"],
                "backfill_end": "2021-01-01T00:00:00Z",
            },
        )
        for payload in invalid_inputs:
            with self.subTest(payload=payload), self.assertRaises(ValidationError):
                ReferenceRateObservationConfiguration(**payload)

    def test_configuration_requires_aware_ordered_backfill_bounds(self):
        for payload in (
            {
                "offset_start": "2021-01-01T00:00:00",
                "backfill_end": "2021-01-02T00:00:00",
            },
            {
                "offset_start": "2021-01-03T00:00:00Z",
                "backfill_end": "2021-01-02T00:00:00Z",
            },
        ):
            with self.subTest(payload=payload), self.assertRaises(ValidationError):
                ReferenceRateObservationConfiguration(
                    index_unique_identifiers=["A"],
                    **payload,
                )

    def test_default_window_is_exactly_ninety_inclusive_calendar_days(self):
        statistics = Mock()
        statistics.get_last_update_for_identity.return_value = None
        config = ReferenceRateObservationConfiguration(
            index_unique_identifiers=["A"],
            bootstrap_lookback_days=90,
        )

        window = resolve_reference_rate_update_window(
            update_statistics=statistics,
            config=config,
            index_identifier="A",
            runtime_end="2026-07-18",
        )

        self.assertEqual(window.start_date, dt.date(2026, 4, 20))
        self.assertEqual(window.end_date, dt.date(2026, 7, 18))
        self.assertEqual((window.end_date - window.start_date).days + 1, 90)

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

    def test_bounded_backfill_ignores_normal_progress(self):
        statistics = Mock()
        statistics.get_last_update_for_identity.return_value = pd.Timestamp(
            "2026-07-16", tz="UTC"
        )
        config = ReferenceRateObservationConfiguration(
            index_unique_identifiers=["A"],
            offset_start="2021-07-19T00:00:00Z",
            backfill_end="2026-04-19T00:00:00Z",
        )

        window = resolve_reference_rate_update_window(
            update_statistics=statistics,
            config=config,
            index_identifier="A",
            runtime_end="2026-07-18",
        )

        self.assertEqual(window.start_date, dt.date(2021, 7, 19))
        self.assertEqual(window.end_date, dt.date(2026, 4, 19))
        statistics.get_last_update_for_identity.assert_not_called()


if __name__ == "__main__":
    unittest.main()
