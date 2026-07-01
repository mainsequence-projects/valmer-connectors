import datetime as dt
import unittest
import uuid
from unittest.mock import patch

from valmer_connectors.instruments.curve_bootstrap import (
    CETE_28_INDEX_UNIQUE_IDENTIFIER,
    TIIE_28_INDEX_UNIQUE_IDENTIFIER,
)
from valmer_connectors.services.pricing_details_repair import (
    SYNTHETIC_GOVERNMENT_INDEX_IDENTIFIER,
    PersistedPricingDetailsRow,
    patch_instrument_dump,
    repair_valmer_asset_pricing_details,
)


class PricingDetailsRepairTests(unittest.TestCase):
    def _row(self, instrument_dump, **overrides):
        values = {
            "scope": "current",
            "asset_uid": uuid.uuid4(),
            "asset_identifier": "M_BONOS_310527",
            "time_index": dt.datetime(2026, 6, 30, 23, 59, 59, tzinfo=dt.UTC),
            "instrument_type": "FixedRateBond",
            "instrument_dump": instrument_dump,
            "serialization_format": "msm_pricing.instrument.v1",
            "pricing_package_version": None,
            "source": "valmer",
            "metadata_json": {"valmer_unique_identifier": "M_BONOS_310527"},
            "valmer_underlying": "Bonos M Bruta(Yield)",
            "valmer_security_type": "M",
            "valmer_issuer": "BONOS",
        }
        values.update(overrides)
        return PersistedPricingDetailsRow(**values)

    def test_patch_calendar_and_synthetic_government_index_name(self):
        cete_uid = uuid.uuid4()
        row = self._row(
            {
                "calendar": {"name": "Mexico-BMV"},
                "schedule": {"calendar": {"name": "Mexico/BMV"}},
                "benchmark_rate_index_name": SYNTHETIC_GOVERNMENT_INDEX_IDENTIFIER,
            }
        )

        patched, changes = patch_instrument_dump(
            row,
            index_uid_by_identifier={CETE_28_INDEX_UNIQUE_IDENTIFIER: cete_uid},
        )

        self.assertEqual(patched["calendar"], {"name": "Mexican stock exchange"})
        self.assertEqual(
            patched["schedule"]["calendar"],
            {"name": "Mexican stock exchange"},
        )
        self.assertEqual(patched["benchmark_rate_index_uid"], str(cete_uid))
        self.assertNotIn("benchmark_rate_index_name", patched)
        self.assertIn("calendar:set_calendar", changes)
        self.assertIn("benchmark_rate_index_name:remove_legacy_field", changes)

    def test_patch_synthetic_government_index_uid_from_asset_underlying(self):
        old_government_uid = uuid.uuid4()
        cete_uid = uuid.uuid4()
        row = self._row(
            {
                "benchmark_rate_index_uid": str(old_government_uid),
            }
        )

        patched, changes = patch_instrument_dump(
            row,
            index_uid_by_identifier={
                SYNTHETIC_GOVERNMENT_INDEX_IDENTIFIER: old_government_uid,
                CETE_28_INDEX_UNIQUE_IDENTIFIER: cete_uid,
            },
        )

        self.assertEqual(patched["benchmark_rate_index_uid"], str(cete_uid))
        self.assertIn("benchmark_rate_index_uid:set_index_uid", changes)

    def test_patch_bad_calendar_class_token(self):
        row = self._row(
            {
                "calendar": {"class": "Mexico-BMV"},
                "schedule": {"calendar": {"class": "Mexico/BMV"}},
            }
        )

        patched, changes = patch_instrument_dump(row, index_uid_by_identifier={})

        self.assertEqual(patched["calendar"], {"name": "Mexican stock exchange"})
        self.assertEqual(
            patched["schedule"]["calendar"],
            {"name": "Mexican stock exchange"},
        )
        self.assertIn("calendar:set_calendar", changes)
        self.assertIn("schedule.calendar:set_calendar", changes)

    def test_patch_mexico_calendar_name_to_display_name(self):
        row = self._row(
            {
                "calendar": {"name": "Mexico"},
                "schedule": {"calendar": {"name": "Mexico"}},
                "calendar_code": "Mexico",
            }
        )

        patched, changes = patch_instrument_dump(row, index_uid_by_identifier={})

        self.assertEqual(patched["calendar"], {"name": "Mexican stock exchange"})
        self.assertEqual(
            patched["schedule"]["calendar"],
            {"name": "Mexican stock exchange"},
        )
        self.assertEqual(patched["calendar_code"], "Mexico")
        self.assertIn("calendar:set_calendar", changes)
        self.assertIn("schedule.calendar:set_calendar", changes)

    def test_patch_floating_index_name_to_uid(self):
        tiie_uid = uuid.uuid4()
        row = self._row(
            {
                "floating_rate_index_name": "TIIE28",
            },
            instrument_type="FloatingRateBond",
            valmer_underlying="TIIE28",
        )

        patched, changes = patch_instrument_dump(
            row,
            index_uid_by_identifier={TIIE_28_INDEX_UNIQUE_IDENTIFIER: tiie_uid},
        )

        self.assertEqual(patched["floating_rate_index_uid"], str(tiie_uid))
        self.assertNotIn("floating_rate_index_name", patched)
        self.assertIn("floating_rate_index_name:remove_legacy_field", changes)

    def test_repair_dry_run_does_not_write(self):
        row = self._row({"calendar": "Mexico-BMV"})

        with (
            patch(
                "valmer_connectors.services.pricing_details_repair.bootstrap_runtime"
            ),
            patch(
                "valmer_connectors.services.pricing_details_repair._load_index_uid_by_identifier",
                return_value={},
            ),
            patch(
                "valmer_connectors.services.pricing_details_repair._query_repair_rows",
                return_value=[row],
            ) as query_rows,
            patch(
                "valmer_connectors.services.pricing_details_repair._bulk_upsert_repair_plans"
            ) as bulk_upsert,
        ):
            summary = repair_valmer_asset_pricing_details(apply=False)

        self.assertEqual(query_rows.call_count, 2)
        bulk_upsert.assert_not_called()
        self.assertFalse(summary.applied)
        self.assertEqual(summary.candidate_rows, 2)
        self.assertEqual(summary.changes["calendar:set_calendar"], 2)

    def test_repair_apply_writes_current_and_timestamped(self):
        current = self._row({"calendar": "Mexico-BMV"})
        timestamped = self._row(
            {"schedule": {"calendar": "Mexico/BMV"}},
            scope="timestamped",
        )

        def fake_query(*, scope, **_kwargs):
            if fake_query.calls >= 2:
                return []
            fake_query.calls += 1
            return [current] if scope == "current" else [timestamped]

        fake_query.calls = 0

        with (
            patch(
                "valmer_connectors.services.pricing_details_repair.bootstrap_runtime"
            ),
            patch(
                "valmer_connectors.services.pricing_details_repair._load_index_uid_by_identifier",
                return_value={},
            ),
            patch(
                "valmer_connectors.services.pricing_details_repair._query_repair_rows",
                side_effect=fake_query,
            ),
            patch(
                "valmer_connectors.services.pricing_details_repair._bulk_upsert_repair_plans"
            ) as bulk_upsert,
        ):
            summary = repair_valmer_asset_pricing_details(apply=True)

        bulk_upsert.assert_called_once()
        self.assertTrue(summary.applied)
        self.assertEqual(summary.patched_current_rows, 1)
        self.assertEqual(summary.patched_timestamped_rows, 1)

    def test_apply_verification_ignores_query_false_positives(self):
        candidate = self._row({"calendar": "Mexico-BMV"})
        false_positive = self._row({"description": "CETE_28"})

        query_results = [
            [candidate],
            [],
            [false_positive],
            [],
        ]

        def fake_query(**_kwargs):
            return query_results.pop(0)

        with (
            patch(
                "valmer_connectors.services.pricing_details_repair.bootstrap_runtime"
            ),
            patch(
                "valmer_connectors.services.pricing_details_repair._load_index_uid_by_identifier",
                return_value={},
            ),
            patch(
                "valmer_connectors.services.pricing_details_repair._query_repair_rows",
                side_effect=fake_query,
            ),
            patch(
                "valmer_connectors.services.pricing_details_repair._bulk_upsert_repair_plans"
            ),
        ):
            summary = repair_valmer_asset_pricing_details(apply=True)

        self.assertTrue(summary.applied)
        self.assertEqual(summary.patched_current_rows, 1)


if __name__ == "__main__":
    unittest.main()
