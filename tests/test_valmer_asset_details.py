import datetime as dt
import uuid
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd

from valmer_connectors.meta_tables.valmer_asset_details import (
    ValmerAssetDetailVersion,
    resolve_valmer_asset_detail_versions,
    upsert_valmer_asset_details,
)


class ValmerAssetDetailsTests(unittest.TestCase):
    def test_upsert_preserves_unique_identifier_after_indexing(self):
        asset_uid = uuid.uuid4()
        second_asset_uid = uuid.uuid4()
        captured_batches = []
        source = pd.DataFrame(
            [
                {
                    "unique_identifier": "M_BONOS_241205",
                    "fecha": "20240102",
                    "tipovalor": "M",
                    "emisora": "BONOS",
                    "serie": "241205",
                },
                {
                    "unique_identifier": "LD_BONDESD_250101",
                    "fecha": "20240102",
                    "tipovalor": "LD",
                    "emisora": "BONDESD",
                    "serie": "250101",
                }
            ]
        )

        def fake_bulk_upsert_model(*_, values, **__):
            captured_batches.append(values)
            return {"rows": values}

        with patch(
            "valmer_connectors.meta_tables.valmer_asset_details.ensure_valmer_asset_detail_runtime",
            return_value=object(),
        ):
            with patch(
                "valmer_connectors.meta_tables.valmer_asset_details.resolve_valmer_asset_detail_versions",
                return_value={},
            ):
                with patch("msm.repositories.crud.search_model", side_effect=AssertionError):
                    with patch(
                        "msm.repositories.crud.bulk_upsert_model",
                        side_effect=fake_bulk_upsert_model,
                    ):
                        with patch(
                            "msm.api.base.operation_result_rows",
                            side_effect=lambda result: result["rows"],
                        ):
                            upsert_valmer_asset_details(
                                source,
                                {
                                    "M_BONOS_241205": SimpleNamespace(uid=asset_uid),
                                    "LD_BONDESD_250101": SimpleNamespace(uid=second_asset_uid),
                                },
                            )

        self.assertEqual(len(captured_batches), 1)
        self.assertEqual(len(captured_batches[0]), 2)
        self.assertEqual(
            captured_batches[0][0]["valmer_unique_identifier"], "M_BONOS_241205"
        )
        self.assertEqual(captured_batches[0][0]["asset_uid"], asset_uid)
        self.assertEqual(
            captured_batches[0][1]["valmer_unique_identifier"], "LD_BONDESD_250101"
        )
        self.assertEqual(captured_batches[0][1]["asset_uid"], second_asset_uid)

    def test_upsert_skips_same_source_date_detail_rows(self):
        asset_uid = uuid.uuid4()
        source = pd.DataFrame(
            [
                {
                    "unique_identifier": "M_BONOS_241205",
                    "fecha": "20240102",
                    "tipovalor": "M",
                    "emisora": "BONOS",
                    "serie": "241205",
                    "nombrecompleto": "BONOS UPDATED NAME",
                }
            ]
        )
        existing_row = {
            "M_BONOS_241205": ValmerAssetDetailVersion(
                asset_uid=asset_uid,
                valmer_unique_identifier="M_BONOS_241205",
                details_asof=dt.datetime(2024, 1, 2, tzinfo=dt.UTC),
            )
        }
        bulk_upsert_model = Mock()

        with patch(
            "valmer_connectors.meta_tables.valmer_asset_details.ensure_valmer_asset_detail_runtime",
            return_value=object(),
        ):
            with patch(
                "valmer_connectors.meta_tables.valmer_asset_details.resolve_valmer_asset_detail_versions",
                return_value=existing_row,
            ):
                with patch("msm.repositories.crud.search_model", side_effect=AssertionError):
                    with patch(
                        "msm.repositories.crud.bulk_upsert_model",
                        bulk_upsert_model,
                    ):
                        with patch(
                            "msm.api.base.operation_result_rows",
                            side_effect=lambda result: result["rows"],
                        ):
                            result = upsert_valmer_asset_details(
                                source,
                                {"M_BONOS_241205": SimpleNamespace(uid=asset_uid)},
                            )

        self.assertEqual(result, {})
        bulk_upsert_model.assert_not_called()

    def test_upsert_writes_detail_rows_from_newer_source_dates(self):
        asset_uid = uuid.uuid4()
        source = pd.DataFrame(
            [
                {
                    "unique_identifier": "M_BONOS_241205",
                    "fecha": "20240103",
                    "tipovalor": "M",
                    "emisora": "BONOS",
                    "serie": "241205",
                }
            ]
        )
        existing_row = {
            "M_BONOS_241205": ValmerAssetDetailVersion(
                asset_uid=asset_uid,
                valmer_unique_identifier="M_BONOS_241205",
                details_asof=dt.datetime(2024, 1, 2, tzinfo=dt.UTC),
            )
        }
        captured_batches = []

        def fake_bulk_upsert_model(*_, values, **__):
            captured_batches.append(values)
            return {"rows": values}

        with patch(
            "valmer_connectors.meta_tables.valmer_asset_details.ensure_valmer_asset_detail_runtime",
            return_value=object(),
        ):
            with patch(
                "valmer_connectors.meta_tables.valmer_asset_details.resolve_valmer_asset_detail_versions",
                return_value=existing_row,
            ):
                with patch("msm.repositories.crud.search_model", side_effect=AssertionError):
                    with patch(
                        "msm.repositories.crud.bulk_upsert_model",
                        side_effect=fake_bulk_upsert_model,
                    ):
                        with patch(
                            "msm.api.base.operation_result_rows",
                            side_effect=lambda result: result["rows"],
                        ):
                            result = upsert_valmer_asset_details(
                                source,
                                {"M_BONOS_241205": SimpleNamespace(uid=asset_uid)},
                            )

        self.assertEqual(len(captured_batches), 1)
        self.assertEqual(len(captured_batches[0]), 1)
        self.assertIn("M_BONOS_241205", result)

    def test_upsert_skips_older_source_date_detail_rows(self):
        asset_uid = uuid.uuid4()
        source = pd.DataFrame(
            [
                {
                    "unique_identifier": "M_BONOS_241205",
                    "fecha": "20240101",
                    "tipovalor": "M",
                    "emisora": "BONOS",
                    "serie": "241205",
                    "nombrecompleto": "BONOS OLD SOURCE NAME",
                }
            ]
        )
        existing_row = {
            "M_BONOS_241205": ValmerAssetDetailVersion(
                asset_uid=asset_uid,
                valmer_unique_identifier="M_BONOS_241205",
                details_asof=dt.datetime(2024, 1, 2, tzinfo=dt.UTC),
            )
        }
        bulk_upsert_model = Mock()

        with patch(
            "valmer_connectors.meta_tables.valmer_asset_details.ensure_valmer_asset_detail_runtime",
            return_value=object(),
        ):
            with patch(
                "valmer_connectors.meta_tables.valmer_asset_details.resolve_valmer_asset_detail_versions",
                return_value=existing_row,
            ):
                with patch("msm.repositories.crud.search_model", side_effect=AssertionError):
                    with patch(
                        "msm.repositories.crud.bulk_upsert_model",
                        bulk_upsert_model,
                    ):
                        with patch(
                            "msm.api.base.operation_result_rows",
                            side_effect=lambda result: result["rows"],
                        ):
                            result = upsert_valmer_asset_details(
                                source,
                                {"M_BONOS_241205": SimpleNamespace(uid=asset_uid)},
                            )

        self.assertEqual(result, {})
        bulk_upsert_model.assert_not_called()

    def test_upsert_writes_changed_static_detail_rows_when_source_date_is_newer(self):
        asset_uid = uuid.uuid4()
        captured_batches = []
        source = pd.DataFrame(
            [
                {
                    "unique_identifier": "M_BONOS_241205",
                    "fecha": "20240103",
                    "tipovalor": "M",
                    "emisora": "BONOS",
                    "serie": "241205",
                    "nombrecompleto": "BONOS NEW NAME",
                }
            ]
        )
        existing_row = {
            "M_BONOS_241205": ValmerAssetDetailVersion(
                asset_uid=asset_uid,
                valmer_unique_identifier="M_BONOS_241205",
                details_asof=dt.datetime(2024, 1, 2, tzinfo=dt.UTC),
            )
        }

        def fake_bulk_upsert_model(*_, values, **__):
            captured_batches.append(values)
            return {"rows": values}

        with patch(
            "valmer_connectors.meta_tables.valmer_asset_details.ensure_valmer_asset_detail_runtime",
            return_value=object(),
        ):
            with patch(
                "valmer_connectors.meta_tables.valmer_asset_details.resolve_valmer_asset_detail_versions",
                return_value=existing_row,
            ):
                with patch("msm.repositories.crud.search_model", side_effect=AssertionError):
                    with patch(
                        "msm.repositories.crud.bulk_upsert_model",
                        side_effect=fake_bulk_upsert_model,
                    ):
                        with patch(
                            "msm.api.base.operation_result_rows",
                            side_effect=lambda result: result["rows"],
                        ):
                            result = upsert_valmer_asset_details(
                                source,
                                {"M_BONOS_241205": SimpleNamespace(uid=asset_uid)},
                            )

        self.assertEqual(len(captured_batches), 1)
        self.assertEqual(len(captured_batches[0]), 1)
        self.assertEqual(result["M_BONOS_241205"]["full_name"], "BONOS NEW NAME")

    def test_detail_version_resolver_uses_projection_query(self):
        asset_uid = uuid.uuid4()

        with patch(
            "valmer_connectors.meta_tables.valmer_asset_details.ensure_valmer_asset_detail_runtime",
            return_value=object(),
        ):
            with patch(
                "msm.repositories.crud.search_model",
                side_effect=AssertionError("full-row search_model should not be used"),
            ):
                with patch(
                    "msm.repositories.base.compile_markets_statement",
                    return_value=object(),
                ):
                    with patch(
                        "msm.repositories.base.execute_markets_operation",
                        return_value={
                            "rows": [
                                {
                                    "asset_uid": str(asset_uid),
                                    "valmer_unique_identifier": "M_BONOS_241205",
                                    "details_asof": "2024-01-02T00:00:00+00:00",
                                }
                            ]
                        },
                    ):
                        versions = resolve_valmer_asset_detail_versions([asset_uid])

        self.assertEqual(
            versions["M_BONOS_241205"],
            ValmerAssetDetailVersion(
                asset_uid=asset_uid,
                valmer_unique_identifier="M_BONOS_241205",
                details_asof=dt.datetime(2024, 1, 2, tzinfo=dt.UTC),
            ),
        )


if __name__ == "__main__":
    unittest.main()
