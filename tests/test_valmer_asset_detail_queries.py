import unittest
import uuid
from unittest.mock import patch

from msm.models.assets import AssetTable

from valmer_connectors.meta_tables.valmer_asset_details import ValmerAssetDetailsTable
from valmer_connectors.queries.asset_details import (
    expand_valmer_asset_detail_alias_frame,
    read_valmer_asset_detail_alias_frame,
    read_valmer_asset_detail_maturity_fields,
    resolve_valmer_detail_identifier_aliases,
)


class ValmerAssetDetailQueriesTest(unittest.TestCase):
    def test_empty_identifier_input_avoids_governed_execution(self):
        with patch(
            "valmer_connectors.queries.asset_details.ensure_valmer_asset_detail_runtime",
            side_effect=AssertionError("runtime should not be attached"),
        ):
            result = read_valmer_asset_detail_alias_frame([])

        self.assertTrue(result.empty)

    def test_alias_frame_resolves_valmer_and_canonical_asset_identifiers(self):
        row = self._detail_row(
            asset_table_identifier="M_BONOS_241205",
            valmer_unique_identifier="M_BONOS_241205_VALMER",
        )

        with self._patched_governed_select([row]):
            result = read_valmer_asset_detail_alias_frame(["M_BONOS_241205"])

        self.assertEqual(
            result["asset_identifier"].tolist(),
            ["M_BONOS_241205_VALMER", "M_BONOS_241205"],
        )
        self.assertEqual(
            result["valmer_unique_identifier"].tolist(),
            ["M_BONOS_241205_VALMER", "M_BONOS_241205_VALMER"],
        )

    def test_public_alias_expansion_accepts_projected_rows(self):
        row = self._detail_row(
            asset_table_identifier="M_BONOS_241205",
            valmer_unique_identifier="M_BONOS_241205_VALMER",
        )

        result = expand_valmer_asset_detail_alias_frame([row])

        self.assertEqual(
            result["asset_identifier"].tolist(),
            ["M_BONOS_241205_VALMER", "M_BONOS_241205"],
        )

    def test_alias_frame_collapses_duplicate_aliases(self):
        row = self._detail_row(
            asset_table_identifier="M_BONOS_241205",
            valmer_unique_identifier="M_BONOS_241205",
        )

        with self._patched_governed_select([row]):
            result = read_valmer_asset_detail_alias_frame(["M_BONOS_241205"])

        self.assertEqual(result["asset_identifier"].tolist(), ["M_BONOS_241205"])

    def test_identifier_alias_map_points_aliases_to_valmer_identifier(self):
        row = self._detail_row(
            asset_table_identifier="M_BONOS_241205",
            valmer_unique_identifier="M_BONOS_241205_VALMER",
        )

        with self._patched_governed_select([row]):
            result = resolve_valmer_detail_identifier_aliases(["M_BONOS_241205"])

        self.assertEqual(
            result,
            {
                "M_BONOS_241205_VALMER": "M_BONOS_241205_VALMER",
                "M_BONOS_241205": "M_BONOS_241205_VALMER",
            },
        )

    def test_maturity_fields_use_exact_public_projection(self):
        row = self._detail_row(
            asset_table_identifier="M_BONOS_241205",
            valmer_unique_identifier="M_BONOS_241205",
        )

        with self._patched_governed_select([row]):
            result = read_valmer_asset_detail_maturity_fields(["M_BONOS_241205"])

        self.assertEqual(
            result.columns.tolist(),
            [
                "asset_uid",
                "asset_table_identifier",
                "asset_identifier",
                "valmer_unique_identifier",
                "valmer_security_type",
                "valmer_issuer",
                "valmer_series",
                "valmer_full_name",
                "valmer_issue_date",
                "valmer_maturity_date",
                "maturity_date",
                "valmer_face_value",
                "valmer_coupon_frequency",
                "valmer_coupon_rate",
            ],
        )

    def test_governed_select_declares_read_scope_for_both_tables(self):
        row = self._detail_row(
            asset_table_identifier="M_BONOS_241205",
            valmer_unique_identifier="M_BONOS_241205",
        )

        with self._patched_governed_select([row]) as compile_statement:
            read_valmer_asset_detail_alias_frame(["M_BONOS_241205"])

        kwargs = compile_statement.call_args.kwargs
        self.assertEqual(kwargs["operation"], "select")
        self.assertEqual(kwargs["access"], "read")
        self.assertEqual(kwargs["models"], [ValmerAssetDetailsTable, AssetTable])

    def _patched_governed_select(self, rows):
        context = object()

        class _PatchStack:
            def __enter__(stack_self):
                stack_self.runtime = patch(
                    "valmer_connectors.queries.asset_details.ensure_valmer_asset_detail_runtime",
                    return_value=context,
                )
                stack_self.compile = patch(
                    "msm.repositories.base.compile_markets_statement",
                    return_value=object(),
                )
                stack_self.execute = patch(
                    "msm.repositories.base.execute_markets_operation",
                    return_value={"rows": rows},
                )
                stack_self.operation_rows = patch(
                    "msm.api.base.operation_result_rows",
                    side_effect=lambda result: result["rows"],
                )
                stack_self.runtime.__enter__()
                compile_statement = stack_self.compile.__enter__()
                stack_self.execute.__enter__()
                stack_self.operation_rows.__enter__()
                return compile_statement

            def __exit__(stack_self, exc_type, exc, tb):
                stack_self.operation_rows.__exit__(exc_type, exc, tb)
                stack_self.execute.__exit__(exc_type, exc, tb)
                stack_self.compile.__exit__(exc_type, exc, tb)
                stack_self.runtime.__exit__(exc_type, exc, tb)

        return _PatchStack()

    @staticmethod
    def _detail_row(
        *,
        asset_table_identifier: str,
        valmer_unique_identifier: str,
    ) -> dict:
        return {
            "asset_uid": str(uuid.uuid4()),
            "asset_table_identifier": asset_table_identifier,
            "asset_identifier": valmer_unique_identifier,
            "valmer_unique_identifier": valmer_unique_identifier,
            "valmer_security_type": "M",
            "valmer_issuer": "BONOS",
            "valmer_series": "241205",
            "valmer_full_name": "BONOS 241205",
            "valmer_issue_date": "2020-01-01",
            "valmer_maturity_date": "2024-12-05",
            "maturity_date": "2024-12-05",
            "valmer_face_value": 100.0,
            "valmer_coupon_frequency": "182",
            "valmer_coupon_rate": 8.0,
        }


if __name__ == "__main__":
    unittest.main()
