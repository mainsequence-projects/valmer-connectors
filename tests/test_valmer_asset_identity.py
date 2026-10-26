import unittest
import uuid
from unittest.mock import Mock, patch

import pandas as pd
from msm.api.assets import Asset
from msm.constants import ASSET_TYPE_BOND, ASSET_TYPE_BOND_DEFINITION

from valmer_connectors.instruments.asset_identity import (
    ValmerAssetRef,
    _normalize_asset_type_map,
    _upsert_asset_table_rows,
    add_valmer_unique_identifier,
    build_valmer_unique_identifier,
    ensure_valmer_asset_runtime,
    normalize_valmer_unique_identifiers,
    resolve_valmer_asset_refs,
    resolve_valmer_asset_uids,
    resolve_valmer_assets,
)


class ValmerAssetIdentityTests(unittest.TestCase):
    def test_build_valmer_unique_identifier(self):
        row = {"tipovalor": "M", "emisora": "BONOS", "serie": "241205"}

        self.assertEqual(build_valmer_unique_identifier(row), "M_BONOS_241205")

    def test_add_valmer_unique_identifier_matches_existing_shape(self):
        frame = pd.DataFrame(
            {
                "tipovalor": ["M", "LD"],
                "emisora": ["BONOS", "BONDESD"],
                "serie": ["241205", "250101"],
                "extra": [1, 2],
            }
        )

        out = add_valmer_unique_identifier(frame)

        self.assertEqual(out["unique_identifier"].tolist(), ["M_BONOS_241205", "LD_BONDESD_250101"])
        self.assertEqual(out["extra"].tolist(), [1, 2])

    def test_build_valmer_unique_identifier_rejects_missing_fields(self):
        with self.assertRaises(KeyError):
            build_valmer_unique_identifier({"tipovalor": "M", "emisora": "BONOS"})

    def test_build_valmer_unique_identifier_rejects_null_fields(self):
        with self.assertRaises(ValueError):
            build_valmer_unique_identifier({"tipovalor": "M", "emisora": pd.NA, "serie": "1"})

    def test_normalize_valmer_unique_identifiers_preserves_order(self):
        values = ["A", "B", "A", None, "C"]

        self.assertEqual(normalize_valmer_unique_identifiers(values), ["A", "B", "C"])

    def test_normalize_asset_type_map_rejects_conflicting_duplicates(self):
        with self.assertRaisesRegex(ValueError, "Conflicting asset_type"):
            _normalize_asset_type_map(
                [
                    ("M_BONOS_241205", ASSET_TYPE_BOND),
                    ("M_BONOS_241205", "future"),
                ]
            )

    def test_normalize_asset_type_map_requires_asset_type(self):
        with self.assertRaisesRegex(ValueError, "cannot be empty"):
            _normalize_asset_type_map({"M_BONOS_241205": " "})

    def test_internal_asset_table_upsert_requires_explicit_asset_type_map(self):
        bond_uid = uuid.uuid4()
        future_uid = uuid.uuid4()

        with patch("valmer_connectors.instruments.asset_identity.ensure_valmer_asset_runtime"):
            with patch.object(Asset, "_active_context", return_value=object()):
                with patch("msm.repositories.crud.bulk_upsert_model", return_value=object()) as bulk:
                    with patch(
                        "msm.api.base.operation_result_rows",
                        return_value=[
                            {
                                "uid": str(bond_uid),
                                "unique_identifier": "M_BONOS_241205",
                                "asset_type": ASSET_TYPE_BOND,
                            },
                            {
                                "uid": str(future_uid),
                                "unique_identifier": "F_MEXDER_240921",
                                "asset_type": "future",
                            },
                        ],
                    ):
                        assets = _upsert_asset_table_rows(
                            {
                                "M_BONOS_241205": ASSET_TYPE_BOND,
                                "F_MEXDER_240921": "future",
                            }
                        )

        self.assertEqual(
            bulk.call_args.kwargs["values"],
            [
                {
                    "unique_identifier": "M_BONOS_241205",
                    "asset_type": ASSET_TYPE_BOND,
                },
                {
                    "unique_identifier": "F_MEXDER_240921",
                    "asset_type": "future",
                },
            ],
        )
        self.assertEqual(assets["M_BONOS_241205"].uid, bond_uid)
        self.assertEqual(assets["F_MEXDER_240921"].asset_type, "future")

    def test_valmer_assets_are_registered_as_core_bonds(self):
        self.assertEqual(ASSET_TYPE_BOND, ASSET_TYPE_BOND_DEFINITION.asset_type)

    def test_asset_schema_guard_uses_existing_markets_runtime(self):
        fake_runtime = Mock()

        with patch("msm.bootstrap.get_runtime", return_value=fake_runtime):
            ensure_valmer_asset_runtime()

        fake_runtime.table.assert_called_once()

    def test_asset_schema_guard_requires_project_bootstrap(self):
        with patch("valmer_connectors.instruments.asset_identity._RUNTIME_READY", False):
            with patch("msm.bootstrap.get_runtime", side_effect=RuntimeError("missing")):
                with self.assertRaisesRegex(RuntimeError, "bootstrap_runtime"):
                    ensure_valmer_asset_runtime()

    def test_asset_ref_resolver_uses_one_set_based_row_call_per_batch(self):
        asset_uid = uuid.uuid4()
        asset = Asset(uid=asset_uid, unique_identifier="M_BONOS_241205", asset_type=ASSET_TYPE_BOND)

        with patch("valmer_connectors.instruments.asset_identity.ensure_valmer_asset_runtime"):
            with patch.object(Asset, "filter", return_value=[asset]) as asset_filter:
                with patch(
                    "msm.repositories.base.compile_markets_statement",
                    side_effect=AssertionError("Markets tables are read with row calls"),
                ):
                    refs = resolve_valmer_asset_refs(["M_BONOS_241205", "MISSING_1"])

        asset_filter.assert_called_once_with(
            unique_identifier__in=["M_BONOS_241205", "MISSING_1"],
            limit=2,
        )
        self.assertEqual(
            refs,
            {
                "M_BONOS_241205": ValmerAssetRef(
                    unique_identifier="M_BONOS_241205",
                    uid=asset_uid,
                    asset_type=ASSET_TYPE_BOND,
                )
            },
        )

    def test_asset_uid_resolver_returns_only_uid_map(self):
        asset_uid = uuid.uuid4()
        asset = Asset(uid=asset_uid, unique_identifier="M_BONOS_241205", asset_type=ASSET_TYPE_BOND)

        with patch("valmer_connectors.instruments.asset_identity.ensure_valmer_asset_runtime"):
            with patch.object(Asset, "filter", return_value=[asset]) as asset_filter:
                uid_map = resolve_valmer_asset_uids(["M_BONOS_241205"], batch_size=1)

        asset_filter.assert_called_once_with(unique_identifier__in=["M_BONOS_241205"], limit=1)
        self.assertEqual(uid_map, {"M_BONOS_241205": asset_uid})

    def test_asset_runtime_guard_needs_no_runtime_under_the_markets_api(self):
        with patch("valmer_connectors.instruments.asset_identity._RUNTIME_READY", False):
            with patch("valmer_connectors.runtime.markets_api_selected", return_value=True):
                with patch("msm.bootstrap.get_runtime", side_effect=RuntimeError("missing")):
                    ensure_valmer_asset_runtime()

    def test_typed_asset_resolver_reuses_projection_refs(self):
        asset_uid = uuid.uuid4()

        with patch(
            "valmer_connectors.instruments.asset_identity.resolve_valmer_asset_refs",
            return_value={
                "M_BONOS_241205": ValmerAssetRef(
                    unique_identifier="M_BONOS_241205",
                    uid=asset_uid,
                    asset_type=ASSET_TYPE_BOND,
                )
            },
        ):
            assets = resolve_valmer_assets(["M_BONOS_241205"])

        self.assertEqual(assets["M_BONOS_241205"].uid, asset_uid)
        self.assertEqual(assets["M_BONOS_241205"].asset_type, ASSET_TYPE_BOND)


if __name__ == "__main__":
    unittest.main()
