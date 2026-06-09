import uuid
import unittest
from unittest.mock import Mock, patch

import pandas as pd
from msm.api.assets import Asset
from msm.constants import ASSET_TYPE_BOND, ASSET_TYPE_BOND_DEFINITION

from valmer_connectors.instruments.asset_identity import (
    ValmerAssetRef,
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

    def test_asset_ref_resolver_uses_projection_query(self):
        asset_uid = uuid.uuid4()

        with patch("valmer_connectors.instruments.asset_identity.ensure_valmer_asset_runtime"):
            with patch.object(Asset, "_active_context", return_value=object()):
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
                                        "unique_identifier": "M_BONOS_241205",
                                        "uid": str(asset_uid),
                                        "asset_type": ASSET_TYPE_BOND,
                                    }
                                ]
                            },
                        ):
                            refs = resolve_valmer_asset_refs(["M_BONOS_241205"])

        self.assertEqual(
            refs["M_BONOS_241205"],
            ValmerAssetRef(
                unique_identifier="M_BONOS_241205",
                uid=asset_uid,
                asset_type=ASSET_TYPE_BOND,
            ),
        )

    def test_asset_uid_resolver_returns_only_uid_map(self):
        asset_uid = uuid.uuid4()

        with patch("valmer_connectors.instruments.asset_identity.ensure_valmer_asset_runtime"):
            with patch.object(Asset, "_active_context", return_value=object()):
                with patch(
                    "msm.repositories.base.compile_markets_statement",
                    return_value=object(),
                ):
                    with patch(
                        "msm.repositories.base.execute_markets_operation",
                        return_value={
                            "rows": [
                                {
                                    "unique_identifier": "M_BONOS_241205",
                                    "uid": str(asset_uid),
                                }
                            ]
                        },
                    ):
                        uid_map = resolve_valmer_asset_uids(["M_BONOS_241205"])

        self.assertEqual(uid_map, {"M_BONOS_241205": asset_uid})

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
