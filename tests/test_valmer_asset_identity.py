import unittest

import pandas as pd
from msm.constants import ASSET_TYPE_BOND, ASSET_TYPE_BOND_DEFINITION

from src.instruments.asset_identity import (
    add_valmer_unique_identifier,
    build_valmer_unique_identifier,
    normalize_valmer_unique_identifiers,
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


if __name__ == "__main__":
    unittest.main()
