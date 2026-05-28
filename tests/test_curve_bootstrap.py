import unittest

from src.instruments.curve_bootstrap import (
    MEXICAN_REFERENCE_INDEX_DEFINITIONS,
    mexican_reference_index_payloads,
)


class ValmerCurveBootstrapTests(unittest.TestCase):
    def test_reference_index_definitions_cover_required_tiie_and_cete_indexes(self):
        identifiers = {
            definition.unique_identifier for definition in MEXICAN_REFERENCE_INDEX_DEFINITIONS
        }

        self.assertEqual(
            identifiers,
            {
                "TIIE_OVERNIGHT",
                "TIIE_28",
                "TIIE_91",
                "TIIE_182",
                "CETE_28",
                "CETE_91",
                "CETE_182",
            },
        )

    def test_reference_index_payloads_target_core_index_api(self):
        payload_by_identifier = {
            payload["unique_identifier"]: payload for payload in mexican_reference_index_payloads()
        }

        tiie_28 = payload_by_identifier["TIIE_28"]
        self.assertIsNone(tiie_28["provider"])
        self.assertEqual(tiie_28["metadata_json"]["index_family"], "TIIE")
        self.assertEqual(tiie_28["metadata_json"]["tenor_days"], 28)
        self.assertNotIn("source_system", tiie_28["metadata_json"])
        self.assertNotIn("legacy_constant_name", tiie_28["metadata_json"])


if __name__ == "__main__":
    unittest.main()
