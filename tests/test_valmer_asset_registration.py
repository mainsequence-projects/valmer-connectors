import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd
from msm.constants import ASSET_TYPE_BOND

from valmer_connectors.assets.registration import (
    classify_valmer_asset_type,
    register_valmer_assets_from_rows,
)


def _source_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "tipovalor": "M",
                "emisora": "BONOS",
                "serie": "341123",
                "fecha": "2026-06-10",
                "nombrecompleto": "BONOS 10% 341123",
                "fechaemision": "2024-11-23",
                "tasacupon": 10.0,
            },
            {
                "tipovalor": "BI",
                "emisora": "CETES",
                "serie": "260910",
                "fecha": "2026-06-10",
                "nombrecompleto": "CETES 260910",
                "fechaemision": "2026-06-11",
                "tasacupon": 0.0,
            },
            {
                "tipovalor": "F",
                "emisora": "MEXDER",
                "serie": "240921",
                "fecha": "2026-06-10",
                "nombrecompleto": "Unsupported future",
            },
        ]
    )


class ValmerAssetRegistrationTests(unittest.TestCase):
    def test_default_classifier_marks_supported_bonds_only(self):
        self.assertEqual(
            classify_valmer_asset_type(
                {"tipovalor": "M", "emisora": "BONOS", "tasacupon": 10}
            ),
            ASSET_TYPE_BOND,
        )
        self.assertEqual(
            classify_valmer_asset_type({"tipovalor": "BI", "emisora": "CETES"}),
            ASSET_TYPE_BOND,
        )
        self.assertIsNone(
            classify_valmer_asset_type({"tipovalor": "F", "emisora": "MEXDER"})
        )

    def test_register_valmer_assets_from_rows_runs_batched_workflow(self):
        existing_cete = SimpleNamespace(
            uid=uuid.uuid4(),
            unique_identifier="BI_CETES_260910",
            asset_type=ASSET_TYPE_BOND,
        )
        new_bono = SimpleNamespace(
            uid=uuid.uuid4(),
            unique_identifier="M_BONOS_341123",
            asset_type=ASSET_TYPE_BOND,
        )
        cete_ref = SimpleNamespace(
            asset_type=ASSET_TYPE_BOND,
            as_asset=lambda: existing_cete,
        )
        logger = Mock()

        with patch(
            "valmer_connectors.assets.registration.resolve_valmer_asset_refs",
            return_value={"BI_CETES_260910": cete_ref},
        ) as resolve_refs:
            with patch(
                "valmer_connectors.assets.registration._upsert_asset_table_rows",
                return_value={"M_BONOS_341123": new_bono},
            ) as upsert_assets:
                with patch(
                    "valmer_connectors.assets.registration.upsert_valmer_asset_details",
                    return_value={
                        "M_BONOS_341123": {},
                        "BI_CETES_260910": {},
                    },
                ) as upsert_details:
                    with patch(
                        "valmer_connectors.assets.registration._publish_valmer_asset_snapshots",
                        return_value=2,
                    ) as publish_snapshots:
                        with patch(
                            "valmer_connectors.assets.registration._build_pricing_detail_map",
                            return_value=(
                                {
                                    "M_BONOS_341123": {"instrument": object()},
                                    "BI_CETES_260910": {"instrument": object()},
                                },
                                {},
                            ),
                        ):
                            with patch(
                                "valmer_connectors.assets.registration."
                                "_persist_valmer_pricing_details_batch",
                                return_value=[
                                    "M_BONOS_341123",
                                    "BI_CETES_260910",
                                ],
                            ) as persist_pricing:
                                result = register_valmer_assets_from_rows(
                                    _source_frame(),
                                    batch_size=500,
                                    logger=logger,
                                )

        resolve_refs.assert_called_once_with(
            ["M_BONOS_341123", "BI_CETES_260910"],
            batch_size=500,
            logger=logger,
        )
        upsert_assets.assert_called_once_with(
            {"M_BONOS_341123": ASSET_TYPE_BOND},
            batch_size=500,
            logger=logger,
        )
        upsert_details.assert_called_once()
        publish_snapshots.assert_called_once()
        persist_pricing.assert_called_once()
        self.assertEqual(
            set(result.assets_by_identifier),
            {"M_BONOS_341123", "BI_CETES_260910"},
        )
        self.assertEqual(result.details_upserted_count, 2)
        self.assertEqual(result.snapshots_published_count, 2)
        self.assertEqual(result.pricing_details_persisted_count, 2)
        self.assertEqual(result.skipped_unsupported, ["F_MEXDER_240921"])

    def test_register_valmer_assets_requires_snapshot_name_when_publishing(self):
        rows = _source_frame()
        rows.loc[0, "nombrecompleto"] = None

        with self.assertRaisesRegex(ValueError, "Valmer asset snapshots"):
            register_valmer_assets_from_rows(
                rows,
                include_pricing_details=False,
                publish_snapshots=True,
            )

    def test_register_valmer_assets_raises_on_asset_type_conflict(self):
        conflicting_ref = SimpleNamespace(
            asset_type="future",
            as_asset=lambda: SimpleNamespace(
                uid=uuid.uuid4(),
                unique_identifier="M_BONOS_341123",
                asset_type="future",
            ),
        )

        with patch(
            "valmer_connectors.assets.registration.resolve_valmer_asset_refs",
            return_value={"M_BONOS_341123": conflicting_ref},
        ):
            with patch(
                "valmer_connectors.assets.registration._upsert_asset_table_rows"
            ) as upsert_assets:
                with self.assertRaisesRegex(RuntimeError, "asset type conflict"):
                    register_valmer_assets_from_rows(
                        _source_frame().iloc[[0]].copy(),
                        include_pricing_details=False,
                        publish_snapshots=False,
                    )

        upsert_assets.assert_not_called()


if __name__ == "__main__":
    unittest.main()
