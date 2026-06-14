import datetime as dt
import unittest
import uuid
from unittest.mock import patch

import pandas as pd
from msm_pricing.instruments.base_instrument import InstrumentModel
from msm_pricing.valuation import ValuationPosition

from valmer_connectors.instruments import vector_to_asset


class FakeBondInstrument(InstrumentModel):
    face_value: float = 100.0

    def price(self) -> float:
        return 101.25


class VectorToAssetValuationTests(unittest.TestCase):
    def test_build_valuation_position_from_sheet_returns_transient_basket(self):
        asset_uid = uuid.uuid4()
        instrument = FakeBondInstrument(face_value=100.0)
        price_check_frame = pd.DataFrame(
            [
                {
                    "instrument_hash": "instrument-1",
                    "FECHA": dt.datetime(2026, 6, 10, tzinfo=dt.UTC),
                    "UID": "M_BONOS_341123",
                    "SUBYACENTE": "Bonos M",
                }
            ]
        )
        source_frame = pd.DataFrame(
            [
                {
                    "subyacente": "Bonos M",
                    "monedaemision": "MPS",
                }
            ]
        )

        with (
            patch.object(vector_to_asset.pd, "read_excel", return_value=source_frame),
            patch.object(
                vector_to_asset,
                "run_price_check",
                return_value=(
                    price_check_frame,
                    {
                        "instrument-1": {
                            "instrument": instrument,
                            "extra_market_info": {"yield": 0.1},
                        }
                    },
                ),
            ),
            patch.object(
                vector_to_asset,
                "resolve_valmer_asset_uids",
                return_value={"M_BONOS_341123": asset_uid},
            ),
        ):
            result = vector_to_asset.build_valuation_position_from_sheet(
                "dummy.xlsx",
                notional_per_line=1_000.0,
                publish_report_artifact=False,
            )

        self.assertIsInstance(result.valuation_position, ValuationPosition)
        self.assertEqual(result.report_artifact_uid, None)
        self.assertEqual(result.report_csv_path, None)
        self.assertEqual(len(result.valuation_position.lines), 1)
        line = result.valuation_position.lines[0]
        self.assertEqual(line.instrument, instrument)
        self.assertEqual(line.asset_uid, asset_uid)
        self.assertEqual(line.units, 10.0)
        self.assertEqual(
            result.valuation_position.valuation_date,
            dt.datetime(2026, 6, 10, tzinfo=dt.UTC),
        )
        self.assertEqual(line.metadata_json["valmer_unique_identifier"], "M_BONOS_341123")
        self.assertEqual(line.metadata_json["extra_market_info"], {"yield": 0.1})

    def test_build_valuation_position_rejects_multiple_valuation_dates(self):
        price_check_frame = pd.DataFrame(
            [
                {
                    "instrument_hash": "instrument-1",
                    "FECHA": dt.datetime(2026, 6, 10, tzinfo=dt.UTC),
                    "asset_uid": None,
                    "UID": "M_BONOS_341123",
                    "SUBYACENTE": "Bonos M",
                },
                {
                    "instrument_hash": "instrument-2",
                    "FECHA": dt.datetime(2026, 6, 11, tzinfo=dt.UTC),
                    "asset_uid": None,
                    "UID": "M_BONOS_341124",
                    "SUBYACENTE": "Bonos M",
                },
            ]
        )

        with self.assertRaisesRegex(ValueError, "exactly one valuation date"):
            vector_to_asset._build_valuation_position_from_price_check(
                price_check_frame,
                {
                    "instrument-1": {"instrument": FakeBondInstrument(face_value=100.0)},
                    "instrument-2": {"instrument": FakeBondInstrument(face_value=100.0)},
                },
                notional_per_line=1_000.0,
            )


if __name__ == "__main__":
    unittest.main()
