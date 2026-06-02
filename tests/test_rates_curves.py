import unittest

from valmer_connectors.instruments.rates_curves import build_tiie_curve_frame_from_csv


class ValmerRatesCurvesTests(unittest.TestCase):
    def test_build_tiie_curve_frame_returns_discount_curve_builder_shape(self):
        content = (
            b"1,TIIE,240101,1,11.00\n"
            b"2,TIIE,240102,2,11.25\n"
            b"3,TIIE,240103,3,11.50\n"
        )

        frame = build_tiie_curve_frame_from_csv(
            content,
            curve_unique_identifier="VALMER_TIIE_28",
        )
        row = frame.reset_index().iloc[0]

        self.assertEqual(frame.index.names, ["time_index", "curve_unique_identifier"])
        self.assertEqual(row["curve_unique_identifier"], "VALMER_TIIE_28")
        self.assertEqual(row["curve"], {1: 0.11, 2: 0.1125, 3: 0.115})


if __name__ == "__main__":
    unittest.main()
