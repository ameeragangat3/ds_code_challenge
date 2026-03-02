"""Unit tests for spatial assignment transformation."""

import tempfile
import unittest
from pathlib import Path

import h3
import pandas as pd

from src.data_transformation import assign_requests_to_hex


class TestSpatialAssignment(unittest.TestCase):
    """Tests for assigning service requests to H3 indices."""

    def test_assign_requests_to_hex_handles_valid_missing_and_out_of_bounds(self) -> None:
        """Assignment should handle valid, missing, and invalid geolocations."""
        lat_valid, lon_valid = -33.9249, 18.4241
        valid_hex = h3.latlng_to_cell(lat_valid, lon_valid, 8)

        sr_df = pd.DataFrame(
            [
                {
                    "notification_number": 1,
                    "latitude": lat_valid,
                    "longitude": lon_valid,
                },
                {
                    "notification_number": 2,
                    "latitude": None,
                    "longitude": lon_valid,
                },
                {
                    "notification_number": 3,
                    "latitude": -33.0,
                    "longitude": 19.0,
                },
            ]
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            sr_path = Path(tmp_dir) / "sr.csv.gz"
            sr_df.to_csv(sr_path, index=False, compression="gzip")

            result = assign_requests_to_hex(
                sr_path=str(sr_path),
                hex_features=[{"properties": {"index": valid_hex}}],
            )

        out = result["dataframe"]
        self.assertEqual(result["total_records"], 3)
        self.assertEqual(result["failure_count"], 1)
        self.assertEqual(result["failure_rate"], 0.33333)
        self.assertEqual(
            out.loc[out["notification_number"] == 1, "computed_h3_index"].iloc[0],
            valid_hex,
        )
        self.assertEqual(
            out.loc[out["notification_number"] == 2, "computed_h3_index"].iloc[0],
            0,
        )


if __name__ == "__main__":
    unittest.main()
