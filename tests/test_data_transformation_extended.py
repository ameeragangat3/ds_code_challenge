"""Unit tests for extended transformation helpers."""

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.data_transformation_extended import (
    anonymise_dataset,
    apply_k_anonymity_filter,
    haversine_distance,
    subsample_sr_hex,
)


class TestDataTransformationExtended(unittest.TestCase):
    """Tests for distance, subsampling, and anonymisation helpers."""

    def test_haversine_distance_for_one_arc_minute_is_about_1_852_km(self) -> None:
        """One arc minute of latitude should be close to 1.852 km."""
        distance = haversine_distance(0.0, 0.0, 1.0 / 60.0, 0.0)
        self.assertAlmostEqual(distance, 1.852, places=2)

    def test_subsample_sr_hex_filters_by_radius(self) -> None:
        """Subsampling should keep only records within requested radius."""
        df = pd.DataFrame(
            [
                {"latitude": 0.0, "longitude": 0.0},
                {"latitude": 0.2, "longitude": 0.2},
            ]
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "sr_hex.csv.gz"
            df.to_csv(input_path, index=False, compression="gzip")

            filtered = subsample_sr_hex(
                sr_hex_path=str(input_path),
                atlantis_lat=0.0,
                atlantis_lon=0.0,
                radius_km=2.0,
            )

        self.assertEqual(len(filtered), 1)
        self.assertIn("distance_from_atlantis_km", filtered.columns)

    def test_anonymise_dataset_drops_sensitive_fields_and_rounds_time(self) -> None:
        """Anonymisation should remove sensitive columns and round timestamps."""
        df = pd.DataFrame(
            [
                {
                    "notification_number": 1001,
                    "reference_number": "REF-1",
                    "latitude": -33.9,
                    "longitude": 18.4,
                    "distance_from_atlantis_km": 1.2,
                    "creation_timestamp": pd.Timestamp("2020-01-01 05:59:00"),
                    "h3_level8_index": "881f146257fffff",
                }
            ]
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            review_path = Path(tmp_dir) / "removed_columns_manual_review.csv"
            anonymised = anonymise_dataset(df, review_output_path=str(review_path))
            self.assertTrue(review_path.exists())

        self.assertNotIn("notification_number", anonymised.columns)
        self.assertNotIn("reference_number", anonymised.columns)
        self.assertNotIn("latitude", anonymised.columns)
        self.assertNotIn("longitude", anonymised.columns)
        self.assertNotIn("distance_from_atlantis_km", anonymised.columns)
        self.assertEqual(
            anonymised["creation_timestamp"].iloc[0],
            pd.Timestamp("2020-01-01 00:00:00"),
        )

    def test_apply_k_anonymity_filter_uses_spatial_col_parameter(self) -> None:
        """K-anonymity should split safe and unsafe records by spatial group size."""
        df = pd.DataFrame(
            [
                {
                    "cell_id": "A",
                    "creation_timestamp": pd.Timestamp("2020-01-01 00:00:00"),
                },
                {
                    "cell_id": "A",
                    "creation_timestamp": pd.Timestamp("2020-01-01 01:00:00"),
                },
                {
                    "cell_id": "A",
                    "creation_timestamp": pd.Timestamp("2020-01-01 02:00:00"),
                },
                {
                    "cell_id": "B",
                    "creation_timestamp": pd.Timestamp("2020-01-01 03:00:00"),
                },
                {
                    "cell_id": "B",
                    "creation_timestamp": pd.Timestamp("2020-01-01 04:00:00"),
                },
            ]
        )

        final_df, review_df = apply_k_anonymity_filter(
            df=df,
            spatial_col="cell_id",
            time_col="creation_timestamp",
            k=3,
        )

        self.assertEqual(len(final_df), 3)
        self.assertEqual(len(review_df), 2)
        self.assertEqual(set(final_df["cell_id"]), {"A"})
        self.assertEqual(set(review_df["cell_id"]), {"B"})


if __name__ == "__main__":
    unittest.main()
