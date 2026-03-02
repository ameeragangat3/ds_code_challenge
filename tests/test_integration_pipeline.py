"""Integration tests spanning multiple pipeline modules."""

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import h3
import pandas as pd

from src.data_transformation import assign_requests_to_hex
from src.data_transformation_extended import (
    anonymise_dataset,
    apply_k_anonymity_filter,
    subsample_sr_hex,
)
from src.ds_code_challenge_pipeline import run_pipeline


class TestPipelineIntegration(unittest.TestCase):
    """Integration coverage for orchestration and local data flow."""

    def test_run_pipeline_creates_expected_output_files(self) -> None:
        """Pipeline run should generate expected output artifacts with mocked I/O."""
        hex_features = [{"properties": {"index": "881f146257fffff"}}]

        computed_df = pd.DataFrame(
            [
                {"notification_number": 1, "computed_h3_index": "881f146257fffff"},
                {"notification_number": 2, "computed_h3_index": "881f146257fffff"},
                {"notification_number": 3, "computed_h3_index": "881f146257fffff"},
                {"notification_number": 4, "computed_h3_index": "881f146257fffff"},
            ]
        )
        reference_df = pd.DataFrame(
            [
                {"notification_number": 1, "h3_level8_index": "881f146257fffff"},
                {"notification_number": 2, "h3_level8_index": "881f146257fffff"},
                {"notification_number": 3, "h3_level8_index": "881f146257fffff"},
                {"notification_number": 4, "h3_level8_index": "881f146257fffff"},
            ]
        )
        subsample_df = pd.DataFrame(
            [
                {
                    "notification_number": 1,
                    "reference_number": "R1",
                    "latitude": -33.56,
                    "longitude": 18.49,
                    "distance_from_atlantis_km": 1.0,
                    "creation_timestamp": "2020-01-01 00:00:00",
                    "h3_level8_index": "cell_A",
                },
                {
                    "notification_number": 2,
                    "reference_number": "R2",
                    "latitude": -33.56,
                    "longitude": 18.50,
                    "distance_from_atlantis_km": 1.1,
                    "creation_timestamp": "2020-01-01 00:30:00",
                    "h3_level8_index": "cell_A",
                },
                {
                    "notification_number": 3,
                    "reference_number": "R3",
                    "latitude": -33.57,
                    "longitude": 18.51,
                    "distance_from_atlantis_km": 1.2,
                    "creation_timestamp": "2020-01-01 01:00:00",
                    "h3_level8_index": "cell_A",
                },
                {
                    "notification_number": 4,
                    "reference_number": "R4",
                    "latitude": -33.58,
                    "longitude": 18.52,
                    "distance_from_atlantis_km": 1.3,
                    "creation_timestamp": "2020-01-01 01:30:00",
                    "h3_level8_index": "cell_B",
                },
            ]
        )
        wind_df = pd.DataFrame(
            [
                {
                    "DateTime": pd.Timestamp("2020-01-01 00:00:00"),
                    "wind_direction_deg": 90.0,
                    "wind_speed_mps": 3.2,
                },
                {
                    "DateTime": pd.Timestamp("2020-01-01 01:00:00"),
                    "wind_direction_deg": 100.0,
                    "wind_speed_mps": 4.0,
                },
            ]
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            previous_cwd = os.getcwd()
            os.chdir(tmp_dir)
            try:
                with (
                    patch("src.ds_code_challenge_pipeline.load_config") as mock_config,
                    patch(
                        "src.ds_code_challenge_pipeline.extract_resolution_8_hexes_s3"
                    ) as mock_extract,
                    patch("src.ds_code_challenge_pipeline.download_aws_file"),
                    patch("src.ds_code_challenge_pipeline.validate_against_reference"),
                    patch("src.ds_code_challenge_pipeline.validate_hex_schema") as mock_schema,
                    patch("src.ds_code_challenge_pipeline.assign_requests_to_hex") as mock_assign,
                    patch("src.ds_code_challenge_pipeline.get_atlantis_centroid") as mock_centroid,
                    patch("src.ds_code_challenge_pipeline.subsample_sr_hex") as mock_subsample,
                    patch("src.ds_code_challenge_pipeline.load_wind_data") as mock_wind,
                    patch("src.ds_code_challenge_pipeline.pd.read_csv") as mock_read_csv,
                ):
                    mock_config.return_value = {
                        "validation": {"conformance_threshold": 0.95},
                        "spatial_assignment": {"failure_threshold": 0.1},
                        "subsample": {"radius_km": 1.852},
                        "wind_enrichment": {"max_retries": 3, "backoff_factor": 2},
                    }
                    mock_extract.return_value = (hex_features, 0.01)
                    mock_schema.return_value = {
                        "total_features": 1,
                        "total_rule_checks": 10,
                        "total_rule_failures": 0,
                        "conformance_score": 1.0,
                        "failure_breakdown": {},
                        "runtime_seconds": 0.01,
                    }
                    mock_assign.return_value = {
                        "dataframe": computed_df,
                        "total_records": 4,
                        "failure_count": 0,
                        "failure_rate": 0.0,
                    }
                    mock_read_csv.return_value = reference_df
                    mock_centroid.return_value = (-33.56, 18.49)
                    mock_subsample.return_value = subsample_df
                    mock_wind.return_value = wind_df

                    run_pipeline()

                output_dir = Path("output")
                self.assertTrue((output_dir / "full_dataset_manual_review.csv").exists())
                self.assertTrue((output_dir / "removed_columns_manual_review.csv").exists())
                self.assertTrue((output_dir / "final_anonymised_dataset.csv").exists())
                self.assertTrue(
                    (output_dir / "manual_review_high_risk_records.csv").exists()
                )

                final_df = pd.read_csv(output_dir / "final_anonymised_dataset.csv")
                review_df = pd.read_csv(output_dir / "manual_review_high_risk_records.csv")

                self.assertEqual(set(final_df["h3_level8_index"]), {"cell_A"})
                self.assertEqual(set(review_df["h3_level8_index"]), {"cell_B"})
            finally:
                os.chdir(previous_cwd)

    def test_local_data_flow_integration_assign_subsample_anonymise(self) -> None:
        """Local flow should integrate assignment, subsampling, and anonymisation."""
        lat_a, lon_a = -33.9249, 18.4241
        lat_b, lon_b = -33.0, 19.0
        valid_hex = h3.latlng_to_cell(lat_a, lon_a, 8)

        sr_df = pd.DataFrame(
            [
                {
                    "notification_number": 1,
                    "latitude": lat_a,
                    "longitude": lon_a,
                    "reference_number": "A",
                    "creation_timestamp": "2020-01-01 01:00:00",
                    "h3_level8_index": valid_hex,
                },
                {
                    "notification_number": 2,
                    "latitude": lat_a,
                    "longitude": lon_a,
                    "reference_number": "B",
                    "creation_timestamp": "2020-01-01 02:00:00",
                    "h3_level8_index": valid_hex,
                },
                {
                    "notification_number": 3,
                    "latitude": lat_a,
                    "longitude": lon_a,
                    "reference_number": "C",
                    "creation_timestamp": "2020-01-01 03:00:00",
                    "h3_level8_index": valid_hex,
                },
                {
                    "notification_number": 4,
                    "latitude": lat_b,
                    "longitude": lon_b,
                    "reference_number": "D",
                    "creation_timestamp": "2020-01-01 04:00:00",
                    "h3_level8_index": "other_cell",
                },
            ]
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            sr_path = Path(tmp_dir) / "sr.csv.gz"
            sr_hex_path = Path(tmp_dir) / "sr_hex.csv.gz"
            review_path = Path(tmp_dir) / "removed_columns_manual_review.csv"

            sr_df[["notification_number", "latitude", "longitude"]].to_csv(
                sr_path,
                index=False,
                compression="gzip",
            )
            sr_df.to_csv(sr_hex_path, index=False, compression="gzip")

            assignment = assign_requests_to_hex(
                sr_path=str(sr_path),
                hex_features=[{"properties": {"index": valid_hex}}],
            )
            self.assertEqual(assignment["failure_count"], 1)

            subsample = subsample_sr_hex(
                sr_hex_path=str(sr_hex_path),
                atlantis_lat=lat_a,
                atlantis_lon=lon_a,
                radius_km=2.0,
            )
            self.assertEqual(len(subsample), 3)

            subsample["creation_timestamp"] = pd.to_datetime(
                subsample["creation_timestamp"]
            )
            anonymised = anonymise_dataset(subsample, review_output_path=str(review_path))
            self.assertTrue(review_path.exists())
            self.assertNotIn("latitude", anonymised.columns)

            final_df, review_df = apply_k_anonymity_filter(
                anonymised,
                spatial_col="h3_level8_index",
                time_col="creation_timestamp",
                k=3,
            )
            self.assertEqual(len(final_df), 3)
            self.assertEqual(len(review_df), 0)


if __name__ == "__main__":
    unittest.main()
