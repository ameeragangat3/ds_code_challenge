#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Spatial assignment transformations for service request data."""

import sys
from typing import Any

import h3
import pandas as pd
import yaml

from src import SR_FILE, SR_HEX_FILE
from src.data_extract_validate import download_aws_file, extract_resolution_8_hexes_s3
from src.logging_config import setup_logging

logger = setup_logging()


def load_config(config_path: str) -> dict[str, Any]:
    """Load YAML configuration values.

    Args:
        config_path: Path to a YAML configuration file.

    Returns:
        Parsed configuration dictionary.
    """
    with open(config_path, "r", encoding="utf-8") as file_obj:
        return yaml.safe_load(file_obj)


def assign_requests_to_hex(
    sr_path: str,
    hex_features: list[dict[str, Any]],
) -> dict[str, Any]:
    """Assign each service request to an H3 resolution-8 index.

    Args:
        sr_path: Path to service request CSV (gzip compressed).
        hex_features: Resolution-8 polygon features used as valid index set.

    Returns:
        Assignment metrics with output dataframe and failure statistics.
    """
    df = pd.read_csv(sr_path, compression="gzip")

    valid_hex_indices = {
        feature["properties"]["index"]
        for feature in hex_features
    }

    def compute_h3(row: pd.Series) -> str | int:
        """Compute H3 index for one row, returning ``0`` for invalid geolocation."""
        lat = row["latitude"]
        lon = row["longitude"]

        if pd.isna(lat) or pd.isna(lon):
            return 0

        try:
            return h3.latlng_to_cell(lat, lon, 8)
        except Exception:
            return 0

    df["computed_h3_index"] = df.apply(compute_h3, axis=1)

    failure_mask = (
        (df["computed_h3_index"] != 0)
        & (~df["computed_h3_index"].isin(valid_hex_indices))
    )
    failure_count = int(failure_mask.sum())
    total_records = len(df)
    failure_rate = round(failure_count / total_records, 5) if total_records > 0 else 0

    return {
        "dataframe": df,
        "total_records": total_records,
        "failure_count": failure_count,
        "failure_rate": failure_rate,
    }


def main() -> None:
    """Execute standalone spatial assignment and validation flow."""
    logger.info("Spatial assignment stage initiated")
    config = load_config("config.yaml")
    spatial_failure_threshold = config["spatial_assignment"]["failure_threshold"]

    logger.info("Download service request file sr.csv.gz from AWS")
    download_aws_file(SR_FILE)
    hex_features, _ = extract_resolution_8_hexes_s3()

    assignment_result = assign_requests_to_hex(
        sr_path="data/sr.csv.gz",
        hex_features=hex_features,
    )

    logger.info(
        "Spatial assignment | total_records=%s | failure_rate=%s",
        assignment_result["total_records"],
        assignment_result["failure_rate"],
    )

    if assignment_result["failure_rate"] > spatial_failure_threshold:
        logger.error("Spatial assignment failed threshold")
        sys.exit(1)

    logger.info("Spatial assignment passed threshold")

    logger.info("Ground truth comparison stage initiated")
    logger.info("Download sr_hex.csv.gz file")
    download_aws_file(SR_HEX_FILE)

    df_hex = pd.read_csv("data/sr_hex.csv.gz", compression="gzip")
    df_computed = assignment_result["dataframe"].copy()

    df_computed["computed_h3_index"] = df_computed["computed_h3_index"].astype(str)
    df_hex["h3_level8_index"] = df_hex["h3_level8_index"].astype(str)

    merged = df_computed.merge(
        df_hex[["notification_number", "h3_level8_index"]],
        on="notification_number",
        how="inner",
    )

    match_rate = (merged["computed_h3_index"] == merged["h3_level8_index"]).mean()
    logger.info("H3 comparison match_rate=%s", round(match_rate, 5))


if __name__ == "__main__":
    main()
