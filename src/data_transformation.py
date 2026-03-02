#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#%%
import pandas as pd
import h3
import yaml
import sys

from src import (SR_FILE, SR_HEX_FILE)
from src.data_extract_validate import download_aws_file, extract_resolution_8_hexes_s3

from src.logging_config import setup_logging
logger = setup_logging()

def load_config(config_path: str):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)
    
def assign_requests_to_hex(sr_path, hex_features):
    """
    Assign each service request to an H3 resolution 8 hex index.
    """

    df = pd.read_csv(sr_path, compression="gzip")

    # Build lookup set of valid hex indices
    valid_hex_indices = {
        feature["properties"]["index"]
        for feature in hex_features
    }

    def compute_h3(row):
        lat = row["latitude"]
        lon = row["longitude"]

        if pd.isna(lat) or pd.isna(lon):
            return 0  # Spec: invalid geolocation → 0

        try:
            return h3.latlng_to_cell(lat, lon, 8)
        except Exception:
            return 0

    df["computed_h3_index"] = df.apply(compute_h3, axis=1)

    failure_mask = (
        (df["computed_h3_index"] != 0) &
        (~df["computed_h3_index"].isin(valid_hex_indices))
    )

    failure_count = int(failure_mask.sum())
    total_records = len(df)

    failure_rate = (
        round(failure_count / total_records, 5)
        if total_records > 0
        else 0
    )

    return {
        "dataframe": df,
        "total_records": total_records,
        "failure_count": failure_count,
        "failure_rate": failure_rate
    }

#%%
def main():
    logger.info("Spatial assignment stage initiated")
    config = load_config("config.yaml")
    spatial_failure_threshold = config["spatial_assignment"]["failure_threshold"]

    logger.info("Download service request file sr.csv.gz from AWS")
    download_aws_file(SR_FILE)
    hex_features, extract_runtime = extract_resolution_8_hexes_s3()

    assignment_result = assign_requests_to_hex(
        sr_path="data/sr.csv.gz",
        hex_features=hex_features
    )

    logger.info(
        f"Spatial assignment | "
        f"total_records={assignment_result['total_records']} | "
        f"failure_rate={assignment_result['failure_rate']}"
    )

    if assignment_result['failure_rate'] > spatial_failure_threshold:
        logger.error("Spatial assignment failed threshold")
        sys.exit(1)
    else: 
        logger.info("Spatial assignment passed threshold")
    
    # Ground Truth Comparison
    logger.info("Ground truth comparison stage initiated")
    logger.info("Download sr_hex.csv.gz file")    
    download_aws_file(SR_HEX_FILE)
    df_hex = pd.read_csv("data/sr_hex.csv.gz", compression="gzip")
    df_computed = assignment_result['dataframe'].copy()

    df_computed["computed_h3_index"] = df_computed["computed_h3_index"].astype(str)
    df_hex["h3_level8_index"] = df_hex["h3_level8_index"].astype(str)

    merged = df_computed.merge(
        df_hex[["notification_number", "h3_level8_index"]],
        on="notification_number",
        how="inner"
    )

    match_rate = (
        (merged["computed_h3_index"] == merged["h3_level8_index"]).mean()
    )

    logger.info(f"H3 comparison match_rate={round(match_rate, 5)}")

if __name__ == "__main__":
    main()
