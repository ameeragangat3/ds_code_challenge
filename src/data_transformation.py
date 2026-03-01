#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#%%
import pandas as pd
import numpy as np
import h3
import yaml
import sys

from src.logging_config import setup_logging
logger = setup_logging()

from src.data_extract_validate import download_aws_file, extract_resolution_8_hexes_s3

SR_FILE = "sr.csv.gz"
SR_HEX_FILE = "sr_hex.csv.gz"

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


# Haversine Distance
def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate distance in kilometers between two lat/lon points.
    Supports vectorised numpy arrays.
    """

    R = 6371  # Earth radius in km

    lat1_rad = np.radians(lat1)
    lon1_rad = np.radians(lon1)
    lat2_rad = np.radians(lat2)
    lon2_rad = np.radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = (
        np.sin(dlat / 2) ** 2 +
        np.cos(lat1_rad) * np.cos(lat2_rad) *
        np.sin(dlon / 2) ** 2
    )

    c = 2 * np.arcsin(np.sqrt(a))

    return R * c


# Atlantis Subsampling
def subsample_atlantis(df, atlantis_lat, atlantis_lon, radius_km):
    """
    Subsample service requests within radius_km of Atlantis centre.
    """

    valid_mask = (
        (~df["latitude"].isna()) &
        (~df["longitude"].isna())
    )

    distances = np.full(len(df), np.nan)

    distances[valid_mask] = haversine_distance(
        df.loc[valid_mask, "latitude"],
        df.loc[valid_mask, "longitude"],
        atlantis_lat,
        atlantis_lon
    )

    df["distance_from_atlantis_km"] = distances

    subsample_df = df[df["distance_from_atlantis_km"] <= radius_km]

    return {
        "atlantis_lat": round(atlantis_lat, 6),
        "atlantis_lon": round(atlantis_lon, 6),
        "radius_km": radius_km,
        "total_records": len(df),
        "subsample_count": len(subsample_df)
    }


def subsample_sr_hex(sr_hex_path, atlantis_lat, atlantis_lon, radius_km):
    """
    Subsample sr_hex.csv.gz within specified radius of Atlantis centroid.
    """

    df = pd.read_csv(sr_hex_path, compression="gzip")

    df["distance_from_atlantis_km"] = haversine_distance(
        df["latitude"],
        df["longitude"],
        atlantis_lat,
        atlantis_lon
    )

    filtered_df = df[df["distance_from_atlantis_km"] <= radius_km].copy()

    return filtered_df

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
