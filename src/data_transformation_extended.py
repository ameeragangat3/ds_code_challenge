#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Extended transformations for subsampling, wind enrichment, and anonymisation."""

import json
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests
from shapely.geometry import shape

from src import (
    ATLANTIS_BOUNDARY_CACHE_PATH,
    OVERPASS_URL,
    WIND_CACHE_PATH,
    WIND_URL,
)
from src.data_transformation import load_config
from src.logging_config import setup_logging

logger = setup_logging()


def download_atlantis_boundary() -> dict[str, Any]:
    """Download Atlantis suburb boundary from OpenStreetMap via Overpass."""
    query = """
    [out:json];
    area["name"="South Africa"]->.searchArea;
    (
      relation(area.searchArea)["name"="Atlantis"];
      way(area.searchArea)["name"="Atlantis"];
    );
    out geom;
    """

    response = requests.post(OVERPASS_URL, data={"data": query}, timeout=60)
    response.raise_for_status()
    data = response.json()

    elements = data.get("elements", [])
    if not elements:
        raise ValueError("Atlantis boundary not found in Overpass response.")

    chosen: dict[str, Any] | None = None
    for element in elements:
        if "members" in element or "geometry" in element:
            chosen = element
            break

    if not chosen:
        raise ValueError("No valid geometry found for Atlantis.")

    coordinates: list[list[tuple[float, float]]] = []
    if "members" in chosen:
        for member in chosen.get("members", []):
            if "geometry" in member:
                coords = [(point["lon"], point["lat"]) for point in member["geometry"]]
                coordinates.append(coords)
    elif "geometry" in chosen:
        coords = [(point["lon"], point["lat"]) for point in chosen["geometry"]]
        coordinates.append(coords)

    geojson = {
        "type": "Feature",
        "geometry": {
            "type": "MultiPolygon",
            "coordinates": [coordinates],
        },
        "properties": {"name": "Atlantis"},
    }

    ATLANTIS_BOUNDARY_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with ATLANTIS_BOUNDARY_CACHE_PATH.open("w", encoding="utf-8") as file_obj:
        json.dump(geojson, file_obj)

    return geojson


def load_atlantis_boundary() -> dict[str, Any]:
    """Load cached Atlantis boundary data or download it if absent."""
    if ATLANTIS_BOUNDARY_CACHE_PATH.exists():
        with ATLANTIS_BOUNDARY_CACHE_PATH.open("r", encoding="utf-8") as file_obj:
            return json.load(file_obj)

    return download_atlantis_boundary()


def get_atlantis_centroid() -> tuple[float, float]:
    """Compute and return Atlantis centroid as ``(latitude, longitude)``."""
    geojson = load_atlantis_boundary()
    polygon = shape(geojson["geometry"])
    centroid = polygon.centroid
    return centroid.y, centroid.x


def download_wind_data(max_retries: int = 3, backoff_factor: int = 2) -> Path:
    """Download wind data with retry and exponential backoff.

    Args:
        max_retries: Maximum retry attempts.
        backoff_factor: Exponential base for backoff wait time.

    Returns:
        Path to cached local wind dataset.
    """
    if WIND_CACHE_PATH.exists():
        logger.info("Wind data already downloaded in %s", WIND_CACHE_PATH)
        return WIND_CACHE_PATH

    attempt = 0
    while attempt < max_retries:
        try:
            response = requests.get(WIND_URL, timeout=30)
            response.raise_for_status()

            WIND_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
            with WIND_CACHE_PATH.open("wb") as file_obj:
                file_obj.write(response.content)

            logger.info("Wind data successfully downloaded and saved in %s", WIND_CACHE_PATH)
            return WIND_CACHE_PATH
        except Exception as exc:
            attempt += 1
            wait_time = backoff_factor ** attempt
            logger.info(
                "%s\nWind download failed (attempt %s). Retrying in %ss...",
                exc,
                attempt,
                wait_time,
            )
            time.sleep(wait_time)

    raise RuntimeError("Failed to download wind data after retries.")


def load_wind_data(max_retries: int = 3, backoff_factor: int = 2) -> pd.DataFrame:
    """Load and clean Atlantis wind measurements from the ODS source file."""
    path = download_wind_data(max_retries=max_retries, backoff_factor=backoff_factor)
    df = pd.read_excel(path, engine="odf", header=None)

    station_row = df.iloc[2]
    variable_row = df.iloc[3]
    columns: list[str] = []

    for idx in range(len(df.columns)):
        if idx == 0:
            columns.append("DateTime")
            continue

        station = str(station_row[idx]).strip()
        variable = str(variable_row[idx]).strip()
        columns.append(f"{station}_{variable}")

    df.columns = columns
    df = df.iloc[5:].reset_index(drop=True)

    direction_col = "Atlantis AQM Site_Wind Dir V"
    speed_col = "Atlantis AQM Site_Wind Speed V"
    if direction_col not in df.columns or speed_col not in df.columns:
        raise ValueError("Could not detect Atlantis wind columns.")

    df = df[["DateTime", direction_col, speed_col]].rename(
        columns={
            direction_col: "wind_direction_deg",
            speed_col: "wind_speed_mps",
        }
    )

    # Keep only rows that resemble the expected date string format.
    df = df[df["DateTime"].astype(str).str.contains(r"\d{2}/\d{2}/\d{4}")]

    df["DateTime"] = pd.to_datetime(
        df["DateTime"],
        format="%d/%m/%Y %H:%M",
        errors="coerce",
    )
    df = df.dropna(subset=["DateTime"])

    df["wind_direction_deg"] = pd.to_numeric(df["wind_direction_deg"], errors="coerce")
    df["wind_speed_mps"] = pd.to_numeric(df["wind_speed_mps"], errors="coerce")
    return df


def anonymise_dataset(
    df: pd.DataFrame,
    review_output_path: str = "data/manual_review.csv",
) -> pd.DataFrame:
    """Anonymise a dataframe while preserving coarse spatial and temporal utility.

    Args:
        df: Input dataframe.
        review_output_path: Path for exporting removed sensitive columns.

    Returns:
        Dataframe with sensitive fields removed and timestamps rounded to 6 hours.
    """
    anonymised_df = df.copy()

    sensitive_columns = [
        "notification_number",
        "reference_number",
        "latitude",
        "longitude",
        "distance_from_atlantis_km",
    ]
    sensitive_existing = [col for col in sensitive_columns if col in anonymised_df.columns]

    if sensitive_existing:
        manual_review_df = anonymised_df[sensitive_existing].copy()
        Path(review_output_path).parent.mkdir(parents=True, exist_ok=True)
        manual_review_df.to_csv(review_output_path, index=False)

    anonymised_df = anonymised_df.drop(columns=sensitive_existing)

    if "creation_timestamp" in anonymised_df.columns:
        anonymised_df["creation_timestamp"] = anonymised_df["creation_timestamp"].dt.floor("6h")

    return anonymised_df


def haversine_distance(
    lat1: Any,
    lon1: Any,
    lat2: Any,
    lon2: Any,
) -> Any:
    """Calculate great-circle distance in kilometers using the Haversine formula."""
    earth_radius_km = 6371

    lat1_rad = np.radians(lat1)
    lon1_rad = np.radians(lon1)
    lat2_rad = np.radians(lat2)
    lon2_rad = np.radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = (
        np.sin(dlat / 2) ** 2
        + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2) ** 2
    )
    c = 2 * np.arcsin(np.sqrt(a))
    return earth_radius_km * c


def subsample_sr_hex(
    sr_hex_path: str,
    atlantis_lat: float,
    atlantis_lon: float,
    radius_km: float,
) -> pd.DataFrame:
    """Subsample ``sr_hex`` records within a radius of Atlantis centroid."""
    df = pd.read_csv(sr_hex_path, compression="gzip")
    df["distance_from_atlantis_km"] = haversine_distance(
        df["latitude"],
        df["longitude"],
        atlantis_lat,
        atlantis_lon,
    )
    return df[df["distance_from_atlantis_km"] <= radius_km].copy()


def apply_k_anonymity_filter(
    df: pd.DataFrame,
    spatial_col: str,
    time_col: str,
    k: int = 3,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Enforce k-anonymity by spatial group size and split high-risk rows.

    Args:
        df: Input dataframe.
        spatial_col: Column used as the spatial quasi-identifier.
        time_col: Timestamp column required to exist in the dataset.
        k: Minimum group size to release records.

    Returns:
        ``(final_df, manual_review_df)`` after k-anonymity filtering.
    """
    logger.info("K-anonymity stage initiated | k=%s", k)

    if spatial_col not in df.columns:
        raise KeyError(f"Missing spatial column: {spatial_col}")
    if time_col not in df.columns:
        raise KeyError(f"Missing time column: {time_col}")

    group_counts = df.groupby([spatial_col]).size().reset_index(name="group_size")
    working_df = df.merge(group_counts, on=[spatial_col], how="left")

    unsafe_mask = working_df["group_size"] < k
    manual_review_df = working_df[unsafe_mask].copy()
    final_df = working_df[~unsafe_mask].copy()

    final_df = final_df.drop(columns=["group_size"])
    manual_review_df = manual_review_df.drop(columns=["group_size"])

    logger.info(
        "K-anonymity report | total_records=%s | released_records=%s | "
        "flagged_for_review=%s",
        len(working_df),
        len(final_df),
        len(manual_review_df),
    )
    return final_df, manual_review_df


def main() -> None:
    """Run extended transformation stage as a standalone script."""
    logger.info("Computing Atlantis centroid from OSM boundary")
    config = load_config("config.yaml")

    radius_km = config["subsample"]["radius_km"]
    max_retries = config["wind_enrichment"]["max_retries"]
    backoff_factor = config["wind_enrichment"]["backoff_factor"]

    atlantis_lat, atlantis_lon = get_atlantis_centroid()
    logger.info(
        "Derived Atlantis centroid | lat=%s | lon=%s",
        round(atlantis_lat, 6),
        round(atlantis_lon, 6),
    )

    logger.info("Atlantis subsampling stage initiated")
    subsample_df = subsample_sr_hex(
        sr_hex_path="data/sr_hex.csv.gz",
        atlantis_lat=atlantis_lat,
        atlantis_lon=atlantis_lon,
        radius_km=radius_km,
    )
    logger.info(
        "Atlantis subsample | radius_km=%s | subsample_count=%s",
        radius_km,
        len(subsample_df),
    )

    logger.info("Loading wind dataset")
    wind_df = load_wind_data(max_retries=max_retries, backoff_factor=backoff_factor)
    logger.info("Wind shape: %s", wind_df.shape)
    logger.info("Wind columns: %s", list(wind_df.columns))
    logger.info("Wind cleaned preview:\n%s", wind_df.head())

    logger.info("Wind enrichment stage initiated")
    subsample_df["creation_timestamp"] = pd.to_datetime(
        subsample_df["creation_timestamp"], errors="coerce"
    )
    subsample_df["creation_timestamp"] = subsample_df["creation_timestamp"].dt.tz_localize(
        None
    )

    subsample_df = subsample_df.sort_values("creation_timestamp")
    wind_df = wind_df.sort_values("DateTime")

    enriched_df = pd.merge_asof(
        subsample_df,
        wind_df,
        left_on="creation_timestamp",
        right_on="DateTime",
        direction="nearest",
        tolerance=pd.Timedelta("1h"),
    ).rename(columns={"DateTime": "wind_timestamp"})

    logger.info("Wind enrichment completed | enriched_records=%s", len(enriched_df))

    logger.info("Anonymisation stage initiated")
    output_dir = Path("output")
    output_dir.mkdir(parents=True, exist_ok=True)

    enriched_df.to_csv(f"{output_dir}/full_dataset_manual_review.csv", index=False)

    anonymised_df = anonymise_dataset(
        enriched_df,
        review_output_path=f"{output_dir}/removed_columns_manual_review.csv",
    )
    logger.info("Manual review file created at %s", output_dir / "removed_columns_manual_review.csv")
    logger.info("Anonymisation completed | final_columns=%s", list(anonymised_df.columns))

    final_df, manual_review_df = apply_k_anonymity_filter(
        anonymised_df,
        spatial_col="h3_level8_index",
        time_col="creation_timestamp",
        k=3,
    )

    final_df.to_csv(f"{output_dir}/final_anonymised_dataset.csv", index=False)
    manual_review_df.to_csv(
        f"{output_dir}/manual_review_high_risk_records.csv",
        index=False,
    )

    logger.info("Anonymisation completed and files exported")


if __name__ == "__main__":
    main()
