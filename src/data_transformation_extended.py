#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#%%
import requests
import json
from pathlib import Path
from shapely.geometry import shape
import pandas as pd
import time

from src.data_transformation import load_config, subsample_sr_hex

from src.logging_config import setup_logging
logger = setup_logging()

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
ATLANTIS_BOUNDARY_CACHE_PATH = Path("data/atlantis_boundary.geojson")

WIND_URL = "https://www.capetown.gov.za/_layouts/OpenDataPortalHandler/DownloadHandler.ashx?DocumentName=Wind_direction_and_speed_2020.ods&DatasetDocument=https%3A%2F%2Fcityapps.capetown.gov.za%2Fsites%2Fopendatacatalog%2FDocuments%2FWind%2FWind_direction_and_speed_2020.ods"

WIND_CACHE_PATH = Path("data/wind_2020.ods")

def download_atlantis_boundary():
    """
    Download Atlantis suburb boundary from OpenStreetMap via Overpass API.
    """

    query = """
    [out:json];
    area["name"="South Africa"]->.searchArea;
    (
      relation(area.searchArea)["name"="Atlantis"];
      way(area.searchArea)["name"="Atlantis"];
    );
    out geom;
    """

    response = requests.post(OVERPASS_URL, data={"data": query})
    response.raise_for_status()

    data = response.json()

    elements = data.get("elements", [])
    if not elements:
        raise ValueError("Atlantis boundary not found in Overpass response.")

    # Choose feature that is in Western Cape (~lat -33)
    chosen = None
    for element in elements:
        if "members" in element or "geometry" in element:
            chosen = element
            break

    if not chosen:
        raise ValueError("No valid geometry found for Atlantis.")

    coordinates = []

    if "members" in chosen:
        for member in chosen.get("members", []):
            if "geometry" in member:
                coords = [(p["lon"], p["lat"]) for p in member["geometry"]]
                coordinates.append(coords)
    elif "geometry" in chosen:
        coords = [(p["lon"], p["lat"]) for p in chosen["geometry"]]
        coordinates.append(coords)

    geojson = {
        "type": "Feature",
        "geometry": {
            "type": "MultiPolygon",
            "coordinates": [coordinates]
        },
        "properties": {
            "name": "Atlantis"
        }
    }

    ATLANTIS_BOUNDARY_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(ATLANTIS_BOUNDARY_CACHE_PATH, "w") as f:
        json.dump(geojson, f)

    return geojson


def load_atlantis_boundary():
    """
    Load cached boundary or download if not present.
    """

    if ATLANTIS_BOUNDARY_CACHE_PATH.exists():
        with open(ATLANTIS_BOUNDARY_CACHE_PATH, "r") as f:
            return json.load(f)

    return download_atlantis_boundary()


def get_atlantis_centroid():
    """
    Compute geometric centroid of Atlantis suburb boundary.
    """

    geojson = load_atlantis_boundary()

    polygon = shape(geojson["geometry"])
    centroid = polygon.centroid

    return centroid.y, centroid.x  # (lat, lon)

def download_wind_data(max_retries=3, backoff_factor=2):
    """
    Download wind data with retry and exponential backoff.
    """

    if WIND_CACHE_PATH.exists():
        return WIND_CACHE_PATH

    attempt = 0

    while attempt < max_retries:
        try:
            response = requests.get(WIND_URL, timeout=30)
            response.raise_for_status()

            WIND_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)

            with open(WIND_CACHE_PATH, "wb") as f:
                f.write(response.content)

            return WIND_CACHE_PATH

        except Exception as e:
            attempt += 1
            wait_time = backoff_factor ** attempt
            print(f"{e}\nWind download failed (attempt {attempt}). Retrying in {wait_time}s...")
            time.sleep(wait_time)

    raise RuntimeError("Failed to download wind data after retries.")


def load_wind_data(max_retries=3, backoff_factor=2):
    """
    Load and clean wind dataset for Atlantis AQM site.
    """

    path = download_wind_data(max_retries,backoff_factor)
    df = pd.read_excel(path, engine="odf", header=None)

    # Correct header rows
    station_row = df.iloc[2]
    variable_row = df.iloc[3]

    columns = []

    for i in range(len(df.columns)):
        if i == 0:
            columns.append("DateTime")
        else:
            station = str(station_row[i]).strip()
            variable = str(variable_row[i]).strip()
            col_name = f"{station}_{variable}"
            columns.append(col_name)

    df.columns = columns

    # Drop metadata rows (0–4)
    df = df.iloc[5:].reset_index(drop=True)

    # Identify Atlantis columns
    direction_col = "Atlantis AQM Site_Wind Dir V"
    speed_col = "Atlantis AQM Site_Wind Speed V"

    if direction_col not in df.columns or speed_col not in df.columns:
        raise ValueError("Could not detect Atlantis wind columns.")

    df = df[["DateTime", direction_col, speed_col]]

    df = df.rename(columns={
        direction_col: "wind_direction_deg",
        speed_col: "wind_speed_mps"
    })

    # Parse datetime
    # Keep only rows that look like datetime strings
    df = df[df["DateTime"].astype(str).str.contains(r"\d{2}/\d{2}/\d{4}")]
    
    # Convert safely
    df["DateTime"] = pd.to_datetime(
        df["DateTime"],
        format="%d/%m/%Y %H:%M",
        errors="coerce"
    )
    
    # Drop rows where conversion failed
    df = df.dropna(subset=["DateTime"])

    # Convert numeric
    df["wind_direction_deg"] = pd.to_numeric(df["wind_direction_deg"], errors="coerce")
    df["wind_speed_mps"] = pd.to_numeric(df["wind_speed_mps"], errors="coerce")

    return df

def anonymise_dataset(df: pd.DataFrame, review_output_path: str = "data/manual_review.csv"):
    """
    Anonymise dataset while preserving spatial precision (~500m via H3)
    and temporal precision (~6 hours).

    Sensitive columns are exported separately for manual review.
    """

    df = df.copy()

    # -----------------------------------------
    # Define sensitive columns
    # -----------------------------------------
    sensitive_columns = [
        "notification_number",
        "reference_number",
        "latitude",
        "longitude",
    ]

    # Only keep columns that actually exist
    sensitive_existing = [col for col in sensitive_columns if col in df.columns]

    # -----------------------------------------
    # Export sensitive data for manual review
    # -----------------------------------------
    if sensitive_existing:
        manual_review_df = df[sensitive_existing].copy()

        # Ensure output directory exists
        Path(review_output_path).parent.mkdir(parents=True, exist_ok=True)

        manual_review_df.to_csv(review_output_path, index=False)

    # -----------------------------------------
    # Drop sensitive columns from analytical dataset
    # -----------------------------------------
    anonymised_df = df.drop(columns=sensitive_existing)

    # -----------------------------------------
    # Temporal precision: round to 6 hours
    # -----------------------------------------
    if "creation_timestamp" in anonymised_df.columns:
        anonymised_df["creation_timestamp"] = (
            anonymised_df["creation_timestamp"]
            .dt.floor("6h")
        )

    return anonymised_df

#%%
def main():
    logger.info("Computing Atlantis centroid from OSM boundary")
    config = load_config("config.yaml")
    radius_km = config["subsample"]["radius_km"]
    wind_enrichment_max_retries = config["wind_enrichment"]["max_retries"]
    wind_enrichment_backoff_factor = config["wind_enrichment"]["backoff_factor"]
    
    atlantis_lat, atlantis_lon = get_atlantis_centroid()

    logger.info(
        f"Derived Atlantis centroid | "
        f"lat={round(atlantis_lat, 6)} | "
        f"lon={round(atlantis_lon, 6)}"
    )

    # -------------------------
    # Atlantis Subsampling
    # -------------------------
    logger.info("Atlantis subsampling stage initiated")

    
    subsample_df = subsample_sr_hex(
        sr_hex_path="data/sr_hex.csv.gz",
        atlantis_lat=atlantis_lat,
        atlantis_lon=atlantis_lon,
        radius_km=radius_km
    )
    
    logger.info(
        f"Atlantis subsample | "
        f"radius_km={radius_km} | "
        f"subsample_count={len(subsample_df)}"
    )

    # wind data implementation

    logger.info("Loading wind dataset")
    wind_df = load_wind_data(wind_enrichment_max_retries,wind_enrichment_backoff_factor)
    
    logger.info(f"Wind shape: {wind_df.shape}")    
    logger.info(f"Wind columns: {list(wind_df.columns)}")
    logger.info(f"Wind cleaned preview:\n{wind_df.head()}")
    
    # Wind Enrichment Stage    
    logger.info("Wind enrichment stage initiated")
    
    # Convert SR timestamp
    subsample_df["creation_timestamp"] = pd.to_datetime(
        subsample_df["creation_timestamp"],
        errors="coerce"
    )
    
    # Remove timezone info to match wind dataset
    subsample_df["creation_timestamp"] = (
        subsample_df["creation_timestamp"]
        .dt.tz_localize(None)
    )
    
    # Sort both datasets
    subsample_df = subsample_df.sort_values("creation_timestamp")
    wind_df = wind_df.sort_values("DateTime")
    
    # Perform nearest-hour join
    enriched_df = pd.merge_asof(
        subsample_df,
        wind_df,
        left_on="creation_timestamp",
        right_on="DateTime",
        direction="nearest",
        tolerance=pd.Timedelta("1h")
    )
    
    enriched_df = enriched_df.rename(columns={"DateTime": "wind_timestamp"})    
    
    logger.info(
        f"Wind enrichment completed | enriched_records={len(enriched_df)}"
    )    

    # Anonymisation Stage
    # -------------------------------------------------
    logger.info("Anonymisation stage initiated")
    
    anonymised_df = anonymise_dataset(
    enriched_df,
    review_output_path="data/manual_review.csv"
    )

    logger.info("Manual review file created at data/manual_review.csv")
    
    logger.info(
        f"Anonymisation completed | "
        f"final_columns={list(anonymised_df.columns)}"
    )    
    
    
if __name__ == "__main__":
    main()
   



