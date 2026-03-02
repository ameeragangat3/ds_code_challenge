#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Project-level constants used across pipeline modules."""

from pathlib import Path

# Public data endpoints and object names.
CREDS_URL = (
    "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/"
    "ds_code_challenge_creds.json"
)
AWS_BUCKET = "cct-ds-code-challenge-input-data"
CITY_HEX_FILE = "city-hex-polygons-8-10.geojson"
REGION = "af-south-1"
VALIDATION_FILE = "city-hex-polygons-8.geojson"
SR_FILE = "sr.csv.gz"
SR_HEX_FILE = "sr_hex.csv.gz"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Local cache paths.
ATLANTIS_BOUNDARY_CACHE_PATH = Path("data/atlantis_boundary.geojson")
WIND_URL = (
    "https://www.capetown.gov.za/_layouts/OpenDataPortalHandler/DownloadHandler.ashx"
    "?DocumentName=Wind_direction_and_speed_2020.ods"
    "&DatasetDocument=https%3A%2F%2Fcityapps.capetown.gov.za%2Fsites%2F"
    "opendatacatalog%2FDocuments%2FWind%2FWind_direction_and_speed_2020.ods"
)
WIND_CACHE_PATH = Path("data/wind_2020.ods")
HEX_SCHEMA = "schemas/hex_schema.json"
