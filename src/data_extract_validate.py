#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Use the AWS S3 SELECT command to read in the H3 resolution 8 data from city-hex-polygons-8-10.geojson. Use the city-hex-polygons-8.geojson file to validate your work.

Please also add an additional validation that checks conformance to a reasonable schema for the dataset. The output of this validation should be a conformance "score" of some sort, with a non-binary threshold of your choice. Explicitly capture the desired schema used to compute this conformance score in a standalone configuration or documentation file.

Please log the time taken to perform the operations described as well as the validation steps, and within reason, try to optimise latency and computational resources used. Please also note the comments above about the nature of the code that we expect.
'''


#%%
import boto3
import json
import requests
import time
from pathlib import Path
import sys

start = time.time()

AWS_CREDS_URL = "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/ds_code_challenge_creds.json"

AWS_BUCKET = "cct-ds-code-challenge-input-data"
AWS_KEY = "city-hex-polygons-8-10.geojson"
AWS_REGION = "af-south-1"

#%
# load AWS creds
response = requests.get(AWS_CREDS_URL)
response.raise_for_status()
creds = response.json()

#%

# create the s3 client

access_key = creds["s3"]["access_key"]
secret_key = creds["s3"]["secret_key"]

session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=AWS_REGION,
)
s3 = session.client("s3")

#%
# use cleint to extract H3 resolution 8 data from city-hex-polygons-8-10.geojson

query="select * from s3object[*].features[*] s where s.properties.resolution = 8"

response = s3.select_object_content(
    Bucket=AWS_BUCKET,
    Key=AWS_KEY,
    ExpressionType="SQL",
    Expression=query,
    InputSerialization={
        "JSON": {"Type": "DOCUMENT"},
        "CompressionType": "NONE",
    },
    OutputSerialization={"JSON": {}},
)

features = []
buffer = ""

for event in response["Payload"]:
    if "Records" in event:
        buffer += event["Records"]["Payload"].decode("utf-8")

for line in buffer.strip().split("\n"):
    if line:
        record = json.loads(line)
        features.append(record)


#%
# download validation file city-hex-polygons-8.geojson and put in data/ folder
VALIDATION_FILE = "city-hex-polygons-8.geojson"
output_path = Path(f"data/{VALIDATION_FILE}")

if output_path.exists():
    print("File already exists locally.")
else:
    output_path.parent.mkdir(exist_ok=True)
    s3.download_file(AWS_BUCKET, VALIDATION_FILE, str(output_path))





print("Download complete.")

#%
# validate against downloaded file

#path = Path("data/city-hex-polygons-8.geojson")
with open(output_path) as f:
    data = json.load(f)



reference_features = data['features']

extracted_indices = {
    f["properties"]["index"]
    for f in features
}

reference_indices = {
    f["properties"]["index"]
    for f in reference_features
}

missing_in_extracted = reference_indices - extracted_indices
extra_in_extracted = extracted_indices - reference_indices

counts_match = len(features) == len(reference_features)
sets_match = extracted_indices == reference_indices

runtime = round(time.time() - start, 3)

print(
    f"Reference comparison | "
    f"counts_match={counts_match} | "
    f"sets_match={sets_match} | "
    f"missing_count={len(missing_in_extracted)} | "
    f"extra_count={len(extra_in_extracted)} | "
    f"runtime_seconds={runtime}"
)

if not sets_match:
    print(
        "S3 Select extraction does not match reference dataset."
    )
else:
    print(f"extracted AWS data matches and validated against {VALIDATION_FILE}")



#% Validate against schema of rules
def load_schema(schema_path: str):
    with open(Path(schema_path), "r") as f:
        return json.load(f)

schema_path="schemas/hex_schema.json"
hex_schema = load_schema(schema_path)

"""
hex_schema = {
    "collection_rules": {
      "type_must_be": "FeatureCollection",
      "features_key_required": True
    },
    "feature_rules": {
      "feature_type_must_be": "Feature",
      "geometry_required": True,
      "properties_required": True
    },
    "geometry_rules": {
      "geometry_type_must_be": "Polygon",
      "coordinates_required": True,
      "coordinates_min_length": 4
    },
    "property_rules": {
      "resolution_required": True,
      "resolution_type": "int",
      "resolution_must_equal": 8,
      "index_required": True,
      "index_type": "str"
    }
}
"""
# created geojson of extracted features
geojson_data = {
    "type": "FeatureCollection",
    "features": features
}


#%

total_rule_checks = 0
total_rule_failures = 0
failure_breakdown = {}

# -------------------------
# Collection-level checks
# -------------------------
total_rule_checks += 1
if geojson_data.get("type") != hex_schema["collection_rules"]["type_must_be"]:
    total_rule_failures += 1
    failure_breakdown["collection_type_mismatch"] = (
        failure_breakdown.get("collection_type_mismatch", 0) + 1
    )

features = geojson_data.get("features", [])

total_rule_checks += 1
if not isinstance(features, list):
    total_rule_failures += 1
    failure_breakdown["features_not_list"] = (
        failure_breakdown.get("features_not_list", 0) + 1
    )

# -------------------------
# Feature-level checks
# -------------------------
for feature in features:

    # Feature type rule
    total_rule_checks += 1
    if feature.get("type") != hex_schema["feature_rules"]["feature_type_must_be"]:
        total_rule_failures += 1
        failure_breakdown["feature_type_invalid"] = (
            failure_breakdown.get("feature_type_invalid", 0) + 1
        )

    # Geometry existence rule
    total_rule_checks += 1
    geometry = feature.get("geometry")
    if geometry is None:
        total_rule_failures += 1
        failure_breakdown["geometry_missing"] = (
            failure_breakdown.get("geometry_missing", 0) + 1
        )
        continue

    # Geometry type rule
    total_rule_checks += 1
    if geometry.get("type") != hex_schema["geometry_rules"]["geometry_type_must_be"]:
        total_rule_failures += 1
        failure_breakdown["geometry_type_invalid"] = (
            failure_breakdown.get("geometry_type_invalid", 0) + 1
        )

    # Coordinates existence rule
    total_rule_checks += 1
    coordinates = geometry.get("coordinates")
    if not coordinates:
        total_rule_failures += 1
        failure_breakdown["coordinates_missing"] = (
            failure_breakdown.get("coordinates_missing", 0) + 1
        )

    # Minimum coordinate length rule
    total_rule_checks += 1
    try:
        if len(coordinates[0]) < hex_schema["geometry_rules"]["coordinates_min_length"]:
            total_rule_failures += 1
            failure_breakdown["coordinates_too_short"] = (
                failure_breakdown.get("coordinates_too_short", 0) + 1
            )
    except Exception:
        total_rule_failures += 1
        failure_breakdown["coordinates_structure_invalid"] = (
            failure_breakdown.get("coordinates_structure_invalid", 0) + 1
        )

    # Properties existence rule
    total_rule_checks += 1
    properties = feature.get("properties")
    if properties is None:
        total_rule_failures += 1
        failure_breakdown["properties_missing"] = (
            failure_breakdown.get("properties_missing", 0) + 1
        )
        continue

    # Resolution existence rule
    total_rule_checks += 1
    resolution = properties.get("resolution")
    if resolution is None:
        total_rule_failures += 1
        failure_breakdown["resolution_missing"] = (
            failure_breakdown.get("resolution_missing", 0) + 1
        )

    # Resolution type rule
    total_rule_checks += 1
    if not isinstance(resolution, int):
        total_rule_failures += 1
        failure_breakdown["resolution_type_invalid"] = (
            failure_breakdown.get("resolution_type_invalid", 0) + 1
        )

    # Resolution equals 8 rule
    total_rule_checks += 1
    if resolution != hex_schema["property_rules"]["resolution_must_equal"]:
        total_rule_failures += 1
        failure_breakdown["resolution_value_invalid"] = (
            failure_breakdown.get("resolution_value_invalid", 0) + 1
        )

    # Index existence rule (UPDATED)
    total_rule_checks += 1
    hex_index = properties.get("index")
    if hex_index is None:
        total_rule_failures += 1
        failure_breakdown["index_missing"] = (
            failure_breakdown.get("index_missing", 0) + 1
        )

    # Index type rule (UPDATED)
    total_rule_checks += 1
    if not isinstance(hex_index, str):
        total_rule_failures += 1
        failure_breakdown["index_type_invalid"] = (
            failure_breakdown.get("index_type_invalid", 0) + 1
        )

total_features = len(features)

conformance_score = (
    (total_rule_checks - total_rule_failures) / total_rule_checks
    if total_rule_checks > 0
    else 0
)

conformance_score=round(conformance_score, 5)

validation_conformance_threshold = 0.95

if conformance_score < validation_conformance_threshold:
    print(f"Validation failed threshold, conformance score {conformance_score}")
    sys.exit(1)
else: 
    print(f"Validation passed threshold, conformance score {conformance_score}")

runtime = round(time.time() - start, 3)

print(f"extraction and validation took {runtime} seconds")

