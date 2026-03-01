#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#%%
import boto3
import json
import requests
import time
from pathlib import Path
import yaml


from src.logging_config import setup_logging

CREDS_URL = "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/ds_code_challenge_creds.json"

AWS_BUCKET = "cct-ds-code-challenge-input-data"
CITY_HEX_FILE = "city-hex-polygons-8-10.geojson"
REGION = "af-south-1"
VALIDATION_FILE = "city-hex-polygons-8.geojson"

logger = setup_logging()

def load_aws_credentials():
    response = requests.get(CREDS_URL)
    response.raise_for_status()
    return response.json()


def create_s3_client():
    creds = load_aws_credentials()

    access_key = creds["s3"]["access_key"]
    secret_key = creds["s3"]["secret_key"]

    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=REGION,
    )

    return session.client("s3")


def extract_resolution_8_hexes_s3():
    """
    Use S3 Select to retrieve only resolution 8 features.
    """

    s3 = create_s3_client()

    start_time = time.time()
    
    expression="select * from s3object[*].features[*] s where s.properties.resolution = 8"

    response = s3.select_object_content(
        Bucket=AWS_BUCKET,
        Key=CITY_HEX_FILE,
        ExpressionType="SQL",
        Expression=expression,
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
    
    # Now safely split by newline
    for line in buffer.strip().split("\n"):
        if line:
            record = json.loads(line)
            features.append(record)

    runtime = round(time.time() - start_time, 3)

    return features, runtime


def download_aws_file(filename):
    output_path = Path(f"data/{filename}")

    if output_path.exists():
        print("File already exists locally.")
    else:
        output_path.parent.mkdir(exist_ok=True)
        s3 = create_s3_client()
        s3.download_file(AWS_BUCKET, filename, str(output_path))


def load_reference_hexes():
    """
    Load the resolution 8 reference dataset:
    city-hex-polygons-8.geojson

    Raises:
        FileNotFoundError if the file does not exist.
    """

    path = Path("data/city-hex-polygons-8.geojson")

    if not path.exists():
        raise FileNotFoundError(
            "city-hex-polygons-8.geojson not found in data folder. "
            "Please download it from the AWS bucket."
        )

    with open(path) as f:
        data = json.load(f)

    if "features" not in data:
        raise ValueError("Reference file does not contain 'features' key.")

    return data["features"]


def validate_against_reference(extracted_features):
    """
    Validate that S3 Select extraction results match
    the reference resolution 8 dataset exactly.

    Checks:
    - Feature count equality
    - Exact index set equality

    Raises:
        ValueError if mismatch is detected.
    """

    logger.info("Reference validation stage initiated")

    start = time.time()
    reference_features = load_reference_hexes()

    extracted_indices = {
        f["properties"]["index"]
        for f in extracted_features
    }

    reference_indices = {
        f["properties"]["index"]
        for f in reference_features
    }

    missing_in_extracted = reference_indices - extracted_indices
    extra_in_extracted = extracted_indices - reference_indices

    counts_match = len(extracted_features) == len(reference_features)
    sets_match = extracted_indices == reference_indices

    runtime = round(time.time() - start, 3)

    logger.info(
        f"Reference comparison | "
        f"counts_match={counts_match} | "
        f"sets_match={sets_match} | "
        f"missing_count={len(missing_in_extracted)} | "
        f"extra_count={len(extra_in_extracted)} | "
        f"runtime_seconds={runtime}"
    )

    if not sets_match:
        raise ValueError(
            "S3 Select extraction does not match reference dataset."
        )

    logger.info("Reference validation passed")

def load_schema(schema_path):
    with open(Path(schema_path), "r") as f:
        return json.load(f)
 
def validate_hex_schema(geojson_data, schema_path):
    """
    Validate GeoJSON hex dataset against schema rules.
    """

    start_time = time.time()

    schema = load_schema(schema_path)

    total_rule_checks = 0
    total_rule_failures = 0
    failure_breakdown = {}

    # Collection-level checks
    # Collection type rule
    total_rule_checks += 1
    if geojson_data.get("type") != schema["collection_rules"]["type_must_be"]:
        total_rule_failures += 1
        failure_breakdown["collection_type_mismatch"] = (
            failure_breakdown.get("collection_type_mismatch", 0) + 1
        )

    # Features key required rule
    total_rule_checks += 1
    if schema["collection_rules"].get("features_key_required", False):
        if "features" not in geojson_data:
            total_rule_failures += 1
            failure_breakdown["features_key_missing"] = (
                failure_breakdown.get("features_key_missing", 0) + 1
            )

    features = geojson_data.get("features", [])

    # Features must be list
    total_rule_checks += 1
    if not isinstance(features, list):
        total_rule_failures += 1
        failure_breakdown["features_not_list"] = (
            failure_breakdown.get("features_not_list", 0) + 1
        )

    # Feature-level checks
    for feature in features:

        # Feature type rule
        total_rule_checks += 1
        if feature.get("type") != schema["feature_rules"]["feature_type_must_be"]:
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
        if geometry.get("type") != schema["geometry_rules"]["geometry_type_must_be"]:
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
            if len(coordinates[0]) < schema["geometry_rules"]["coordinates_min_length"]:
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

        # Resolution equals rule
        total_rule_checks += 1
        if resolution != schema["property_rules"]["resolution_must_equal"]:
            total_rule_failures += 1
            failure_breakdown["resolution_value_invalid"] = (
                failure_breakdown.get("resolution_value_invalid", 0) + 1
            )

        # Index existence rule
        total_rule_checks += 1
        hex_index = properties.get("index")
        if hex_index is None:
            total_rule_failures += 1
            failure_breakdown["index_missing"] = (
                failure_breakdown.get("index_missing", 0) + 1
            )

        # Index type rule
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

    runtime = round(time.time() - start_time, 3)

    return {
        "total_features": total_features,
        "total_rule_checks": total_rule_checks,
        "total_rule_failures": total_rule_failures,
        "conformance_score": round(conformance_score, 5),
        "failure_breakdown": failure_breakdown,
        "runtime_seconds": runtime,
    }
   

def load_yaml(path):
    with open(path, "r") as file:
        data = yaml.safe_load(file)
    return data

def main():
    # load config data
    config_data = load_yaml('config.yaml')

    logger.info("Data extraction stage initiated")

    hex_features, extract_runtime = extract_resolution_8_hexes_s3()
    
    logger.info(
        f"S3 Select extraction completed | "
        f"resolution_8_features={len(hex_features)} | "
        f"runtime_seconds={extract_runtime}"
    )

    logger.info(f"Extracted {len(hex_features)} resolution 8 hex features")
    logger.info("Data extraction stage complete")    
    
    logger.info("Reference validation stage initiated")
    
    logger.info("Download validation file city-hex-polygons-8.geojson from AWS")
    download_aws_file(VALIDATION_FILE)
    
    validate_against_reference(hex_features)
    logger.info("Reference validation stage completed")
    
    
    logger.info("Additional Validation stage initiated")

    validation_geojson = {
        "type": "FeatureCollection",
        "features": hex_features
    }

    report = validate_hex_schema(validation_geojson, "schemas/hex_schema.json")
    
    logger.info(
        f"Validation report | "
        f"total_features={report['total_features']} | "
        f"total_rule_checks={report['total_rule_checks']} | "
        f"total_rule_failures={report['total_rule_failures']} | "
        f"conformance_score={report['conformance_score']} | "
        f"runtime_seconds={report['runtime_seconds']}"
    )
    
    if report['failure_breakdown']:
        logger.info(f"Failure breakdown | {report['failure_breakdown']}")
    
    if report['conformance_score'] < config_data['validation']['conformance_threshold']:
        raise ValueError(f"Schema validation failed threshold, conformance score: {report['conformance_score']}")
    else:
        logger.info(f"Schema validation passed threshold, conformance score: {report['conformance_score']}")
    
    
if __name__ == "__main__":
    main()
    

