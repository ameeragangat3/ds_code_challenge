#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Extraction and validation utilities for challenge input datasets."""

import json
import time
from pathlib import Path
from typing import Any

import boto3
import requests
import yaml

from src import AWS_BUCKET, CITY_HEX_FILE, CREDS_URL, REGION, VALIDATION_FILE
from src.logging_config import setup_logging

logger = setup_logging()


def load_aws_credentials() -> dict[str, Any]:
    """Load AWS credentials JSON used by this challenge.

    Returns:
        Parsed credentials payload.
    """
    response = requests.get(CREDS_URL, timeout=30)
    response.raise_for_status()
    return response.json()


def create_s3_client() -> Any:
    """Create a boto3 S3 client from downloaded challenge credentials."""
    creds = load_aws_credentials()
    access_key = creds["s3"]["access_key"]
    secret_key = creds["s3"]["secret_key"]

    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=REGION,
    )
    return session.client("s3")


def extract_resolution_8_hexes_s3() -> tuple[list[dict[str, Any]], float]:
    """Extract only resolution-8 features from the combined city hex GeoJSON.

    Returns:
        A tuple of extracted feature objects and extraction runtime in seconds.
    """
    s3 = create_s3_client()
    start_time = time.time()

    expression = (
        "select * from s3object[*].features[*] s "
        "where s.properties.resolution = 8"
    )

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

    features: list[dict[str, Any]] = []
    payload_buffer = ""

    for event in response["Payload"]:
        if "Records" in event:
            payload_buffer += event["Records"]["Payload"].decode("utf-8")

    # S3 Select emits newline-delimited JSON records.
    for line in payload_buffer.strip().split("\n"):
        if line:
            features.append(json.loads(line))

    runtime = round(time.time() - start_time, 3)
    return features, runtime


def download_aws_file(filename: str) -> None:
    """Download a file from the challenge bucket into the local ``data/`` folder."""
    output_path = Path(f"data/{filename}")
    if output_path.exists():
        logger.info("%s file already exists locally.", filename)
        return

    output_path.parent.mkdir(exist_ok=True)
    s3 = create_s3_client()
    s3.download_file(AWS_BUCKET, filename, str(output_path))


def load_reference_hexes() -> list[dict[str, Any]]:
    """Load the local reference resolution-8 hex feature collection.

    Raises:
        FileNotFoundError: If the local reference file does not exist.
        ValueError: If the loaded payload does not contain ``features``.
    """
    path = Path("data/city-hex-polygons-8.geojson")
    if not path.exists():
        raise FileNotFoundError(
            "city-hex-polygons-8.geojson not found in data folder. "
            "Please download it from the AWS bucket."
        )

    with path.open("r", encoding="utf-8") as file_obj:
        data = json.load(file_obj)

    if "features" not in data:
        raise ValueError("Reference file does not contain 'features' key.")

    return data["features"]


def validate_against_reference(extracted_features: list[dict[str, Any]]) -> None:
    """Validate extracted features against the provided reference dataset.

    Args:
        extracted_features: Features obtained from S3 Select extraction.

    Raises:
        ValueError: If extracted feature index sets differ from the reference.
    """
    logger.info("Reference validation stage initiated")
    start = time.time()

    reference_features = load_reference_hexes()
    extracted_indices = {feature["properties"]["index"] for feature in extracted_features}
    reference_indices = {feature["properties"]["index"] for feature in reference_features}

    missing_in_extracted = reference_indices - extracted_indices
    extra_in_extracted = extracted_indices - reference_indices

    counts_match = len(extracted_features) == len(reference_features)
    sets_match = extracted_indices == reference_indices
    runtime = round(time.time() - start, 3)

    logger.info(
        "Reference comparison | counts_match=%s | sets_match=%s | missing_count=%s "
        "| extra_count=%s | runtime_seconds=%s",
        counts_match,
        sets_match,
        len(missing_in_extracted),
        len(extra_in_extracted),
        runtime,
    )

    if not sets_match:
        raise ValueError("S3 Select extraction does not match reference dataset.")

    logger.info("Reference validation passed")


def load_schema(schema_path: str | Path) -> dict[str, Any]:
    """Load schema definition used for GeoJSON conformance scoring."""
    with Path(schema_path).open("r", encoding="utf-8") as file_obj:
        return json.load(file_obj)


def validate_hex_schema(
    geojson_data: dict[str, Any],
    schema_path: str | Path,
) -> dict[str, Any]:
    """Validate GeoJSON hex payload and return conformance metrics.

    Args:
        geojson_data: GeoJSON-like dictionary to validate.
        schema_path: Path to the schema JSON file.

    Returns:
        Validation report including check counts, failures, and score.
    """
    start_time = time.time()
    schema = load_schema(schema_path)

    total_rule_checks = 0
    total_rule_failures = 0
    failure_breakdown: dict[str, int] = {}

    total_rule_checks += 1
    if geojson_data.get("type") != schema["collection_rules"]["type_must_be"]:
        total_rule_failures += 1
        failure_breakdown["collection_type_mismatch"] = (
            failure_breakdown.get("collection_type_mismatch", 0) + 1
        )

    total_rule_checks += 1
    if schema["collection_rules"].get("features_key_required", False):
        if "features" not in geojson_data:
            total_rule_failures += 1
            failure_breakdown["features_key_missing"] = (
                failure_breakdown.get("features_key_missing", 0) + 1
            )

    features = geojson_data.get("features", [])

    total_rule_checks += 1
    if not isinstance(features, list):
        total_rule_failures += 1
        failure_breakdown["features_not_list"] = (
            failure_breakdown.get("features_not_list", 0) + 1
        )

    for feature in features:
        total_rule_checks += 1
        if feature.get("type") != schema["feature_rules"]["feature_type_must_be"]:
            total_rule_failures += 1
            failure_breakdown["feature_type_invalid"] = (
                failure_breakdown.get("feature_type_invalid", 0) + 1
            )

        total_rule_checks += 1
        geometry = feature.get("geometry")
        if geometry is None:
            total_rule_failures += 1
            failure_breakdown["geometry_missing"] = (
                failure_breakdown.get("geometry_missing", 0) + 1
            )
            continue

        total_rule_checks += 1
        if geometry.get("type") != schema["geometry_rules"]["geometry_type_must_be"]:
            total_rule_failures += 1
            failure_breakdown["geometry_type_invalid"] = (
                failure_breakdown.get("geometry_type_invalid", 0) + 1
            )

        total_rule_checks += 1
        coordinates = geometry.get("coordinates")
        if not coordinates:
            total_rule_failures += 1
            failure_breakdown["coordinates_missing"] = (
                failure_breakdown.get("coordinates_missing", 0) + 1
            )

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

        total_rule_checks += 1
        properties = feature.get("properties")
        if properties is None:
            total_rule_failures += 1
            failure_breakdown["properties_missing"] = (
                failure_breakdown.get("properties_missing", 0) + 1
            )
            continue

        total_rule_checks += 1
        resolution = properties.get("resolution")
        if resolution is None:
            total_rule_failures += 1
            failure_breakdown["resolution_missing"] = (
                failure_breakdown.get("resolution_missing", 0) + 1
            )

        total_rule_checks += 1
        if not isinstance(resolution, int):
            total_rule_failures += 1
            failure_breakdown["resolution_type_invalid"] = (
                failure_breakdown.get("resolution_type_invalid", 0) + 1
            )

        total_rule_checks += 1
        if resolution != schema["property_rules"]["resolution_must_equal"]:
            total_rule_failures += 1
            failure_breakdown["resolution_value_invalid"] = (
                failure_breakdown.get("resolution_value_invalid", 0) + 1
            )

        total_rule_checks += 1
        hex_index = properties.get("index")
        if hex_index is None:
            total_rule_failures += 1
            failure_breakdown["index_missing"] = (
                failure_breakdown.get("index_missing", 0) + 1
            )

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


def load_yaml(path: str | Path) -> dict[str, Any]:
    """Load a YAML configuration file into a dictionary."""
    with Path(path).open("r", encoding="utf-8") as file_obj:
        return yaml.safe_load(file_obj)


def main() -> None:
    """Run extraction and validation stages as a standalone script."""
    config_data = load_yaml("config.yaml")

    logger.info("Data extraction stage initiated")
    hex_features, extract_runtime = extract_resolution_8_hexes_s3()
    logger.info(
        "S3 Select extraction completed | resolution_8_features=%s | "
        "runtime_seconds=%s",
        len(hex_features),
        extract_runtime,
    )

    logger.info("Extracted %s resolution 8 hex features", len(hex_features))
    logger.info("Data extraction stage complete")

    logger.info("Reference validation stage initiated")
    logger.info("Download validation file city-hex-polygons-8.geojson from AWS")
    download_aws_file(VALIDATION_FILE)
    validate_against_reference(hex_features)
    logger.info("Reference validation stage completed")

    logger.info("Additional validation stage initiated")
    validation_geojson = {"type": "FeatureCollection", "features": hex_features}

    report = validate_hex_schema(validation_geojson, "schemas/hex_schema.json")
    logger.info(
        "Validation report | total_features=%s | total_rule_checks=%s | "
        "total_rule_failures=%s | conformance_score=%s | runtime_seconds=%s",
        report["total_features"],
        report["total_rule_checks"],
        report["total_rule_failures"],
        report["conformance_score"],
        report["runtime_seconds"],
    )

    if report["failure_breakdown"]:
        logger.info("Failure breakdown | %s", report["failure_breakdown"])

    threshold = config_data["validation"]["conformance_threshold"]
    if report["conformance_score"] < threshold:
        raise ValueError(
            "Schema validation failed threshold, conformance score: "
            f"{report['conformance_score']}"
        )

    logger.info(
        "Schema validation passed threshold, conformance score: %s",
        report["conformance_score"],
    )


if __name__ == "__main__":
    main()
