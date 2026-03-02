#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Orchestrates the end-to-end Data Engineering challenge pipeline."""

import sys
import time
from pathlib import Path

import pandas as pd

from src import HEX_SCHEMA, SR_FILE, SR_HEX_FILE, VALIDATION_FILE
from src.data_extract_validate import (
    download_aws_file,
    extract_resolution_8_hexes_s3,
    validate_against_reference,
    validate_hex_schema,
)
from src.data_transformation import assign_requests_to_hex, load_config
from src.data_transformation_extended import (
    anonymise_dataset,
    apply_k_anonymity_filter,
    get_atlantis_centroid,
    load_wind_data,
    subsample_sr_hex,
)
from src.logging_config import setup_logging


def run_pipeline() -> None:
    """Execute the full extraction, transformation, validation, and anonymisation flow."""
    pipeline_start_time = time.time()
    logger = setup_logging()
    logger.info("DS Code Challenge pipeline started")

    logger.info("Load all config parameters")
    config = load_config("config.yaml")
    validation_threshold = config["validation"]["conformance_threshold"]
    spatial_failure_threshold = config["spatial_assignment"]["failure_threshold"]
    radius_km = config["subsample"]["radius_km"]
    wind_max_retries = config["wind_enrichment"]["max_retries"]
    wind_backoff_factor = config["wind_enrichment"]["backoff_factor"]

    logger.info("Data extraction stage initiated")
    extraction_start_time = time.time()

    hex_features, extract_runtime = extract_resolution_8_hexes_s3()
    logger.info(
        "AWS S3 Select extraction completed | resolution_8_features=%s | "
        "runtime_seconds=%s",
        len(hex_features),
        extract_runtime,
    )

    extraction_stage_runtime = round(time.time() - extraction_start_time, 3)
    logger.info("1.1 Data extraction stage completed in %s seconds", extraction_stage_runtime)
    logger.info("Pipeline runtime %s seconds", round(time.time() - pipeline_start_time, 3))

    logger.info("Reference validation stage initiated")
    ref_val_start_time = time.time()

    logger.info("Download validation file city-hex-polygons-8.geojson from AWS")
    download_aws_file(VALIDATION_FILE)

    logger.info("Perform validation of extracted data against city-hex-polygons-8.geojson")
    validate_against_reference(hex_features)

    reference_validation_stage_runtime = round(time.time() - ref_val_start_time, 3)
    logger.info(
        "1.2 Reference validation stage completed in %s seconds",
        reference_validation_stage_runtime,
    )
    logger.info("Pipeline runtime %s seconds", round(time.time() - pipeline_start_time, 3))

    logger.info("Additional schema validation stage initiated")
    add_ref_val_start_time = time.time()

    validation_geojson = {"type": "FeatureCollection", "features": hex_features}
    validation_report = validate_hex_schema(validation_geojson, HEX_SCHEMA)

    logger.info(
        "Validation report | total_features=%s | total_rule_checks=%s | "
        "total_rule_failures=%s | conformance_score=%s | runtime_seconds=%s",
        validation_report["total_features"],
        validation_report["total_rule_checks"],
        validation_report["total_rule_failures"],
        validation_report["conformance_score"],
        validation_report["runtime_seconds"],
    )

    if validation_report["failure_breakdown"]:
        logger.info("Failure breakdown | %s", validation_report["failure_breakdown"])

    if validation_report["conformance_score"] < validation_threshold:
        raise ValueError(
            "Schema validation failed threshold, conformance score: "
            f"{validation_report['conformance_score']}"
        )

    logger.info(
        "Schema validation passed threshold, conformance score: %s",
        validation_report["conformance_score"],
    )

    add_reference_validation_stage_runtime = round(time.time() - add_ref_val_start_time, 3)
    logger.info(
        "1.3 Additional schema validation stage completed in %s seconds",
        add_reference_validation_stage_runtime,
    )
    logger.info("Pipeline runtime %s seconds", round(time.time() - pipeline_start_time, 3))

    logger.info("Data transformation spatial assignment stage initiated")
    data_trans_spatial_start_time = time.time()

    logger.info("Download service request file sr.csv.gz from AWS")
    download_aws_file(SR_FILE)

    assignment_result = assign_requests_to_hex(
        sr_path=f"data/{SR_FILE}",
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

    data_trans_spatial_stage_runtime = round(time.time() - data_trans_spatial_start_time, 3)
    logger.info(
        "2.1 Data transformation spatial assignment completed in %s seconds",
        data_trans_spatial_stage_runtime,
    )
    logger.info("Pipeline runtime %s seconds", round(time.time() - pipeline_start_time, 3))

    logger.info("Ground-truth comparison stage initiated")
    data_trans_spatial_validate_start_time = time.time()

    logger.info("Download transformation validation file sr_hex.csv.gz")
    download_aws_file(SR_HEX_FILE)

    logger.info("Compare computed h3 indices against ground-truth file")
    df_hex = pd.read_csv(f"data/{SR_HEX_FILE}", compression="gzip")
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
    if match_rate < validation_threshold:
        raise ValueError(
            "Spatial transformation failed validation threshold, match rate score: "
            f"{match_rate}"
        )

    logger.info("Spatial transformation passed threshold, match rate score: %s", match_rate)

    data_trans_spatial_validate_stage_runtime = round(
        time.time() - data_trans_spatial_validate_start_time,
        3,
    )
    logger.info(
        "2.2 Ground-truth validation completed in %s seconds",
        data_trans_spatial_validate_stage_runtime,
    )
    logger.info("Pipeline runtime %s seconds", round(time.time() - pipeline_start_time, 3))

    logger.info("Atlantis subsampling stage initiated")
    data_trans_atlantis_start_time = time.time()

    logger.info("Compute Atlantis centroid from downloaded polygon boundary")
    atlantis_lat, atlantis_lon = get_atlantis_centroid()
    logger.info(
        "Derived Atlantis centroid | lat=%s | lon=%s",
        round(atlantis_lat, 6),
        round(atlantis_lon, 6),
    )

    logger.info(
        "Select sr_hex records within 1 arc minute (~1.852km) of Atlantis centroid"
    )
    subsample_df = subsample_sr_hex(
        sr_hex_path=f"data/{SR_HEX_FILE}",
        atlantis_lat=atlantis_lat,
        atlantis_lon=atlantis_lon,
        radius_km=radius_km,
    )

    logger.info(
        "Atlantis subsample | radius_from_center=%s | records=%s",
        radius_km,
        len(subsample_df),
    )

    data_trans_atlantis_stage_runtime = round(time.time() - data_trans_atlantis_start_time, 3)
    logger.info(
        "5.1 Atlantis subsampling stage completed in %s seconds",
        data_trans_atlantis_stage_runtime,
    )
    logger.info("Pipeline runtime %s seconds", round(time.time() - pipeline_start_time, 3))

    logger.info("Wind enrichment stage initiated")
    data_trans_wind_start_time = time.time()

    logger.info("Load wind dataset")
    wind_df = load_wind_data(wind_max_retries, wind_backoff_factor)

    logger.debug("Wind shape: %s", wind_df.shape)
    logger.debug("Wind columns: %s", list(wind_df.columns))
    logger.debug("Wind cleaned preview:\n%s", wind_df.head())

    logger.info("Enrich Atlantis subsample dataset with wind data")

    subsample_df["creation_timestamp"] = pd.to_datetime(
        subsample_df["creation_timestamp"],
        errors="coerce",
    )
    subsample_df["creation_timestamp"] = subsample_df["creation_timestamp"].dt.tz_localize(
        None
    )

    subsample_df = subsample_df.sort_values("creation_timestamp")
    wind_df = wind_df.sort_values("DateTime")

    logger.info("Join Atlantis subsample with wind data by nearest hour")
    enriched_df = pd.merge_asof(
        subsample_df,
        wind_df,
        left_on="creation_timestamp",
        right_on="DateTime",
        direction="nearest",
        tolerance=pd.Timedelta("1h"),
    ).rename(columns={"DateTime": "wind_timestamp"})

    logger.info("Wind enrichment completed | enriched_records=%s", len(enriched_df))

    data_trans_wind_stage_runtime = round(time.time() - data_trans_wind_start_time, 3)
    logger.info(
        "5.2 Atlantis wind enrichment stage completed in %s seconds",
        data_trans_wind_stage_runtime,
    )
    logger.info("Pipeline runtime %s seconds", round(time.time() - pipeline_start_time, 3))

    logger.info("Anonymisation stage initiated")
    data_trans_anon_start_time = time.time()

    output_dir = Path("output")
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Perform column-level anonymisation")
    anonymised_df = anonymise_dataset(
        enriched_df,
        review_output_path=f"{output_dir}/removed_columns_manual_review.csv",
    )
    logger.info("Column anonymisation completed | final_columns=%s", list(anonymised_df.columns))

    logger.info("Apply k-anonymity filtering")
    final_anonymised_df, manual_review_df = apply_k_anonymity_filter(
        anonymised_df,
        spatial_col="h3_level8_index",
        time_col="creation_timestamp",
        k=3,
    )

    logger.info("Save anonymised and review datasets to output directory")
    enriched_df.to_csv(f"{output_dir}/full_dataset_manual_review.csv", index=False)
    final_anonymised_df.to_csv(f"{output_dir}/final_anonymised_dataset.csv", index=False)
    manual_review_df.to_csv(f"{output_dir}/manual_review_high_risk_records.csv", index=False)

    logger.info("Final anonymised dataset created at %s", output_dir / "final_anonymised_dataset.csv")
    logger.info(
        "Anonymised columns manual review file created at %s",
        output_dir / "removed_columns_manual_review.csv",
    )
    logger.info(
        "Anonymised high-risk records manual review file created at %s",
        output_dir / "manual_review_high_risk_records.csv",
    )
    logger.info("Full dataset manual review file created at %s", output_dir / "full_dataset_manual_review.csv")

    logger.info("Anonymisation completed and files exported to output/")
    data_trans_anon_stage_runtime = round(time.time() - data_trans_anon_start_time, 3)
    logger.info("5.3 Anonymisation stage completed in %s seconds", data_trans_anon_stage_runtime)
    logger.info("Pipeline runtime %s seconds", round(time.time() - pipeline_start_time, 3))

    logger.info("Pipeline completed")


if __name__ == "__main__":
    run_pipeline()
