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

    # 1.1 Data Extraction Stage
    logger.info("1.1 Data extraction stage initiated")
    extraction_start_time = time.time()

    logger.info("Extract resolution 8 hex data from AWS bucket")
    hex_features, extract_runtime = extract_resolution_8_hexes_s3()

    logger.info(
        "AWS S3 Select extraction completed | resolution_8_features=%s | "
        "runtime_seconds=%s",
        len(hex_features),
        extract_runtime,
    )

    logger.info(f"Extracted {len(hex_features)} resolution 8 hex features")
    extraction_stage_runtime = round(time.time() - extraction_start_time, 3)
    logger.info(
        f"1.1 Data extraction stage completed in {extraction_stage_runtime} seconds")
    logger.info(
        f"Pipeline runtime {round(time.time() - pipeline_start_time, 3)} seconds")
    logger.info("\n---------------------------------------------------------\n")

    # 1.2. Reference Validation Against Provided Validation Dataset
    logger.info("1.2 Reference validation stage initiated")
    ref_val_start_time = time.time()

    logger.info("Download validation file city-hex-polygons-8.geojson from AWS")
    download_aws_file(VALIDATION_FILE)

    logger.info(
        "Perform validation of extracted data against city-hex-polygons-8.geojson")
    validate_against_reference(hex_features)

    reference_validation_stage_runtime = round(
        time.time() - ref_val_start_time, 3)
    logger.info(
        f"1.2 Reference validation stage completed in {reference_validation_stage_runtime} seconds")
    logger.info(
        f"Pipeline runtime {round(time.time() - pipeline_start_time, 3)} seconds")
    logger.info("\n---------------------------------------------------------\n")

    # 1.3 Additional Validation Against Provided Validation Dataset Schema
    logger.info("1.3 Additional schema validation stage initiated")
    add_ref_val_start_time = time.time()

    logger.info("create a geojson format from extracted hex_features data")
    validation_geojson = {
        "type": "FeatureCollection", "features": hex_features}
    logger.info("calculate validation metrics and conformance score")
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

    if validation_report['failure_breakdown']:
        logger.info(
            f"Failure breakdown | {validation_report['failure_breakdown']}")

    if validation_report["conformance_score"] < validation_threshold:
        raise ValueError(
            "Schema validation failed threshold, conformance score: "
            f"{validation_report['conformance_score']}"
        )

    logger.info(
        f"Schema validation passed threshold, conformance score: {validation_report['conformance_score']}")

    add_reference_validation_stage_runtime = round(
        time.time() - add_ref_val_start_time, 3)
    logger.info(
        f"1.3 Additional reference validation stage completed in {add_reference_validation_stage_runtime} seconds")
    logger.info(
        f"Pipeline runtime {round(time.time() - pipeline_start_time, 3)} seconds")
    logger.info("\n---------------------------------------------------------\n")

    # 2.1 Data Transformation Spatial Assignment Stage
    logger.info("2.1 Data transformation spatial assignment stage initiated")
    data_trans_spatial_start_time = time.time()

    logger.info("Download service request file sr.csv.gz from AWS")
    download_aws_file(SR_FILE)

    logger.info("Assign each service request to an H3 resolution 8 hex index")
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

    data_trans_spatial_stage_runtime = round(
        time.time() - data_trans_spatial_start_time, 3)
    logger.info("All SR successfully assigned a H3 resolution 8 index")
    logger.info(
        f"2.1 Data transformation spatial assignment completed in {data_trans_spatial_stage_runtime} seconds")
    logger.info(
        f"Pipeline runtime {round(time.time() - pipeline_start_time, 3)} seconds")
    logger.info("\n---------------------------------------------------------\n")

    # 2.2 Ground Truth Comparison, Validate Transformation
    logger.info(
        "2.2 Ground truth comparison, validate transformation stage initiated")
    data_trans_spatial_validate_start_time = time.time()

    logger.info("Download transformation validation file sr_hex.csv.gz")
    download_aws_file(SR_HEX_FILE)

    logger.info(
        "Load in validation hex file and compare against computed sr file with hex assignment")
    df_hex = pd.read_csv(f"data/{SR_HEX_FILE}", compression="gzip")
    df_computed = assignment_result["dataframe"].copy()

    # ensure hex column is type str
    df_computed["computed_h3_index"] = df_computed["computed_h3_index"].astype(
        str)
    df_hex["h3_level8_index"] = df_hex["h3_level8_index"].astype(str)

    # perform merge to compare
    merged = df_computed.merge(
        df_hex[["notification_number", "h3_level8_index"]],
        on="notification_number",
        how="inner",
    )
    logger.info(
        "Calculate the merge match rate of the 2 hex datasets to validate")
    match_rate = (merged["computed_h3_index"] ==
                  merged["h3_level8_index"]).mean()

    logger.info(f"H3 comparison match_rate={round(match_rate, 5)}")
    if match_rate < validation_threshold:
        raise ValueError(
            "Spatial transformation failed validation threshold, match rate score: "
            f"{match_rate}"
        )

    logger.info(
        f"Spatial transformation passed threshold, match rate score: {match_rate}")

    data_trans_spatial_validate_stage_runtime = round(
        time.time() - data_trans_spatial_validate_start_time, 3)
    logger.info(f"Computed hex dataset validated against {SR_HEX_FILE}")
    logger.info(
        f"2.2 Additional reference validation stage completed in {data_trans_spatial_validate_stage_runtime} seconds")
    logger.info(
        f"Pipeline runtime {round(time.time() - pipeline_start_time, 3)} seconds")
    logger.info("\n---------------------------------------------------------\n")

    # 5.1 Atlantis Subsampling
    logger.info("5.1 Atlantis subsampling stage initiated")
    data_trans_atlantis_start_time = time.time()

    logger.info("Compute Atlantis centroid from downloaded polygon boundary")
    atlantis_lat, atlantis_lon = get_atlantis_centroid()
    logger.info(
        "Derived Atlantis centroid | lat=%s | lon=%s",
        round(atlantis_lat, 6),
        round(atlantis_lon, 6),
    )

    logger.info(
        "Select subsample of sr_hex.csv.gz that are within 1 minute (~1.852km) from the calculated centroid of Atlantis")
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

    data_trans_atlantis_stage_runtime = round(
        time.time() - data_trans_atlantis_start_time, 3)
    logger.info(
        f"Created Atlantis subsample of SR hex dataset, no. of records in subsample: {len(subsample_df)}")
    logger.info(
        f"5.1 Atlantis subsampling stage completed in {data_trans_atlantis_stage_runtime} seconds")
    logger.info(
        f"Pipeline runtime {round(time.time() - pipeline_start_time, 3)} seconds")
    logger.info("\n---------------------------------------------------------\n")

    # 5.2 Wind Enrichment
    logger.info("5.2 Wind enrichment stage initiated")
    data_trans_wind_start_time = time.time()

    logger.info("Load wind dataset")
    wind_df = load_wind_data(wind_max_retries, wind_backoff_factor)

    logger.debug("Wind shape: %s", wind_df.shape)
    logger.debug("Wind columns: %s", list(wind_df.columns))
    logger.debug("Wind cleaned preview:\n%s", wind_df.head())

    logger.info("Enrich Atlantis subsample dataset with wind data")

    # Convert SR timestamp
    subsample_df["creation_timestamp"] = pd.to_datetime(
        subsample_df["creation_timestamp"],
        errors="coerce",
    )
    # Remove timezone info to match wind dataset
    subsample_df["creation_timestamp"] = subsample_df["creation_timestamp"].dt.tz_localize(
        None
    )
    # Sort both datasets
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

    logger.info("Wind enrichment completed | enriched_records=%s",
                len(enriched_df))

    data_trans_wind_stage_runtime = round(
        time.time() - data_trans_wind_start_time, 3)
    logger.info(
        f"Enriched Atlantis subsample data with wind data, no. of enriched_records={len(enriched_df)}")
    logger.info(
        f"5.2 Atlantis wind enrichment stage completed in {data_trans_wind_stage_runtime} seconds")
    logger.info(
        f"Pipeline runtime {round(time.time() - pipeline_start_time, 3)} seconds")
    logger.info("\n---------------------------------------------------------\n")

    # 5.3 Anonymisation Stage
    logger.info("5.3 Anonymisation stage initiated")
    data_trans_anon_start_time = time.time()

    # ensure output directory exists at project root
    output_dir = Path("output")
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Perform column-level anonymisation")
    anonymised_df = anonymise_dataset(
        enriched_df,
        review_output_path=f"{output_dir}/removed_columns_manual_review.csv",
    )
    logger.info("Column anonymisation completed | final_columns=%s",
                list(anonymised_df.columns))

    logger.info("Apply k-anonymity filtering")
    final_anonymised_df, manual_review_df = apply_k_anonymity_filter(
        anonymised_df,
        spatial_col="h3_level8_index",
        time_col="creation_timestamp",
        k=3,
    )

    logger.info("Save anonymised and review datasets to output directory")
    enriched_df.to_csv(
        f"{output_dir}/full_dataset_manual_review.csv", index=False)
    final_anonymised_df.to_csv(
        f"{output_dir}/final_anonymised_dataset.csv", index=False)
    manual_review_df.to_csv(
        f"{output_dir}/manual_review_high_risk_records.csv", index=False)
    logger.info(
        f"Final anonyised dataset created at {output_dir}/final_anonymised_dataset.csv")
    logger.info(
        f"Anonymised columns manual review file created at {output_dir}/removed_columns_manual_review.csv")
    logger.info(
        f"Anonymised high risk records manual review file created at {output_dir}/manual_review_high_risk_records.csv")
    logger.info(
        f"Full datset for full manual review file created at {output_dir}/full_dataset_manual_review.csv")

    logger.info("Anonymisation completed and files exported to output/")
    data_trans_anon_stage_runtime = round(
        time.time() - data_trans_anon_start_time, 3)
    logger.info(
        f"5.3 Anonymisation stage completed in {data_trans_anon_stage_runtime} seconds")
    logger.info(
        f"Pipeline runtime {round(time.time() - pipeline_start_time, 3)} seconds")
    logger.info("\n---------------------------------------------------------\n")

    logger.info("Pipeline Completed...........")


if __name__ == "__main__":
    run_pipeline()
