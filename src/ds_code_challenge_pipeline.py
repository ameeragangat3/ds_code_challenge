#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#%%
from src.logging_config import setup_logging
from src.data_extract_validate import (extract_resolution_8_hexes_s3, download_aws_file, validate_against_reference,
                                       validate_hex_schema)

from src.data_transformation import (load_config, assign_requests_to_hex)

from src.data_transformation_extended import (get_atlantis_centroid, subsample_sr_hex,load_wind_data, anonymise_dataset, 
                                              apply_k_anonymity_filter)

from src import (VALIDATION_FILE, SR_FILE, SR_HEX_FILE, HEX_SCHEMA)

import sys
import pandas as pd
from pathlib import Path
import time



#%%

def run_pipeline():
    pipeline_start_time = time.time()
    logger = setup_logging()

    logger.info("DS Code Challeng Pipeline Started")

    logger.info("Load in all config parameters")
    config = load_config("config.yaml")
    validation_threshold = config["validation"]["conformance_threshold"]
    spatial_failure_threshold = config["spatial_assignment"]["failure_threshold"]
    radius_km = config["subsample"]["radius_km"]
    wind_enrichment_max_retries = config["wind_enrichment"]["max_retries"]
    wind_enrichment_backoff_factor = config["wind_enrichment"]["backoff_factor"]

    # -------------------------
    # 1.1 Extraction Stage
    # -------------------------
    logger.info("Data extraction stage initiated")
    extraction_start_time = time.time()

    # extract resolution 8 hex data from AWS bucket
    hex_features, extract_runtime = extract_resolution_8_hexes_s3()
    
    logger.info(
        f"AWS S3 Select extraction completed | "
        f"resolution_8_features={len(hex_features)} | "
        f"runtime_seconds={extract_runtime}"
    )

    logger.info(f"Extracted {len(hex_features)} resolution 8 hex features")
    extraction_stage_runtime = round(time.time() - extraction_start_time, 3)
    logger.info(f"1.1 Data extraction stage completed in {extraction_stage_runtime} seconds")
    logger.info(f"Pipeline runtime {round(time.time() - pipeline_start_time, 3)} seconds")
    
    # -------------------------------------------------------------
    # 1.2. Reference Validation Against Provided Validation Dataset
    # -------------------------------------------------------------
    logger.info("Reference validation stage initiated")
    ref_val_start_time = time.time()
    
    logger.info("Download validation file city-hex-polygons-8.geojson from AWS")
    download_aws_file(VALIDATION_FILE)
    
    logger.info("Perform validation of extracted data against city-hex-polygons-8.geojson")
    validate_against_reference(hex_features)
    
    reference_validation_stage_runtime = round(time.time() - ref_val_start_time, 3)
    logger.info(f"1.3 Reference validation stage completed in {reference_validation_stage_runtime} seconds")
    logger.info(f"Pipeline runtime {round(time.time() - pipeline_start_time, 3)} seconds")
    
    # --------------------------------------------------------------------
    # 1.3 Additional Validation Against Provided Validation Dataset Schema
    # --------------------------------------------------------------------
    logger.info("Additional validation stage initiated")
    add_ref_val_start_time = time.time()
    
    # create a geojson format from extracted hex_features data
    validation_geojson = {
        "type": "FeatureCollection",
        "features": hex_features
    }

    # calculate validation metrics and conformance score
    validation_report = validate_hex_schema(validation_geojson, HEX_SCHEMA)
    
    logger.info(
        f"Validation report | "
        f"total_features={validation_report['total_features']} | "
        f"total_rule_checks={validation_report['total_rule_checks']} | "
        f"total_rule_failures={validation_report['total_rule_failures']} | "
        f"conformance_score={validation_report['conformance_score']} | "
        f"runtime_seconds={validation_report['runtime_seconds']}"
    )
    
    if validation_report['failure_breakdown']:
        logger.info(f"Failure breakdown | {validation_report['failure_breakdown']}")
    
    if validation_report['conformance_score'] < validation_threshold:
        raise ValueError(f"Schema validation failed threshold, conformance score: {validation_report['conformance_score']}")
    else:
        logger.info(f"Schema validation passed threshold, conformance score: {validation_report['conformance_score']}")

    add_reference_validation_stage_runtime = round(time.time() - add_ref_val_start_time, 3)
    logger.info(f"1.3 Additional reference validation stage completed in {add_reference_validation_stage_runtime} seconds")
    logger.info(f"Pipeline runtime {round(time.time() - pipeline_start_time, 3)} seconds")
    
    # -------------------------
    # 2.1 Data Transformation Spatial Assignment Stage
    # -------------------------
    logger.info("Data transformation spatial assignment stage initiated")
    data_trans_spatial_start_time = time.time()
    
    logger.info("Download service request file sr.csv.gz from AWS")
    download_aws_file(SR_FILE)
    
    #Assign each service request to an H3 resolution 8 hex index
    assignment_result = assign_requests_to_hex(
        sr_path=f"data/{SR_FILE}",
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
        
    data_trans_spatial_stage_runtime = round(time.time() - data_trans_spatial_start_time, 3)
    logger.info("All SR successfully assigned a H3 resolution 8 index")
    logger.info(f"2.1 Data transformation spatial assignment completed in {data_trans_spatial_stage_runtime} seconds")
    logger.info(f"Pipeline runtime {round(time.time() - pipeline_start_time, 3)} seconds")

    # ----------------------------------------------------
    # 2.2 Ground Truth Comparison, Validate Transformation
    # ----------------------------------------------------
    logger.info("Ground truth comparison, validate transformation stage initiated")
    data_trans_spatial_validate_start_time = time.time()

    logger.info("Download transformation validation file sr_hex.csv.gz file")    
    download_aws_file(SR_HEX_FILE)

    # read in validation hex file and compare against computed sr file with hex assignment
    logger.info("Load in validation hex file and compare against computed sr file with hex assignment")
    df_hex = pd.read_csv(f"data/{SR_HEX_FILE}", compression="gzip")
    df_computed = assignment_result['dataframe'].copy()

    # ensure hex column is type str 
    df_computed["computed_h3_index"] = df_computed["computed_h3_index"].astype(str)
    df_hex["h3_level8_index"] = df_hex["h3_level8_index"].astype(str)

    # perform merge to compare
    merged = df_computed.merge(
        df_hex[["notification_number", "h3_level8_index"]],
        on="notification_number",
        how="inner"
    )

    #calculate the merge match rate of the two datasets
    logger.info("Calculate the merge match rate of the 2 hex datasets to validate")
    match_rate = (
        (merged["computed_h3_index"] == merged["h3_level8_index"]).mean()
    )

    logger.info(f"H3 comparison match_rate={round(match_rate, 5)}")

    if match_rate < validation_threshold:
        raise ValueError(f"Spatial transformation failed validation threshold, match rate score: {match_rate}")
    else:
        logger.info(f"Spatial transformation passed threshold, match rate score: {match_rate}")

    data_trans_spatial_validate_stage_runtime = round(time.time() - data_trans_spatial_validate_start_time, 3)
    logger.info(f"Computed hex dataset validated against {SR_HEX_FILE}")
    logger.info(f"2.2 Additional reference validation stage completed in {data_trans_spatial_validate_stage_runtime} seconds")
    logger.info(f"Pipeline runtime {round(time.time() - pipeline_start_time, 3)} seconds")

    # ------------------------
    # 5.1 Atlantis Subsampling
    # ------------------------
    logger.info("Atlantis subsampling stage initiated")
    data_trans_atlantis_start_time = time.time()
    
    logger.info("Computing Atlantis suburb centroid from downloaded polygon boundary")
    atlantis_lat, atlantis_lon = get_atlantis_centroid()
    logger.info(
        f"Derived Atlantis centroid | "
        f"lat={round(atlantis_lat, 6)} | "
        f"lon={round(atlantis_lon, 6)}"
    )


    logger.info("Select subsample of sr_hex.csv.gz that are within 1 minute (~1.852km) from the calculated centroid of Atlantis")    
    subsample_df = subsample_sr_hex(
        sr_hex_path=f"data/{SR_HEX_FILE}",
        atlantis_lat=atlantis_lat,
        atlantis_lon=atlantis_lon,
        radius_km=radius_km
    )
    
    logger.info(
        f"Atlantis subsample | "
        f"radius from centre={radius_km} | "
        f"SR's within subsample={len(subsample_df)}"
    )
    
    data_trans_atlantis_stage_runtime = round(time.time() - data_trans_atlantis_start_time, 3)
    logger.info(f"Created Atlantis subsample of SR hex dataset, no. of records in subsample: {{len(subsample_df)}}")
    logger.info(f"5.1 Atlantis subsampling stage completed in {data_trans_atlantis_stage_runtime} seconds")
    logger.info(f"Pipeline runtime {round(time.time() - pipeline_start_time, 3)} seconds")


    # -------------------
    # 5.2 Wind Enrichment 
    # -------------------
    logger.info("Wind enrichment stage initiated")
    data_trans_wind_start_time = time.time()

    logger.info("Loading wind dataset")
    wind_df = load_wind_data(wind_enrichment_max_retries,wind_enrichment_backoff_factor)
    
    logger.debug(f"Wind shape: {wind_df.shape}")    
    logger.debug(f"Wind columns: {list(wind_df.columns)}")
    logger.debug(f"Wind cleaned preview:\n{wind_df.head()}")
    
    # Wind Enrichment Stage    
    logger.info("Enrich the subsample Atlantis dataset with Wind data")
    
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
    logger.info("Join Atlantis subsample dataset with Wind data by nearest hour join")
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
        f"Wind enrichment completed | no. of enriched_records={len(enriched_df)}"
    )    
    data_trans_wind_stage_runtime = round(time.time() - data_trans_wind_start_time, 3)
    logger.info(f"Enriched Atlantis subsample data with wind data, no. of enriched_records={len(enriched_df)}")
    logger.info(f"5.2 Atlantis wind enrichment stage completed in {data_trans_wind_stage_runtime} seconds")
    logger.info(f"Pipeline runtime {round(time.time() - pipeline_start_time, 3)} seconds")

    
    # -------------------------------------------------
    # 5.3 Anonymisation Stage
    # -------------------------------------------------
    logger.info("Anonymisation stage initiated")
    data_trans_anon_start_time = time.time()
    
    # Ensure output directory exists at project root
    output_dir = Path("output")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info("Perform column anonymisation on enriched Atlantis subsample wind dataset")
    anonymised_df = anonymise_dataset(
    enriched_df,
    review_output_path=f"{output_dir}/removed_columns_manual_review.csv"
    )
    
    logger.info(
        f"Column anonymisation completed | "
        f"final_columns={list(anonymised_df.columns)}"
    )    
    
    logger.info("Perform records K-anonymity anonymisation on enriched Atlantis subsample wind dataset")
    final_anonymised_df, manual_review_df = apply_k_anonymity_filter(
        anonymised_df,
        spatial_col="h3_level8_index",
        time_col="creation_timestamp",
        k=3
    )
    
    logger.info("Save anonymised and review datasets to output directory")
    enriched_df.to_csv(f"{output_dir}/full_dataset_manual_review.csv", index=False)
    final_anonymised_df.to_csv(f"{output_dir}/final_anonymised_dataset.csv", index=False)
    manual_review_df.to_csv(f"{output_dir}/manual_review_high_risk_records.csv", index=False)
    logger.info(f"Final anonyised dataset created at {output_dir}/final_anonymised_dataset.csv")
    logger.info(f"Anonymised columns manual review file created at {output_dir}/removed_columns_manual_review.csv")
    logger.info(f"Anonymised high risk records manual review file created at {output_dir}/manual_review_high_risk_records.csv")
    logger.info(f"Full datset for full manual review file created at {output_dir}/full_dataset_manual_review.csv")
    
    logger.info("Anonymisation completed and files exported to output/")
    data_trans_anon_stage_runtime = round(time.time() - data_trans_anon_start_time, 3)
    logger.info(f"5.3 Anonymisation stage completed in {data_trans_anon_stage_runtime} seconds")
    logger.info(f"Pipeline runtime {round(time.time() - pipeline_start_time, 3)} seconds")
    
    logger.info("Pipeline Completed...........")

if __name__ == "__main__":
    run_pipeline()    