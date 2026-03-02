# City of Cape Town -- Data Engineering Challenge

------------------------------------------------------------------------

## Executive Overview

This repository contains a full Data Engineering implementation of the
City of Cape Town technical challenge.

The solution demonstrates:

-   Efficient AWS S3 Select extraction
-   Schema-driven validation with quantitative conformance scoring
-   GeoJSON structural validation against reference dataset
-   H3 spatial indexing and assignment
-   Programmatic centroid computation (no hardcoded values)
-   Accurate geospatial filtering using the Haversine formula
-   Resilient ingestion of unreliable wind dataset with retry strategy
-   Structured anonymisation including k-anonymity
-   Comprehensive logging, validation thresholds, and runtime tracking


------------------------------------------------------------------------

# Project Structure

    ds_code_challenge-main/
    │
    ├── config.yaml
    ├── schemas/
    │   └── hex_schema.json
    ├── src/
    │   ├── ds_code_challenge_pipeline.py
    │   ├── data_extract_validate.py
    │   ├── data_transformation.py
    │   ├── data_transformation_extended.py
    │   ├── logging_config.py
    │   └── __init__.py
    ├── tests/
    │   ├── test_data_extract_validate.py
    │   ├── test_data_transformation.py
    │   ├── test_data_transformation_extended.py
    │   └── test_integration_pipeline.py
    │
    ├── data/      # raw input data
    ├── output/    # generated datasets
    ├── requirements.txt
    └── README.md

------------------------------------------------------------------------

# Setup & Execution

## Environment Setup

Recommended Python version: `3.11` (project dependencies are pinned in `requirements.txt`).

``` bash
python3 -m venv venv
source venv/bin/activate
python3 --version
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

## Run Pipeline

``` bash
python3 -m src.ds_code_challenge_pipeline
```

Outputs are written to:

    output/

## Run Unit Tests

``` bash
python3 -m unittest discover -s tests -v
```

## Run Integration Tests Only

``` bash
python3 -m unittest tests.test_integration_pipeline -v
```

------------------------------------------------------------------------

# Architecture Overview

## 1. End-to-End Data Flow

    S3: city-hex-polygons-8-10.geojson
        ↓
    S3 Select (Resolution = 8)
        ↓
    Schema Validation + Conformance Score
        ↓
    Validation Against city-hex-polygons-8.geojson
        ↓
    Load sr.csv.gz
        ↓
    H3 Spatial Assignment (Resolution 8)
        ↓
    Validation Against sr_hex.csv.gz
        ↓
    Atlantis Boundary Download (Overpass API)
        ↓
    Centroid Computation
        ↓
    Haversine Radius Filtering (1 Arc Minute ≈ 1.852 km)
        ↓
    Wind Dataset Download (Retry Strategy)
        ↓
    Wind Cleaning & Temporal Join
        ↓
    Anonymisation
        ↓
    K-Anonymity (k = 3)
        ↓
    Final Outputs (anonymised + manual review)

------------------------------------------------------------------------

## 2. Validation Flow

    Extracted GeoJSON
        ↓
    Collection-Level Rules
        ↓
    Feature-Level Rules
        ↓
    Geometry Rules
        ↓
    Property Rules
        ↓
    Conformance Score Calculation
        ↓
    Score ≥ Threshold ?
            → Yes → Continue Pipeline
            → No  → Fail Execution
    
    Conformance score formula:
    
        (total_rule_checks - total_failures) / total_rule_checks
    
    All rule counts, failure breakdowns, and thresholds are logged.

------------------------------------------------------------------------

## 3. Anonymisation Flow

    Filtered Atlantis Subsample
        ↓
    Remove Direct Identifiers
        ↓
    Reduce Spatial Precision (H3 Resolution 8 ≈ 500m)
        ↓
    Reduce Temporal Precision (6-hour buckets)
        ↓
    Apply K-Anonymity (k = 3, grouped by H3)
        ↓
    Group Size ≥ 3 ?
            → Yes → Release Record
            → No  → Export to Manual Review
------------------------------------------------------------------------

# Section 1 -- Extraction & Validation

### S3 Select

Query used:

    select * from s3object[*].features[*] s 
    where s.properties.resolution = 8

This minimises transfer and improves performance.

### Validation Against Reference Dataset

Ensures:

-   Matching feature counts
-   Matching H3 index sets
-   Resolution consistency
-   Structural integrity

### Schema-Based Validation

Rules enforced include:

-   GeoJSON type
-   Feature type
-   Geometry type
-   Coordinates structure
-   Resolution presence & value
-   Index presence & type

Conformance threshold is configurable and non-binary.

------------------------------------------------------------------------

# Section 2 -- Spatial Assignment

H3 resolution 8 computed using:

    h3.geo_to_h3(latitude, longitude, 8)

Missing coordinates → index = 0.

Validation compares computed values to `sr_hex.csv.gz`.

Join failure thresholds are logged and enforced.

------------------------------------------------------------------------

# Section 5 -- Further Transformations

## Atlantis Centroid

-   Boundary downloaded programmatically via Overpass API
-   Centroid computed using Shapely geometry operations
-   No hardcoding used

## Radius Calculation

1 arc minute = 1/60 degree ≈ 1.852 km.

Used as filtering radius.

## Haversine Distance

Used for great-circle distance:

    d = 2R * arcsin(√a)

Ensures accurate spherical calculation.

------------------------------------------------------------------------

## Wind Dataset Handling

Wind dataset downloaded programmatically.

### Retry Strategy

-   Exponential backoff
-   Maximum retry attempts
-   Timeout handling
-   Controlled failure

Rationale: ensures resilience against unreliable endpoint.

### Wind Data Retry Strategy Flow

    Attempt Wind Download
        ↓
    Success ?
            → Yes → Continue Processing
            → No  → Wait (Exponential Backoff)
                    ↓
                Retry Limit Reached ?
                    → Yes → Fail Cleanly
                    → No  → Retry Download

Wind joined using `pandas.merge_asof` with 1-hour tolerance.

------------------------------------------------------------------------

# Anonymisation Justification

## Summary

- Spatial precision reduced to H3 resolution 8 (~500m)
- Temporal precision reduced to 6-hour buckets
- Direct identifiers removed
- K-anonymity (`k = 3`) applied on spatial identifier
- Records failing k-anonymity exported for manual review

This balances privacy and analytical utility.

## Justification Discussion

The objective of the anonymisation process was to reduce re-identification risk while preserving analytical utility for spatial and environmental analysis.

### 1. Spatial Precision Control

Exact latitude and longitude coordinates were removed. Instead, location was represented using the H3 spatial indexing system at resolution level 8. At this resolution, each hexagon represents an area of approximately 0.5 km² (~500m spatial precision).

This satisfies the requirement of preserving location accuracy to within approximately 500m while preventing precise geolocation of individual service requests.

### 2. Temporal Precision Control

The `creation_timestamp` field was rounded down to 6-hour intervals using time bucketing. This preserves temporal accuracy within 6 hours and reduces linkage risk from exact timestamps.

Wind data was joined using the nearest hourly observation and aligned to this temporal precision.

### 3. Removal of Direct and Indirect Identifiers

Columns that could contribute to identifying an individual resident (for example, reference numbers, notification numbers, raw coordinates, and distance from centroid) were removed from the released dataset.

Operational fields such as directorate, department, and request codes were retained, since they describe service characteristics rather than personal attributes and are necessary for infrastructure and service analysis.

### 4. K-Anonymity Enforcement

To further mitigate re-identification risk, k-anonymity with `k = 3` was applied using the H3 resolution 8 index as the quasi-identifier.

Any hexagon containing fewer than three service requests in the filtered dataset was considered high risk and removed from the released dataset. These records were exported to a separate manual review file for controlled human-led anonymisation.

### 5. Privacy-Utility Balance

Applied transformations:

- Remove exact location precision
- Reduce temporal granularity
- Suppress rare spatial records

Preserved utility:

- Suburb-level infrastructure insights
- Environmental augmentation (wind)
- Service delivery analytics capability

The final dataset meets the required spatial (~500m) and temporal (<=6 hours) precision constraints and implements structural safeguards (k-anonymity and suppression) to significantly reduce re-identification risk while maintaining analytical value.

------------------------------------------------------------------------

# Logging & Performance

Each stage logs:

-   Start & end time
-   Runtime
-   Record counts
-   Validation metrics
-   Threshold breaches

------------------------------------------------------------------------

# Outputs

Generated in:

    output/

Includes:

-   final_anonymised_dataset.csv
-   manual_review_high_risk_records.csv
-   removed_columns_manual_review.csv
-   full_dataset_manual_review.csv

------------------------------------------------------------------------
