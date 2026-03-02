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
    │
    ├── data/      # raw input data
    ├── output/    # generated datasets
    ├── requirements.txt
    └── README.md

------------------------------------------------------------------------

# Setup & Execution

## Environment Setup

``` bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Run Pipeline

``` bash
python -m src.ds_code_challenge_pipeline
```

Outputs are written to:

    output/

------------------------------------------------------------------------

# Architecture Overview

## 1. End-to-End Data Flow

+------------------------------------------------------+
|            S3: city-hex-polygons-8-10.geojson       |
+------------------------------------------------------+
                        |
                        v
+------------------------------------------------------+
|               S3 Select Extraction                  |
+------------------------------------------------------+
                        |
                        v
+------------------------------------------------------+
|              Schema-Based Validation                |
|      (GeoJSON rules + Conformance Scoring)         |
+------------------------------------------------------+
                        |
                        v
+------------------------------------------------------+
|  Validation Against city-hex-polygons-8.geojson     |
+------------------------------------------------------+
                        |
                        v
+------------------------------------------------------+
|              Load Service Requests (sr.csv.gz)      |
+------------------------------------------------------+
                        |
                        v
+------------------------------------------------------+
|           H3 Spatial Assignment (Resolution 8)      |
+------------------------------------------------------+
                        |
                        v
+------------------------------------------------------+
|       Validation Against sr_hex.csv.gz (Match Rate) |
+------------------------------------------------------+
                        |
                        v
+------------------------------------------------------+
|     Atlantis Boundary Download (Overpass API)       |
+------------------------------------------------------+
                        |
                        v
+------------------------------------------------------+
|         Centroid Computation (Shapely)              |
+------------------------------------------------------+
                        |
                        v
+------------------------------------------------------+
|      Haversine Distance Filtering (1 Arc Minute)    |
+------------------------------------------------------+
                        |
                        v
+------------------------------------------------------+
|       Wind Dataset Download (Retry Strategy)        |
+------------------------------------------------------+
                        |
                        v
+------------------------------------------------------+
|         Wind Data Cleaning & Temporal Join          |
+------------------------------------------------------+
                        |
                        v
+------------------------------------------------------+
|                 Anonymisation Stage                 |
+------------------------------------------------------+
                        |
                        v
+------------------------------------------------------+
|           K-Anonymity Enforcement (k = 3)           |
+------------------------------------------------------+
                        |
                        v
+------------------------------------------------------+
|                    Final Outputs                    |
|     - final_anonymised.csv                          |
|     - manual_review.csv                             |
+------------------------------------------------------+

------------------------------------------------------------------------

## 2. Validation Flow

                +---------------------------+
                |     Extracted GeoJSON     |
                +---------------------------+
                              |
                              v
                +---------------------------+
                |   Collection-Level Rules  |
                | - GeoJSON type check      |
                | - Features list check     |
                +---------------------------+
                              |
                              v
                +---------------------------+
                |     Feature-Level Rules   |
                | - Feature type            |
                | - Properties existence    |
                +---------------------------+
                              |
                              v
                +---------------------------+
                |       Geometry Rules      |
                | - Geometry type           |
                | - Coordinates structure   |
                +---------------------------+
                              |
                              v
                +---------------------------+
                |       Property Rules      |
                | - Resolution present      |
                | - Resolution == 8         |
                | - Index present & type    |
                +---------------------------+
                              |
                              v
                +---------------------------+
                |   Conformance Score Calc  |
                | (Checks - Failures)/Total |
                +---------------------------+
                              |
                              v
                     +----------------+
                     | Score >= Threshold? |
                     +----------------+
                        |            |
                        v            v
              +----------------+   +----------------+
              | Continue       |   | Fail Execution |
              | Pipeline       |   | with Error     |
              +----------------+   +----------------+

Conformance score formula:

    (total_rule_checks - total_failures) / total_rule_checks

All rule counts, failure breakdowns, and thresholds are logged.

------------------------------------------------------------------------

## 3. Anonymisation Flow

+--------------------------------------------------+
|         Filtered Atlantis Subsample              |
+--------------------------------------------------+
                    |
                    v
+--------------------------------------------------+
|      Remove Direct Identifiers                   |
|  - notification_number                           |
|  - reference_number                              |
|  - raw latitude/longitude                        |
+--------------------------------------------------+
                    |
                    v
+--------------------------------------------------+
|      Spatial Precision Reduction                 |
|  - Use H3 Resolution 8 (~500m precision)        |
+--------------------------------------------------+
                    |
                    v
+--------------------------------------------------+
|      Temporal Precision Reduction                |
|  - Round timestamps to 6-hour buckets            |
+--------------------------------------------------+
                    |
                    v
+--------------------------------------------------+
|      Apply K-Anonymity (k = 3)                   |
|  - Group by H3 index                             |
+--------------------------------------------------+
                    |
                    v
         +-------------------------------+
         | Group Size >= 3 ?             |
         +-------------------------------+
              |                      |
              v                      v
+------------------------+   +---------------------------+
| Release Record         |   | Flag for Manual Review    |
| (Include in final set) |   | Export to review dataset  |
+------------------------+   +---------------------------+

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

### WInd Data Retry Strategy Flow
+--------------------------------------------+
|  Attempt Wind Dataset Download             |
+--------------------------------------------+
                |
                v
        +------------------+
        | Success?         |
        +------------------+
           |           |
           v           v
+----------------+   +---------------------------+
| Continue       |   | Wait (Exponential Backoff)|
| Processing     |   | Increase Retry Counter    |
+----------------+   +---------------------------+
                                |
                                v
                      +-----------------------+
                      | Retry Limit Reached? |
                      +-----------------------+
                         |              |
                         v              v
                 +----------------+  +------------------+
                 | Raise Error    |  | Retry Download   |
                 | (Fail Cleanly) |  +------------------+
                 +----------------+

Wind joined using `pandas.merge_asof` with 1-hour tolerance.

------------------------------------------------------------------------

# Anonymisation Justification

Summary:
- Spatial precision reduced to H3 resolution 8 (\~500m).\
Temporal precision reduced to 6-hour buckets.\
Direct identifiers removed.\
K-anonymity (k=3) applied on spatial identifier.
Records failing k-anonymity exported for manual review.
This balances privacy and analytical utility.

Justification Discussion:
The objective of the anonymisation process was to reduce the risk of re-identification while preserving analytical utility for spatial and environmental analysis.

1. Spatial Precision Control

Exact latitude and longitude coordinates were removed from the dataset. Instead, location was represented using the H3 spatial indexing system at resolution level 8. At this resolution, each hexagon represents an area of approximately 0.5 km² (≈500m spatial precision). This satisfies the requirement of preserving location accuracy to within approximately 500m, while preventing precise geolocation of individual service requests.

Using H3 also ensures consistent spatial aggregation and avoids the disclosure risk associated with raw coordinates.

2. Temporal Precision Control

The creation_timestamp field was rounded down to 6-hour intervals using time bucketing. This ensures temporal accuracy is preserved within 6 hours, as required. Exact timestamps were removed to prevent linkage attacks that could combine public knowledge of an event time with service request data.

The wind data was joined using the nearest hourly observation and similarly aligned to this temporal resolution.

3. Removal of Direct and Indirect Identifiers

Columns that could contribute to identifying an individual resident (e.g., reference numbers, notification numbers, raw coordinates, distance from centroid) were removed from the released dataset.

Operational fields such as directorate, department, and request codes were retained, as they describe service characteristics rather than personal attributes. These fields are necessary for meaningful infrastructure and service analysis.

4. K-Anonymity Enforcement

To further mitigate re-identification risk, k-anonymity with k = 3 was applied using the H3 resolution 8 index as the quasi-identifier.

Any hexagon containing fewer than three service requests in the filtered dataset was considered high risk and removed from the released dataset. These records were exported to a separate manual review file for controlled, human-led anonymisation.

This ensures that no released record can be uniquely identified based on spatial grouping alone.

5. Privacy–Utility Balance

The applied transformations:

Remove exact location precision

Reduce temporal granularity

Suppress rare spatial records

while preserving:

Suburb-level infrastructure insights

Environmental augmentation (wind)

Service delivery analytics capability

The final dataset therefore meets the required spatial (~500m) and temporal (≤6 hours) precision constraints and implements structural safeguards (k-anonymity and suppression) to significantly reduce re-identification risk while maintaining analytical value.
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

-   final_anonymised.csv
-   manual_review.csv

------------------------------------------------------------------------

