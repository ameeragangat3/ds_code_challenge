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
import time

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

