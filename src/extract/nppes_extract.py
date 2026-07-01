"""
==============================================================
Project Name : Healthcare_MDM

Module       : NPPES Provider Extraction

Author       : Naresh

Description  : This module extracts Healthcare Provider data from the official NPPES Provider Registry API.

Business Flow

NPPES Registry API
        │
        ▼
Python Extraction
        │
        ▼
Raw JSON File
        │
        ▼
JISB Transformation
        │
        ▼
Processed JSON File
        │
        ▼
AWS S3
        │
        ▼
Databricks Auto Loader
        │
        ▼
Bronze Layer
        │
        ▼
Silver Layer
        │
        ▼
Gold Layer
        │
        ▼
Snowflake

Responsibilities

1. Connect to the NPPES API.
2. Download Provider data.
3. Save the Raw API response.
4. Transform into JISB format.
5. Save the Processed payload.
6. Upload Raw & Processed files to AWS S3.

No Data Quality.
No Business Rules.
No Matching.
No MDM Processing.

==============================================================
"""

# ============================================================
# Standard Python Libraries
# ============================================================

import json                                        # Read and write JSON files
import os                                          # Handle operating system file paths
from datetime import datetime                      # Generate unique timestamps for every execution


# ============================================================
# Third Party Libraries
# ============================================================

import requests                                    # Send REST API requests
from dotenv import load_dotenv                     # Load all environment variables


# ============================================================
# Project Modules
# ============================================================

from src.transform.process_jisb_response import process_jisb_response

from src.transform.transform_to_jisb import transform_to_jisb

from src.utils.s3_upload import upload_to_s3


# ============================================================
# Load Environment Variables
#
# Purpose : Read all configuration values from the .env file.
# This keeps sensitive information outside the source code.
# ============================================================

load_dotenv()


# ============================================================
# NPPES API Configuration

# These values are loaded from the .env file. Business users can change search parameters without modifying the Python code.
# ============================================================

BASE_URL = os.getenv("NPPES_BASE_URL")

API_VERSION = os.getenv("NPPES_API_VERSION")

CITY = os.getenv("NPPES_CITY")

STATE = os.getenv("NPPES_STATE")

ENUMERATION_TYPE = os.getenv("NPPES_ENUMERATION_TYPE")

LIMIT = int(os.getenv("NPPES_LIMIT"))

SKIP = int(os.getenv("NPPES_SKIP"))


# ============================================================
# Batch Information
#
# Every execution creates:
#
# • Unique Batch ID
# • Unique Raw File Name
# • Unique Processed File Name
#
# Example :
#
# Batch ID
# BATCH_20260627_103045
#
# Raw File
# raw_20260627_103045.json
#
# Processed File
# processed_20260627_103045.json
#
# This is the standard approach followed in production Data Engineering projects to maintain historical batches.
# ============================================================

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

BATCH_ID = f"BATCH_{TIMESTAMP}"

RAW_FILE_NAME = f"raw_{TIMESTAMP}.json"

PROCESSED_FILE_NAME = f"processed_{TIMESTAMP}.json"


# ============================================================
# Local Storage Paths

# Raw Response Saved exactly as received from the API.

# Processed Response Saved after converting into JISB format.
# ============================================================

RAW_FILE_PATH = f"data/raw/{RAW_FILE_NAME}"

PROCESSED_FILE_PATH = f"data/processed/{PROCESSED_FILE_NAME}"

# ============================================================
# Extract Provider Data from the NPPES Registry API
#
# Purpose : Connect to the official NPPES Registry API and retrieve Healthcare Provider records based on the search criteria configured inside the .env file.
#
# The API response is returned as a Python Dictionary.
#
# Example Search
#
# City              : Baltimore
# State             : MD
# Enumeration Type  : NPI-1
# Limit             : 0
# Skip              : 0
#
# Returns
# -------
# Python Dictionary
# ============================================================

def get_provider_data():

    params = {

        "version": API_VERSION,                    # API Version

        "city": CITY,                              # Provider City

        "state": STATE,                            # Provider State

        "enumeration_type": ENUMERATION_TYPE,      # Individual Providers

        "limit": LIMIT,                            # Number of records per request

        "skip": SKIP,                              # Pagination offset

        "pretty": "on"                             # Pretty formatted JSON

    }

    response = requests.get(

        BASE_URL,

        params=params,

        timeout=30

    )

    # Raise an exception if the API request fails.

    response.raise_for_status()

    # Convert JSON response into a Python Dictionary.

    return response.json()


# ============================================================
# Save Raw API Response

# Purpose : Preserve the original response exactly as received from the NPPES API.

# No transformation. No filtering. No business logic.

# Output Example

# data/raw/
# raw_20260627_101530.json

# ============================================================

def save_raw_json(raw_response):

    # Create the Raw folder if it does not already exist.

    os.makedirs(

        "data/raw",

        exist_ok=True

    )

    # Write the complete API response into a JSON file.

    with open(

        RAW_FILE_PATH,

        "w",

        encoding="utf-8"

    ) as file:

        json.dump(

            raw_response,

            file,

            indent=4

        )


# ============================================================
# Transform Raw Provider Data into JISB Payload
#
# Purpose: Convert every Provider record from the NPPES response into the enterprise JISB payload format.
#
# This payload will later be consumed by ORIEO.
#
# Returns
# -------
# List of Processed JISB Provider Records
# ============================================================

def process_provider_data(raw_response):

    # Store every transformed Provider payload.

    processed_records = []

    # Extract Provider records from the Raw API response.

    provider_records = process_jisb_response(

        raw_response

    )

    # Convert every Provider into JISB format.

    for provider in provider_records["searchResult"]["records"]:

        jisb_payload = transform_to_jisb(

            provider

        )

        processed_records.append(

            jisb_payload

        )

    return processed_records


# ============================================================
# Save Processed JISB Payload
#
# Purpose : Store the transformed Provider payload before uploading it into AWS S3.
#
# Output Example :
#
# data/processed/
# processed_20260627_101530.json
#
# ============================================================
def save_processed_json(processed_response):

    """
    Save the processed JISB payload as a Bronze-ready JSON file.

    Responsibilities

    1. Create the processed folder if it does not exist.
    2. Wrap every JISB payload with ingestion metadata.
    3. Preserve the complete JISB payload inside PROVIDER_JSON.
    4. Save the Bronze-ready JSON file.
    """

    # ----------------------------------------------------------
    # Create the Processed folder if it does not already exist.
    # ----------------------------------------------------------

    os.makedirs(

        "data/processed",

        exist_ok=True

    )

    # ----------------------------------------------------------
    # Store Bronze-ready provider records.
    #
    # Each record contains:
    #
    # • Complete JISB Payload
    # • Source System
    # • Payload Format
    # • Target System
    # • Load Timestamp
    # • Load Date
    # • File Name
    # • Batch ID
    #
    # Databricks Bronze Auto Loader reads
    # these records directly.
    # ----------------------------------------------------------

    bronze_records = []

    for payload in processed_response:

        bronze_record = {

            "PROVIDER_JSON": json.dumps(

                payload

            ),

            "SOURCE_SYSTEM": "NPPES",

            "PAYLOAD_FORMAT": "JISB",

            "TARGET_SYSTEM": "ORIEO",

            "LOAD_TIMESTAMP": datetime.now().strftime(

                "%Y-%m-%d %H:%M:%S"

            ),

            "LOAD_DATE": datetime.now().strftime(

                "%Y-%m-%d"

            ),

            "FILE_NAME": PROCESSED_FILE_NAME,

            "BATCH_ID": BATCH_ID

        }

        bronze_records.append(

            bronze_record

        )

    # ----------------------------------------------------------
    # Save the Bronze-ready JSON records.
    #
    # Every provider record now contains
    # pipeline metadata required by the
    # Databricks Bronze layer.
    # ----------------------------------------------------------

    with open(

        PROCESSED_FILE_PATH,

        "w",

        encoding="utf-8"

    ) as file:

        json.dump(

            bronze_records,

            file,

            indent=4,

            ensure_ascii=False

        )
    # ----------------------------------------------------------
    # Display a success message after the processed
    # Bronze-ready JSON file is created successfully.
    # ----------------------------------------------------------

    print(

        f"Processed Bronze-ready JSON saved successfully : "

        f"{PROCESSED_FILE_PATH}"

    )

    # ----------------------------------------------------------
    # Return the processed file path.
    #
    # This path is used later while uploading
    # the processed JSON file into AWS S3.
    # ----------------------------------------------------------

    return PROCESSED_FILE_PATH        

# ============================================================
# Main Execution Flow
#
# Purpose : This function controls the complete Provider
# Extraction pipeline.
# ============================================================

def main():

    print("\n====================================================")
    print("Healthcare MDM Provider Extraction Started")
    print("====================================================\n")

    raw_response = get_provider_data()

    print("Provider data extracted successfully.")

    save_raw_json(raw_response)

    print(f"Raw File Saved : {RAW_FILE_NAME}")

    processed_response = process_provider_data(raw_response)

    print(f"Provider Records Processed : {len(processed_response)}")

    save_processed_json(processed_response)

    print(f"Processed File Saved : {PROCESSED_FILE_NAME}")

    upload_to_s3(

        RAW_FILE_PATH,

        f"raw/{RAW_FILE_NAME}"

    )

    print("Raw File Uploaded to AWS S3 Successfully.")

    upload_to_s3(

        PROCESSED_FILE_PATH,

        f"processed/{PROCESSED_FILE_NAME}"

    )

    print("Processed File Uploaded to AWS S3 Successfully.")

    print("\n====================================================")

    print(f"Batch ID          : {BATCH_ID}")

    print(f"Raw File          : {RAW_FILE_NAME}")

    print(f"Processed File    : {PROCESSED_FILE_NAME}")

    print(f"Total Providers   : {len(processed_response)}")

    print("AWS S3 Upload     : SUCCESS")

    print("Status            : COMPLETED")

    print("====================================================\n")


# ============================================================
# Application Entry Point
# ============================================================

if __name__ == "__main__":

    main()