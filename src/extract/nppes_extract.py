"""
=============================================
Project Name : Healthcare_MDM
Module       : NPPES Provider Extraction
Author       : Naresh
Description  : This module is responsible for extracting Healthcare Provider data from the official NPPES Provider Registry API.

Business Flow:
    NPPES API
        ↓
    Python Extraction
        ↓
    Databricks Bronze Layer
        ↓
    Databricks Silver Layer
        ↓
    Databricks Gold Layer
        ↓
    Snowflake

This module ONLY extracts raw data.

No transformation.
No deduplication.
No business rules.

==================================
"""
import json                                                             # Used to read and write JSON files
import os                                                               # Used to build platform-independent file paths

import requests                                                         # Used to call the NPPES REST API
from dotenv import load_dotenv                                          # Used to load environment variables

from src.transform.process_jsib_response import process_jsib_response
from src.transform.transform_to_jsib import transform_to_jsib

load_dotenv()                                                           # Load all values from .env file


BASE_URL = os.getenv("NPPES_BASE_URL")                                  # Read NPPES API URL
API_VERSION = os.getenv("NPPES_API_VERSION")                            # Read API version


RAW_FILE_PATH = "data/raw/nppes_raw_response.json"                      # Raw API response file
PROCESSED_FILE_PATH = "data/processed/processed_response.json"          # Processed JSIB payload file


def get_provider_data():
    """
    Extract provider data from the NPPES API.
    """

    params = {
        "version": API_VERSION,                                         # Mandatory API version
        "city": "Baltimore",                                            # Sample search criteria
        "limit": 10,                                                    # Number of provider records
        "pretty": "on"                                                  # Pretty JSON response
    }

    response = requests.get(
        BASE_URL,
        params=params,
        timeout=30
    )

    response.raise_for_status()                                         # Raise exception for HTTP errors

    return response.json()                                              # Return API response as Python dictionary


def save_raw_json(raw_response):
    """
    Save the raw NPPES API response.
    """

    os.makedirs("data/raw", exist_ok=True)                              # Create raw folder if it does not exist

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


def save_processed_json(processed_response):
    """
    Save the transformed JSIB payload.
    """

    os.makedirs("data/processed", exist_ok=True)                        # Create processed folder if it does not exist

    with open(
        PROCESSED_FILE_PATH,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            processed_response,
            file,
            indent=4
        )


def process_provider_data(raw_response):
    """
    Convert the raw NPPES response into the JSIB payload.
    """

    processed_records = []                                              # Store transformed provider payloads

    provider_records = process_jsib_response(raw_response)              # Extract provider information

    for provider in provider_records["searchResult"]["records"]:

        jsib_payload = transform_to_jsib(provider)                      # Convert provider into JSIB payload

        processed_records.append(jsib_payload)                          # Store transformed payload

    return processed_records


def main():
    """
    Main execution flow.
    """

    raw_response = get_provider_data()                                  # Extract provider data from NPPES API

    save_raw_json(raw_response)                                         # Save raw API response

    processed_response = process_provider_data(raw_response)            # Process and transform provider records

    save_processed_json(processed_response)                             # Save processed provider payload

    print("Provider extraction completed successfully.")


if __name__ == "__main__":

    main()