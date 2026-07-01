"""
==============================================================
Project Name : Healthcare_MDM

Module       : Process JISB Response

Author       : Naresh

Description
-----------
This module processes the raw NPPES Provider Registry API response and converts it into a standardized Provider record structure.

Business Flow

NPPES API Response
         │
         ▼
Read Provider Collection
         │
         ▼
Extract Required Attributes
         │
         ▼
Flatten Nested JSON Objects
         │
         ▼
Build Standardized Provider Record
         │
         ▼
Return Records for JISB Transformation

Responsibilities

1. Read provider records returned by the NPPES API.
2. Extract required provider attributes.
3. Flatten nested JSON objects.
4. Build standardized provider records.
5. Return standardized records for JISB transformation.

This Module Does NOT

• Call JISB APIs
• Perform Matching
• Perform Deduplication
• Apply Survivorship
• Create Golden Records
• Load Databricks
• Load Snowflake

==============================================================
"""

# ============================================================
# Import Logging Module
#
# Logging is used to capture informational messages, warnings and unexpected errors during execution.
#
# Production pipelines should always use logging instead of print statements.
# ============================================================

import logging


# ============================================================
# Configure Logging
#
# Log Level
# ---------
# INFO
#
# Log Format
# ----------
# Time
# Log Level
# Message
# ============================================================

logging.basicConfig(

    level=logging.INFO,

    format="%(asctime)s - %(levelname)s - %(message)s"

)

logger = logging.getLogger(__name__)


# ============================================================
# Process Raw NPPES API Response
#
# Purpose
# -------
# Convert the raw NPPES API response into a collection of standardized Provider records.
#
# Every Provider record is flattened so that downstream transformation modules can consume a consistent structure.
#
# Parameters
# ----------
#
# response_json
#
# Raw JSON response returned by the NPPES Registry API.
#
# Returns
# -------
#
# Dictionary
#
# {
#     searchResult
#         totalRecords
#         records
# }
#
# ============================================================

def process_jisb_response(response_json: dict) -> dict:

    try:

        # ----------------------------------------------------
        # Read the Provider collection returned by the API.
        #
        # If the response does not contain any Provider records, an empty list will be returned.
        # ----------------------------------------------------

        provider_results = response_json.get(

            "results",

            []

        )

        logger.info(

            "Total Provider Records Received : %s",

            len(provider_results)

        )

        # ----------------------------------------------------
        # Store all standardized Provider records.
        # ----------------------------------------------------

        transformed_records = []

        # ----------------------------------------------------
        # Process every Provider individually.
        # ----------------------------------------------------

        for provider in provider_results:

            # ------------------------------------------------
            # Read the Provider Basic Information.
            #
            # Contains
            #
            # Name
            # Gender
            # Credential
            # Status
            # Enumeration Date
            # Last Updated
            # ------------------------------------------------

            basic = provider.get(

                "basic",

                {}

            )

            # ------------------------------------------------
            # Read Provider Address Collection.
            #
            # One Provider can contain multiple addresses.
            #
            # Example
            #
            # Practice Address
            # Mailing Address
            # ------------------------------------------------

            addresses = provider.get(

                "addresses",

                []

            )

            # ------------------------------------------------
            # Read Provider Taxonomy Collection.
            #
            # One Provider can contain multiple specialties.
            # ------------------------------------------------

            taxonomies = provider.get(

                "taxonomies",

                []

            )

            # ------------------------------------------------
            # Read Alternate Provider Identifiers.
            # ------------------------------------------------

            identifiers = provider.get(

                "identifiers",

                []

            )

            # ------------------------------------------------
            # Select the Primary Address.
            #
            # If no address exists, use an empty dictionary.
            # ------------------------------------------------

            first_address = addresses[0] if addresses else {}

            # ------------------------------------------------
            # Select the Primary Taxonomy.
            #
            # If no taxonomy exists, use an empty dictionary.
            # ------------------------------------------------

            first_taxonomy = taxonomies[0] if taxonomies else {}

            # ------------------------------------------------
            # Select the First Alternate Identifier.
            #
            # If no identifier exists, use an empty dictionary.
            # ------------------------------------------------

            first_identifier = identifiers[0] if identifiers else {}

            # ------------------------------------------------
            # Build the Standardized Provider Record.
            #
            # Every attribute required by JISB is collected into one flat dictionary.
            # ------------------------------------------------

            provider_record = {

                "Provider ID": provider.get(
                    "number",
                    ""
                ),                                                  # National Provider Identifier (NPI)

                "First Name": basic.get(
                    "first_name",
                    ""
                ),                                                  # Provider First Name

                "Middle Name": basic.get(
                    "middle_name",
                    ""
                ),                                                  # Provider Middle Name

                "Last Name": basic.get(
                    "last_name",
                    ""
                ),                                                  # Provider Last Name

                "Full Name": " ".join(

                    filter(

                        None,

                        [

                            basic.get("first_name", ""),

                            basic.get("middle_name", ""),

                            basic.get("last_name", "")

                        ]

                    )

                ),                                                  # Construct the complete Provider Name

                "Credential": basic.get(
                    "credential",
                    ""
                ),                                                  # Professional Credential

                "Gender": basic.get(
                    "sex",
                    ""
                ),                                                  # Provider Gender

                "Status": basic.get(
                    "status",
                    ""
                ),                                                  # Provider Status

                "Enumeration Date": basic.get(
                    "enumeration_date",
                    ""
                ),                                                  # Initial NPI Registration Date

                "Last Updated": basic.get(
                    "last_updated",
                    ""
                ),                                                  # Last Profile Update Date

                "Address Line 1": first_address.get(
                    "address_1",
                    ""
                ),                                                  # Primary Practice Address

                "City": first_address.get(
                    "city",
                    ""
                ),                                                  # Practice City

                "State": first_address.get(
                    "state",
                    ""
                ),                                                  # Practice State

                "Postal Code": first_address.get(
                    "postal_code",
                    ""
                ),                                                  # ZIP / Postal Code

                "Country Code": first_address.get(
                    "country_code",
                    ""
                ),                                                  # ISO Country Code

                "Country Name": first_address.get(
                    "country_name",
                    ""
                ),                                                  # Country Name

                "Phone Number": first_address.get(
                    "telephone_number",
                    ""
                ),                                                  # Primary Contact Number

                "Fax Number": first_address.get(
                    "fax_number",
                    ""
                ),                                                  # Fax Number

                "Taxonomy Code": first_taxonomy.get(
                    "code",
                    ""
                ),                                                  # Provider Taxonomy Code

                "Taxonomy Description": first_taxonomy.get(
                    "desc",
                    ""
                ),                                                  # Provider Specialty Description

                "License Number": first_taxonomy.get(
                    "license",
                    ""
                ),                                                  # Professional License Number

                "License State": first_taxonomy.get(
                    "state",
                    ""
                ),                                                  # License Issuing State

                "Identifier": first_identifier.get(
                    "identifier",
                    ""
                ),                                                  # Alternate Provider Identifier

                "Identifier Issuer": first_identifier.get(
                    "issuer",
                    ""
                ),                                                  # Organization Issuing Identifier

                "Identifier Type": first_identifier.get(
                    "desc",
                    ""
                )                                                   # Identifier Description

            }

            # ------------------------------------------------
            # Add the standardized Provider record into the output collection.
            # ------------------------------------------------

            transformed_records.append(

                provider_record

            )

        # ----------------------------------------------------
        # Processing Completed Successfully.
        #
        # Return all standardized Provider records together with the total record count.
        # ----------------------------------------------------

        logger.info(

            "Provider Records Successfully Standardized : %s",

            len(transformed_records)

        )

        return {

            "searchResult": {

                "totalRecords": len(

                    transformed_records

                ),

                "records": transformed_records

            }

        }

    except Exception as exception:

        # ----------------------------------------------------
        # Log the complete exception together with the stack trace.
        #
        # This helps identify failures during production execution.
        # ----------------------------------------------------

        logger.exception(

            "Failed while processing the NPPES Provider Response."

        )

        raise