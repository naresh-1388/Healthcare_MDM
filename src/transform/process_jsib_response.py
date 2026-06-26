import logging                                                      # Import logging module for recording application events


logging.basicConfig(
    level=logging.INFO,                                             # Configure INFO level logging
    format="%(asctime)s - %(levelname)s - %(message)s"              # Configure standard log format
)

logger = logging.getLogger(__name__)                                # Create logger instance for this module


def process_jsib_response(response_json: dict) -> dict:
    """
    Process the raw NPPES API response.

    Responsibilities

    1. Read provider records from the NPPES API.
    2. Extract all required provider attributes.
    3. Flatten nested JSON objects.
    4. Build a standardized provider object.
    5. Return standardized records for JSIB transformation.

    This module does NOT

    - Call JSIB APIs
    - Perform Deduplication
    - Perform Matching
    - Create Golden Records
    - Load Databricks
    - Load Snowflake
    """

    try:

        provider_results = response_json.get(
            "results",
            []
        )                                                            # Read provider collection returned by the API

        transformed_records = []                                     # Store standardized provider records

        for provider in provider_results:                            # Process every provider individually

            basic = provider.get(
                "basic",
                {}
            )                                                        # Read provider basic information

            addresses = provider.get(
                "addresses",
                []
            )                                                        # Read provider address collection

            taxonomies = provider.get(
                "taxonomies",
                []
            )                                                        # Read provider taxonomy collection

            identifiers = provider.get(
                "identifiers",
                []
            )                                                        # Read provider identifier collection

            first_address = addresses[0] if addresses else {}        # Read primary provider address

            first_taxonomy = taxonomies[0] if taxonomies else {}      # Read primary taxonomy

            first_identifier = identifiers[0] if identifiers else {} # Read first alternate identifier

            provider_record = {

                "Provider ID": provider.get(
                    "number",
                    ""
                ),                                                    # National Provider Identifier (NPI)

                "First Name": basic.get(
                    "first_name",
                    ""
                ),                                                    # Provider first name

                "Middle Name": basic.get(
                    "middle_name",
                    ""
                ),                                                    # Provider middle name

                "Last Name": basic.get(
                    "last_name",
                    ""
                ),                                                    # Provider last name

                "Full Name": " ".join(
                    filter(
                        None,
                        [
                            basic.get("first_name", ""),
                            basic.get("middle_name", ""),
                            basic.get("last_name", "")
                        ]
                    )
                ),                                                    # Build complete provider name

                "Credential": basic.get(
                    "credential",
                    ""
                ),                                                    # Professional credential

                "Gender": basic.get(
                    "sex",
                    ""
                ),                                                    # Provider gender

                "Status": basic.get(
                    "status",
                    ""
                ),                                                    # Provider status

                "Enumeration Date": basic.get(
                    "enumeration_date",
                    ""
                ),                                                    # Initial NPI registration date

                "Last Updated": basic.get(
                    "last_updated",
                    ""
                ),                                                    # Last profile update date

                "Address Line 1": first_address.get(
                    "address_1",
                    ""
                ),                                                    # Primary address line

                "City": first_address.get(
                    "city",
                    ""
                ),                                                    # Provider city

                "State": first_address.get(
                    "state",
                    ""
                ),                                                    # Provider state

                "Postal Code": first_address.get(
                    "postal_code",
                    ""
                ),                                                    # Postal code

                "Country Code": first_address.get(
                    "country_code",
                    ""
                ),                                                    # ISO country code

                "Country Name": first_address.get(
                    "country_name",
                    ""
                ),                                                    # Country name

                "Phone Number": first_address.get(
                    "telephone_number",
                    ""
                ),                                                    # Primary contact number

                "Fax Number": first_address.get(
                    "fax_number",
                    ""
                ),                                                    # Fax number

                "Taxonomy Code": first_taxonomy.get(
                    "code",
                    ""
                ),                                                    # Provider taxonomy code

                "Taxonomy Description": first_taxonomy.get(
                    "desc",
                    ""
                ),                                                    # Provider specialty

                "License Number": first_taxonomy.get(
                    "license",
                    ""
                ),                                                    # Professional license number

                "License State": first_taxonomy.get(
                    "state",
                    ""
                ),                                                    # License issuing state

                "Identifier": first_identifier.get(
                    "identifier",
                    ""
                ),                                                    # Alternate provider identifier

                "Identifier Issuer": first_identifier.get(
                    "issuer",
                    ""
                ),                                                    # Organization issuing identifier

                "Identifier Type": first_identifier.get(
                    "desc",
                    ""
                )                                                     # Identifier description

            }

            transformed_records.append(
                provider_record
            )

        return {

            "searchResult": {

                "totalRecords": len(
                    transformed_records
                ),                                                    # Store total number of standardized provider records

                "records": transformed_records                         # Return the complete standardized provider collection

            }

        }

    except Exception:

        logger.exception(                                              # Log the complete exception with stack trace
            "Failed while processing the NPPES provider response."
        )

        raise                                                         # Re-raise the exception to stop pipeline execution                                                         # Add standardized provider record into the output collection