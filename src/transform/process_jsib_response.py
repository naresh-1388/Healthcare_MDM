import logging                                                           # Import logging module for application logging


logging.basicConfig(
    level=logging.INFO,                                                  # Log INFO level and above messages
    format="%(asctime)s - %(levelname)s - %(message)s"                   # Define standard log message format
)

logger = logging.getLogger(__name__)                                     # Create logger object for this module


def process_jsib_response(response_json: dict) -> dict:
    """
    Process the raw NPPES API response and convert it into
    the standard JSIB provider structure.

    Responsibilities:
        1. Read provider records from the API response.
        2. Extract only required business fields.
        3. Flatten nested JSON objects.
        4. Create a standard provider object.

    This function DOES NOT:

        - Perform deduplication.
        - Apply business rules.
        - Load data into Databricks.
        - Save JSON files.
    """

    try:

        results = response_json.get("results", []) or []                 # Read provider records returned by the API

        transformed_records = []                                         # Store transformed provider records

        for provider in results:                                         # Process every provider individually

            basic = provider.get("basic", {}) or {}                      # Read basic provider information safely

            provider_id = provider.get("number", "")                     # Read National Provider Identifier (NPI)

            first_name = basic.get("first_name", "")                     # Read provider first name

            middle_name = basic.get("middle_name", "")                   # Read provider middle name

            last_name = basic.get("last_name", "")                       # Read provider last name

            gender = basic.get("sex", "")                                # Read provider gender

            status = basic.get("status", "")                             # Read provider active status

            full_name = " ".join(                                        # Build complete provider name
                filter(
                    None,
                    [
                        first_name,
                        middle_name,
                        last_name
                    ]
                )
            )

            hcp_addresses = []                                           # Store all provider addresses

            addresses = provider.get("addresses", []) or []              # Read address collection

            for address in addresses:                                    # Process every provider address

                address_record = {

                    "Address Line 1": address.get(
                        "address_1", ""
                    ),

                    "City": address.get(
                        "city", ""
                    ),

                    "State": address.get(
                        "state", ""
                    ),

                    "Postal Code": address.get(
                        "postal_code", ""
                    ),

                    "Country": {

                        "Code": address.get(
                            "country_code", ""
                        ),

                        "Name": address.get(
                            "country_name", ""
                        )

                    },

                    "Phone": address.get(
                        "telephone_number", ""
                    ),

                    "Fax": address.get(
                        "fax_number", ""
                    ),

                    "Address Purpose": address.get(
                        "address_purpose", ""
                    ),

                    "Address Type": address.get(
                        "address_type", ""
                    )

                }

                hcp_addresses.append(address_record)                     # Add formatted address into provider address list
                specialties = []                                             # Store provider specialties

            taxonomies = provider.get("taxonomies", []) or []            # Read taxonomy collection

            for taxonomy in taxonomies:                                  # Process every provider specialty

                specialty = {

                    "Code": taxonomy.get(
                        "code", ""
                    ),

                    "Description": taxonomy.get(
                        "desc", ""
                    ),

                    "License": taxonomy.get(
                        "license", ""
                    ),

                    "State": taxonomy.get(
                        "state", ""
                    ),

                    "Primary": taxonomy.get(
                        "primary", False
                    )

                }

                specialties.append(specialty)                            # Add specialty into specialty collection

            alternate_identifiers = []                                   # Store alternate identifiers

            identifiers = provider.get("identifiers", []) or []          # Read alternate identifiers

            for identifier in identifiers:                               # Process every identifier

                identifier_record = {

                    "Identifier Type": identifier.get(
                        "desc", ""
                    ),

                    "Identifier Value": identifier.get(
                        "identifier", ""
                    ),

                    "Issuer": identifier.get(
                        "issuer", ""
                    ),

                    "State": identifier.get(
                        "state", ""
                    )

                }

                alternate_identifiers.append(identifier_record)           # Add identifier into identifier collection

            provider_record = {

                "Provider ID": provider_id,

                "First Name": first_name,

                "Middle Name": middle_name,

                "Full Name": full_name,

                "Last Name": last_name,

                "Gender": gender,

                "Status": status,

                "HCP Address": hcp_addresses,

                "Specialty": specialties,

                "AlternateIdentifier": alternate_identifiers

            }

            transformed_records.append(provider_record)                  # Store transformed provider record

        return {

            "searchResult": {

                "totalRecords": len(transformed_records),

                "records": transformed_records

            }

        }

    except Exception as exception:

        logger.error(
            f"Error while processing JSIB response : {str(exception)}",
            exc_info=True
        )

        raise