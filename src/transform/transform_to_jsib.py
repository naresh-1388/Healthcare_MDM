"""
Transform the standardized provider record into the JSIB payload format.

Responsibilities

1. Read standardized provider records.
2. Build the enterprise JSIB payload.
3. Apply field matching configuration.
4. Return the payload for downstream processing.

This module does NOT

- Call JSIB APIs
- Load Databricks
- Load Snowflake
- Perform Deduplication
"""


# COUNTRY TO CODBASE MAPPING

COUNTRY_TO_CODBASE = {

    "india": "IN",

    "united states": "US",

    "usa": "US",

    "canada": "CA",

    "uk": "GB"

}


# JSIB FIELD MAPPING

# Source Field
# Target Field
# Match Method
# Fuzzy Precision

JSIB_FIELD_MAPPING = [

    ("Provider ID", "ProviderID", "EXACT", 100),

    ("First Name", "FirstName", "EXACT", 100),

    ("Middle Name", "MiddleName", "EXACT", 100),

    ("Last Name", "LastName", "EXACT", 100),

    ("Full Name", "FullName", "FUZZY", 95),

    ("Credential", "Credential", "EXACT", 100),

    ("Gender", "Gender", "EXACT", 100),

    ("Status", "Status", "EXACT", 100),

    ("Enumeration Date", "EnumerationDate", "EXACT", 100),

    ("Last Updated", "LastUpdated", "EXACT", 100),

    ("Address Line 1", "AddressLine1", "FUZZY", 90),

    ("City", "City", "FUZZY", 90),

    ("State", "State", "EXACT", 100),

    ("Postal Code", "PostalCode", "EXACT", 100),

    ("Country Code", "CountryCode", "EXACT", 100),

    ("Country Name", "CountryName", "EXACT", 100),

    ("Phone Number", "PhoneNumber", "EXACT", 100),

    ("Fax Number", "FaxNumber", "EXACT", 100),

    ("Taxonomy Code", "TaxonomyCode", "EXACT", 100),

    ("Taxonomy Description", "TaxonomyDescription", "FUZZY", 90),

    ("License Number", "LicenseNumber", "EXACT", 100),

    ("License State", "LicenseState", "EXACT", 100),

    ("Identifier", "Identifier", "EXACT", 100),

    ("Identifier Issuer", "IdentifierIssuer", "EXACT", 100),

    ("Identifier Type", "IdentifierType", "EXACT", 100)

]
def transform_to_jsib(provider: dict) -> dict:
    """
    Transform a standardized provider record into the enterprise JSIB payload.

    Responsibilities

    1. Read standardized provider fields.
    2. Build JSIB field collection.
    3. Apply configured matching rules.
    4. Return the complete JSIB payload.
    """

    provider = provider or {}                                          # Prevent NoneType errors by using an empty dictionary

    country_name = (
        provider.get(
            "Country Name",
            ""
        )
        .strip()
        .lower()
    )                                                                  # Read provider country for CodBase mapping

    cod_base = COUNTRY_TO_CODBASE.get(
        country_name,
        "US"
    )                                                                  # Default to US if the country is not configured

    payload = {

        "resultSize": "20",

        "entityType": "Activity",

        "codBases": [

            cod_base

        ],

        "fields": []

    }                                                                  # Create the base JSIB payload structure

    for source_field, target_field, method, precision in JSIB_FIELD_MAPPING:

        value = provider.get(
            source_field,
            ""
        )                                                              # Read the standardized provider value

        if value is None:

            value = ""                                                 # Replace NULL values with an empty string

        payload["fields"].append(

            {

                "method": method,

                "name": target_field,

                "values": [

                    str(value)

                ],

                "fuzzyPrecision": precision

            }

        )                                                              # Append one JSIB field definition into the payload

    return payload                                                     # Return the completed JSIB payload