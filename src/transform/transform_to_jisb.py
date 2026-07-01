"""
==============================================================
Project Name : Healthcare_MDM

Module       : JISB Payload Transformation

Author       : Naresh

Description
-----------
This module converts a standardized Healthcare Provider record into the enterprise JISB payload format.

Business Flow

Standardized Provider Record
            │
            ▼
Read Standardized Fields
            │
            ▼
Apply JISB Field Mapping
            │
            ▼
Build Enterprise JISB Payload
            │
            ▼
Return Payload to Python Extraction Module

Responsibilities

1. Read standardized provider fields.
2. Build the enterprise JISB payload.
3. Apply configured field matching rules.
4. Apply fuzzy matching precision.
5. Convert provider data into the format
   expected by the downstream MDM system.

This Module Does NOT

• Call external APIs
• Load Databricks
• Load Snowflake
• Perform Deduplication
• Apply Survivorship
• Execute Business Rules

==============================================================
"""


# ============================================================
# Country to CodBase Mapping
#
# JISB requires a Country Code Base (CodBase)
# to determine which country-specific reference
# data should be used while processing records.
#
# If a provider country is not configured,
# the default CodBase will be US.
# ============================================================

COUNTRY_TO_CODBASE = {

    "india": "IN",

    "united states": "US",

    "usa": "US",

    "canada": "CA",

    "uk": "GB"

}


# ============================================================
# JISB Field Mapping Configuration
#
# Every tuple contains
#
# Source Field
# Target JISB Field
# Matching Method
# Fuzzy Matching Precision
#
# Matching Methods
#
# EXACT
# Exact value comparison.
#
# FUZZY
# Similarity comparison using
# the configured precision score.
#
# This configuration allows the payload
# generation logic to remain generic.
#
# Any new field can be added here without
# modifying the transformation logic.
# ============================================================

JISB_FIELD_MAPPING = [

    ("Provider ID","ProviderID","EXACT",100),

    ("First Name","FirstName","EXACT",100),

    ("Middle Name","MiddleName","EXACT",100),

    ("Last Name","LastName","EXACT",100),

    ("Full Name","FullName","FUZZY",95),

    ("Credential","Credential","EXACT",100),

    ("Gender","Gender","EXACT",100),

    ("Status","Status","EXACT",100),

    ("Enumeration Date","EnumerationDate","EXACT",100),

    ("Last Updated","LastUpdated","EXACT",100),

    ("Address Line 1","AddressLine1","FUZZY",90),

    ("City","City","FUZZY",90),

    ("State","State","EXACT",100),

    ("Postal Code","PostalCode","EXACT",100),

    ("Country Code","CountryCode","EXACT",100),

    ("Country Name","CountryName","EXACT",100),

    ("Phone Number","PhoneNumber","EXACT",100),

    ("Fax Number","FaxNumber","EXACT",100),

    ("Taxonomy Code","TaxonomyCode","EXACT",100),

    ("Taxonomy Description","TaxonomyDescription","FUZZY",90),

    ("License Number","LicenseNumber","EXACT",100),

    ("License State","LicenseState","EXACT",100),

    ("Identifier","Identifier","EXACT",100),

    ("Identifier Issuer","IdentifierIssuer","EXACT",100),

    ("Identifier Type","IdentifierType","EXACT",100)

]
# ============================================================
# Transform Standardized Provider Record into JISB Payload
#
# Purpose
# -------
# Convert a standardized Healthcare Provider record into
# the enterprise JISB payload format.
#
# Business Flow
#
# Standardized Provider Record
#             │
#             ▼
# Read Provider Fields
#             │
#             ▼
# Determine Country CodBase
#             │
#             ▼
# Apply JISB Field Mapping
#             │
#             ▼
# Build Enterprise JISB Payload
#             │
#             ▼
# Return Payload
#
# Parameters
# ----------
# provider
#
# Standardized Provider Dictionary.
#
# Returns
# -------
# Enterprise JISB Payload Dictionary
# ============================================================

def transform_to_jisb(provider: dict) -> dict:

    # --------------------------------------------------------
    # Prevent NoneType errors.
    #
    # If the incoming provider object is None,
    # replace it with an empty dictionary.
    # --------------------------------------------------------

    provider = provider or {}

    # --------------------------------------------------------
    # Read Provider Country.
    #
    # The Country Name is used to determine
    # the appropriate JISB CodBase.
    #
    # Example
    #
    # United States
    # India
    # Canada
    # --------------------------------------------------------

    country_name = (

        provider.get(

            "Country Name",

            ""

        )

        .strip()

        .lower()

    )

    # --------------------------------------------------------
    # Determine CodBase.
    #
    # If the country is not configured,
    # default to US.
    # --------------------------------------------------------

    cod_base = COUNTRY_TO_CODBASE.get(

        country_name,

        "US"

    )

    # --------------------------------------------------------
    # Create the base JISB Payload.
    #
    # Fields will be added dynamically
    # using the configured mapping.
    # --------------------------------------------------------

    payload = {

        "resultSize": "20",

        "entityType": "Activity",

        "codBases": [

            cod_base

        ],

        "fields": []

    }

    # --------------------------------------------------------
    # Build every JISB Field.
    #
    # The mapping configuration controls
    #
    # Source Field
    #
    # Target Field
    #
    # Match Method
    #
    # Fuzzy Precision
    #
    # This makes the transformation logic
    # generic and reusable.
    # --------------------------------------------------------

    for (

        source_field,

        target_field,

        match_method,

        fuzzy_precision

    ) in JISB_FIELD_MAPPING:

        # --------------------------------------------
        # Read Provider Value.
        #
        # Missing fields become
        # an empty string.
        # --------------------------------------------

        value = provider.get(

            source_field,

            ""

        )

        if value is None:

            value = ""

        # --------------------------------------------
        # Build one JISB Field.
        #
        # Every provider attribute becomes
        # one JISB field object.
        # --------------------------------------------

        jisb_field = {

            "method": match_method,

            "name": target_field,

            "values": [

                str(value)

            ],

            "fuzzyPrecision": fuzzy_precision

        }

        # --------------------------------------------
        # Append the field into the payload.
        # --------------------------------------------

        payload["fields"].append(

            jisb_field

        )

    # --------------------------------------------------------
    # Return the completed JISB Payload.
    # --------------------------------------------------------

    return payload