"""
Transform the standardized provider record into the JSIB payload format.
"""

# COUNTRY TO CODBASE MAPPING

COUNTRY_TO_CODBASE = {

    "india": "IN",
    "united states": "US",
    "usa": "US",
    "canada": "CA",
    "uk": "GB"
}

# FIELD MAPPING
# source_field, target_field, match_method, fuzzy_precision

JSIB_FIELD_MAPPING = [

    ("First Name", "FirstName", "EXACT", 100),

    ("Last Name", "LastName", "EXACT", 100),

    ("City", "City", "FUZZY", 80),

    ("Phone", "PhoneNumber", "EXACT", 100)

]

def transform_to_jsib(provider: dict) -> dict:
    """
    Convert a standardized provider record into the
    JSIB request payload.

    This function only creates the payload.

    It does not call any external API.
    """

    provider = provider or {}                                            # Prevent None values

    addresses = provider.get("HCP Address", [])                          # Read provider addresses

    first_address = addresses[0] if addresses else {}                    # Read first available address

    country = (
        first_address
        .get("Country", {})
        .get("Name", "")
    ).strip().lower()                                                    # Read country name

    cod_base = COUNTRY_TO_CODBASE.get(
        country,
        "US"
    )                                                                     # Default to US if country is missing

    payload = {

        "resultSize": "20",

        "entityType": "Activity",

        "codBases": [

            cod_base
        ],
        "fields": []
    }
    for source_field, target_field, method, precision in JSIB_FIELD_MAPPING:

        if source_field == "City":

            value = first_address.get(
                "City",
                ""
            )
        elif source_field == "Phone":

            value = first_address.get(
                "Phone",
                ""
            )
        else:
            value = provider.get(
                source_field,
                ""
            )
        payload["fields"].append(

            {

                "method": method,

                "name": target_field,

                "values": [

                    value
                ],

                "fuzzyPrecision": precision

            }

        )

    return payload