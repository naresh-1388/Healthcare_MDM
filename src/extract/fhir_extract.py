import os
import requests
from dotenv import load_dotenv
import json

load_dotenv()

FHIR_BASE_URL = os.getenv("FHIR_BASE_URL")

url = f"{FHIR_BASE_URL}/Practitioner"

response = requests.get(url)

print("Status Code :", response.status_code)

if response.status_code == 200:
    data = response.json()

    entries = data.get("entry", [])

    print(f"Records Retrieved : {len(entries)}")

    print(json.dumps(data, indent=2)[:5000])

    for record in entries[:5]:
        resource = record.get("resource", {})
        print(resource.get("id"))
else:
    print(response.text)