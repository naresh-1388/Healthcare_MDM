"""
==============================================================
Project Name : Healthcare_MDM

Module       : AWS S3 Upload Utility

Author       : Naresh

Description
-----------
This module uploads local files into an AWS S3 bucket.

Business Flow

Local File
      │
      ▼
AWS S3 Bucket
      │
      ├── raw/
      ├── processed/
      ├── gold/
      └── archive/

Responsibilities

1. Read AWS configuration from the .env file.
2. Establish a secure connection with AWS S3.
3. Upload any local file into the required S3 location.
4. Support dynamic S3 object names.
5. Return upload status.

==============================================================
"""

# ============================================================
# Standard Python Libraries
# ============================================================

import os                                                    # File and path operations

# ============================================================
# Third Party Libraries
# ============================================================

import boto3                                                 # AWS SDK

from dotenv import load_dotenv                              # Load .env variables


# ============================================================
# Load Environment Variables
# ============================================================

load_dotenv()


# ============================================================
# AWS Configuration
#
# These values are loaded from the .env file.
# ============================================================

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")

AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

AWS_REGION = os.getenv("AWS_REGION")

AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")


# ============================================================
# Create AWS S3 Client
#
# This client is reused throughout the application.
# ============================================================

s3_client = boto3.client(

    "s3",

    aws_access_key_id=AWS_ACCESS_KEY_ID,

    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,

    region_name=AWS_REGION

)


# ============================================================
# Upload File to AWS S3
#
# Purpose
# -------
# Upload any local file into AWS S3.
#
# Parameters
# ----------
# local_file_path
# Local file available on the machine.
#
# Example: data/raw/raw_20260627_101500.json
#
# s3_object_name
#
# Complete destination object path inside S3.
#
# Example:
#
# raw/raw_20260627_101500.json
#
# processed/processed_20260627_101500.json
#
# gold/provider_master.parquet
#
# ============================================================

def upload_to_s3(

    local_file_path: str,

    s3_object_name: str

):

    try:

        s3_client.upload_file(

            Filename=local_file_path,

            Bucket=AWS_BUCKET_NAME,

            Key=s3_object_name

        )

        print("\n---------------------------------------------")

        print("AWS S3 Upload Successful")

        print("---------------------------------------------")

        print(f"Bucket      : {AWS_BUCKET_NAME}")

        print(f"Local File  : {local_file_path}")

        print(f"S3 Object   : {s3_object_name}")

        print("---------------------------------------------\n")

    except Exception as exception:

        print("\n---------------------------------------------")

        print("AWS S3 Upload Failed")

        print("---------------------------------------------")

        print(exception)

        print("---------------------------------------------\n")

        raise