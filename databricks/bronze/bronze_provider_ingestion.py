# Databricks notebook source
# Import the SparkSession class to create and manage Spark DataFrames

from pyspark.sql import SparkSession

# Import Spark SQL functions required to build the Bronze layer

from pyspark.sql import SparkSession
from pyspark.sql.types  import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DateType
)
# Create or retrieve the active Spark Session

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# DBTITLE 1,Create Bronze Table with Idempotency
# Create the Bronze Delta table if it does not exist.
#
# Purpose: Store the raw provider payload exactly as received from Python.
# The table preserves the full JISB JSON string plus 7 audit metadata columns.
#
# IDEMPOTENCY PROTECTION:
# - IF NOT EXISTS: Skips creation if table already exists
# - Protects existing data from accidental deletion/overwrite
# - Safe for pipeline re-runs and job scheduling
#
# Why IF NOT EXISTS?
# - First run: Creates the table
# - Subsequent runs: Skips creation, preserves all existing records
# - No impact on data: 100% safe for production pipelines

spark.sql("""
    CREATE TABLE IF NOT EXISTS healthcare_mdm.bronze.provider_bronze
    (
        PROVIDER_JSON   STRING     COMMENT 'Raw JISB JSON payload (unchanged from source)',
        SOURCE_SYSTEM   STRING     COMMENT 'Originating system (e.g., Epic, Cerner)',
        PAYLOAD_FORMAT  STRING     COMMENT 'Data format (JISB, HL7, FHIR)',
        TARGET_SYSTEM   STRING     COMMENT 'Destination system identifier',
        LOAD_TIMESTAMP  TIMESTAMP  COMMENT 'Exact time record was ingested into Bronze',
        LOAD_DATE       DATE       COMMENT 'Date of ingestion (for partition/filtering)',
        FILE_NAME       STRING     COMMENT 'Source file name for lineage tracking',
        BATCH_ID        STRING     COMMENT 'Unique batch identifier for idempotency'
    )
    USING DELTA
    COMMENT 'Bronze layer – raw JISB provider payload with ingestion metadata. Append-only, immutable source of truth.'
""")

print("Bronze table ready (created or already exists)")

# COMMAND ----------

# Create the Bronze schema if it does not exist.

# Purpose : Ensure the Bronze schema is available before creating the Delta table.

spark.sql("""
CREATE SCHEMA IF NOT EXISTS Healthcare_MDM.bronze
""")

# COMMAND ----------

# Define the schema for the Bronze layer.

# Purpose : Read processed JSON files coming from the S3 landing area. Using an explicit schema improves performance and avoids schema inference.

bronze_schema = StructType([

    StructField("PROVIDER_JSON", StringType(), True),
    StructField("SOURCE_SYSTEM", StringType(), True),
    StructField("PAYLOAD_FORMAT", StringType(), True),
    StructField("TARGET_SYSTEM", StringType(), True),
    StructField("LOAD_TIMESTAMP", TimestampType(), True),
    StructField("LOAD_DATE", DateType(), True),
    StructField("FILE_NAME", StringType(), True),
    StructField("BATCH_ID", StringType(), True)

])

# COMMAND ----------

# Read processed provider files from the S3 landing area using Auto Loader.
# Purpose : Continuously detect and ingest new JSON files as they arrive.
#
# cloudFiles.format = json   → Auto Loader reads JSON files from S3.
# schema             = explicit → Avoids schema inference overhead.
# load path          = processed folder → Files moved here after Python processing.

bronze_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(bronze_schema)
        .load(
            "/Volumes/healthcare_mdm/landing/healthcare_files/processed/"
        )
)

# COMMAND ----------

# Write the streaming provider records into the Bronze Delta table.
#
# Purpose : Persist every incoming provider batch into the Bronze layer.
#
# outputMode  = append          → New records are added; existing records are untouched.
# checkpoint  = dedicated path  → Auto Loader tracks which files were already processed.
#                                 Re-running the notebook will NOT re-process the same file.
# trigger     = availableNow    → Process all currently available files, then stop.
#                                 Behaves like a batch job – no continuous streaming.

query = (
    bronze_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            "/Volumes/healthcare_mdm/landing/healthcare_files/checkpoint/bronze"
        )
        .trigger(availableNow=True)
        .toTable("healthcare_mdm.bronze.provider_bronze")
)

query.awaitTermination()
print("Bronze stream completed.")

# COMMAND ----------

# Validate: Read the Bronze Delta table.
# Purpose : Confirm that Auto Loader successfully wrote provider records.

bronze_table_df = spark.table("healthcare_mdm.bronze.provider_bronze")

# COMMAND ----------

# Validate: Display all Bronze records.
# Purpose : Confirm provider payload and metadata columns are intact.

bronze_table_df.show(truncate=False)


# COMMAND ----------

# Validate: Display the total Bronze record count.
# Purpose : Confirm every processed file has been loaded.

print(f"Bronze Record Count : {bronze_table_df.count()}")


# COMMAND ----------

# Validate: Display one complete provider payload.
# Purpose : Confirm the JSON document is preserved without modification.

bronze_table_df.select("PROVIDER_JSON").show(1, truncate=False)


# COMMAND ----------

# Validate: Display ingestion metadata.
# Purpose : Confirm auditing, lineage, payload format and target system columns.

bronze_table_df.select(
    "SOURCE_SYSTEM",
    "PAYLOAD_FORMAT",
    "TARGET_SYSTEM",
    "LOAD_TIMESTAMP",
    "LOAD_DATE",
    "FILE_NAME",
    "BATCH_ID"
).show(50, truncate=False)


# COMMAND ----------

display(bronze_table_df)

# COMMAND ----------

# DBTITLE 1,Idempotency Verification Guide
# MAGIC %md
# MAGIC ## ✅ Idempotency Verification
# MAGIC
# MAGIC **To verify Bronze layer idempotency protection is working:**
# MAGIC
# MAGIC 1. **Run this notebook twice** with the same source files in `/processed/`
# MAGIC 2. **Check the record count** before and after the second run
# MAGIC 3. **Expected Result**: Record count should be **identical**
# MAGIC
# MAGIC **Why?**
# MAGIC * Auto Loader checkpoint remembers which files were processed
# MAGIC * Same files are NOT re-ingested on subsequent runs
# MAGIC * Only NEW files arriving in `/processed/` will be loaded
# MAGIC
# MAGIC **Test Command:**
# MAGIC ```python
# MAGIC print(f"Bronze Record Count: {spark.table('healthcare_mdm.bronze.provider_bronze').count()}")
# MAGIC ```
# MAGIC
# MAGIC **Run this notebook 5 times → Count stays the same** (unless new files arrive) ✅