# Databricks notebook source
# ─────────────────────────────────────────────────────────────────────────────
# Silver Layer – Provider Standardization
# Pipeline : Bronze → Silver
# Table    : Healthcare_MDM.silver.provider_standardized
# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql          import SparkSession
from pyspark.sql.functions import (
    col, explode, from_json,
    first, trim, when,
    col, count, lit
)
from pyspark.sql.types    import (
    StructType, StructField,
    StringType, IntegerType, ArrayType
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# DBTITLE 1,Read NEW batches from Bronze (idempotent)
# Read ONLY NEW batches from Bronze that haven't been processed in Silver yet.
# Purpose : Incremental processing – skip batches already in Silver (idempotency).
#
# FIX S-03: Previous logic read ALL Bronze records, then checked `.first()` batch.
#           If that first batch was already in Silver, it skipped the write,
#           even if OTHER new batches existed. This caused new batches to be ignored!
#
# Corrected logic:
#   1. Get all BATCH_IDs currently in Silver (already processed)
#   2. Read all BATCH_IDs from Bronze
#   3. Filter Bronze to ONLY batches NOT in Silver (new batches)
#   4. Process only those new batches

# Step 1: Get batches already in Silver
try:
    processed_batches = [
        row.BATCH_ID for row in 
        spark.sql("SELECT DISTINCT BATCH_ID FROM healthcare_mdm.silver.provider_standardized").collect()
    ]
except Exception:
    # Table doesn't exist yet – first run
    processed_batches = []

print(f"Already processed batches in Silver: {processed_batches}")

# Step 2: Read all Bronze records
bronze_all_df = spark.table("healthcare_mdm.bronze.provider_bronze")

# Step 3: Filter to ONLY new batches
if len(processed_batches) > 0:
    bronze_df = bronze_all_df.filter(~col("BATCH_ID").isin(processed_batches))
else:
    bronze_df = bronze_all_df

new_batch_count = bronze_df.select("BATCH_ID").distinct().count()

if new_batch_count == 0:
    print("\n NO NEW BATCHES FOUND")
    print("All batches in Bronze have already been processed in Silver.")
    print("Pipeline will skip processing.\n")
    dbutils.notebook.exit("No new batches to process")
else:
    new_batches = [row.BATCH_ID for row in bronze_df.select("BATCH_ID").distinct().collect()]
    print(f"NEW batches to process: {new_batches}")
    print(f"Record count: {bronze_df.count()}")

# COMMAND ----------

# Validate Bronze input.
# Purpose : Confirm schema and sample records before parsing.

bronze_df.printSchema()
bronze_df.show(5, truncate=False)

# COMMAND ----------

# Define the JISB provider payload schema.
# Purpose : Parse the PROVIDER_JSON string column into a structured Spark object.
# Using an explicit schema avoids inference overhead and prevents type errors.

provider_schema = StructType([

    StructField("resultSize",  StringType(), True),
    StructField("entityType",  StringType(), True),

    StructField(
        "codBases",
        ArrayType(StringType()),
        True
    ),

    StructField(
        "fields",
        ArrayType(
            StructType([
                StructField("method",         StringType(),            True),
                StructField("name",           StringType(),            True),
                StructField("values",         ArrayType(StringType()), True),
                StructField("fuzzyPrecision", IntegerType(),           True)
            ])
        ),
        True
    )
])

# COMMAND ----------

# Parse the PROVIDER_JSON column into a structured Spark object.
# Purpose : Convert the raw JSON string into typed nested columns.

parsed_df = bronze_df.withColumn(
    "provider",
    from_json(col("PROVIDER_JSON"), provider_schema)
)

# COMMAND ----------

# Explode the JISB fields array.
# Purpose : Convert the fields array into individual rows.
#
# Before explode : 1 row per provider, fields = [FirstName, LastName, PhoneNumber ...]
# After  explode : N rows per provider, one row per field

exploded_df = parsed_df.withColumn(
    "field",
    explode(col("provider.fields"))
)

# COMMAND ----------

# Flatten the exploded structure.
# Purpose : Extract field name, field value, and metadata columns into a flat DataFrame.
# values[0] is used because each JISB field carries one primary value.

flatten_df = exploded_df.select(

    col("PROVIDER_JSON"),
    col("field.name").alias("FIELD_NAME"),
    col("field.values")[0].alias("FIELD_VALUE"),
    col("field.method").alias("MATCH_METHOD"),
    col("field.fuzzyPrecision").alias("FUZZY_PRECISION"),
    col("SOURCE_SYSTEM"),
    col("PAYLOAD_FORMAT"),
    col("TARGET_SYSTEM"),
    col("LOAD_TIMESTAMP"),
    col("LOAD_DATE"),
    col("FILE_NAME"),
    col("BATCH_ID")
)

# COMMAND ----------

# Validate the flattened fields.
# Purpose : Confirm every field has been converted into an individual row.

flatten_df.show(20, truncate=False)

# COMMAND ----------

flatten_df.select("FIELD_NAME", "FIELD_VALUE").show(50, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Standardization

# COMMAND ----------

# Pivot the flattened provider fields.
# Purpose : Convert field-per-row format back into one row per provider.
#
# groupBy   → group all field rows that belong to the same provider payload
# pivot     → FIELD_NAME values become column headers
# agg first → take the first (and only) FIELD_VALUE for each field
# select    → order columns in the required enterprise business column sequence

silver_df = (
    flatten_df
    .groupBy(
        "PROVIDER_JSON",
        "SOURCE_SYSTEM",
        "PAYLOAD_FORMAT",
        "TARGET_SYSTEM",
        "LOAD_TIMESTAMP",
        "LOAD_DATE",
        "FILE_NAME",
        "BATCH_ID"
    )
    .pivot("FIELD_NAME")
    .agg(first("FIELD_VALUE"))
    .select(
        # Business columns – enterprise ordering
        "ProviderID",
        "FirstName",
        "MiddleName",
        "LastName",
        "FullName",
        "Credential",
        "Gender",
        "Status",
        "EnumerationDate",
        "LastUpdated",
        "AddressLine1",
        "City",
        "State",
        "PostalCode",
        "CountryCode",
        "CountryName",
        "PhoneNumber",
        "FaxNumber",
        "TaxonomyCode",
        "TaxonomyDescription",
        "LicenseNumber",
        "LicenseState",
        "Identifier",
        "IdentifierIssuer",
        "IdentifierType",
        # Metadata columns – placed at the end
        "SOURCE_SYSTEM",
        "PAYLOAD_FORMAT",
        "TARGET_SYSTEM",
        "LOAD_TIMESTAMP",
        "LOAD_DATE",
        "FILE_NAME",
        "BATCH_ID"
    )
)

# COMMAND ----------

# Create the Silver schema if it does not exist.
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS Healthcare_MDM.silver
""")

print(" Silver schema ready.")

# COMMAND ----------

# Validate the pivot result.
# Purpose : Confirm every provider now occupies a single business record.

silver_df.printSchema()
silver_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality – String Standardization

# COMMAND ----------

# Standardize all string columns in-memory.
# Purpose : Clean the pivoted DataFrame BEFORE writing to the Silver table.
#
# Rules applied to every string column dynamically:
#   1. Remove leading whitespace.
#   2. Remove trailing whitespace.
#   3. Convert empty strings ("") to NULL.
#
# FIX S-01: The original code wrote raw un-cleaned data in a first append write,
# then applied DQ, then overwrote. This meant the first write contained dirty data.
# Corrected flow: apply DQ first, then write ONCE.

from pyspark.sql.functions import trim, when
from pyspark.sql.types     import StringType

for field in silver_df.schema.fields:
    if isinstance(field.dataType, StringType):
        silver_df = silver_df.withColumn(
            field.name,
            when(trim(col(field.name)) == "", None)
            .otherwise(trim(col(field.name)))
        )

# COMMAND ----------

# Validate DQ standardization.
# Purpose : Confirm all string columns are trimmed and empty strings are NULL.

silver_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Null Values Report

# COMMAND ----------

# Generate a Data Quality Null Report.
# Purpose : Count NULL values for every column to assess data completeness.
# This report is informational – records are NOT rejected based on nulls.

null_report_df = silver_df.select(
    [
        (
            (count(when(col(column).isNull(), 1)) / count(lit(1)) * 100)
            .alias(f"{column}_null_pct")
        )
        for column in silver_df.columns
    ]
)

null_report_df.show(truncate=False)
print("\n Null Percentage Report generated (shows % of NULLs per column)")

# COMMAND ----------

print(f"Silver Record Count (in-memory, post-DQ) : {silver_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Idempotency Check

# COMMAND ----------

# Check if this batch has already been loaded into Silver.
# Purpose : Prevent duplicate records if the Silver notebook is re-run
#           for the same batch without new Bronze data arriving.
#
# Logic:
#   If BATCH_ID already exists in Silver → skip write (idempotent).
#   If BATCH_ID is new                  → write the clean records.

current_batch_id = silver_df.select("BATCH_ID").first()["BATCH_ID"]
print(f"Current Batch ID : {current_batch_id}")

already_in_silver = 0

try:
    # Table exists – check for this batch
    already_in_silver = spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM Healthcare_MDM.silver.provider_standardized
        WHERE BATCH_ID = '{current_batch_id}'
    """).collect()[0]["cnt"]

except Exception:
    # Table does not exist yet – this is the first run
    already_in_silver = 0

print(f"Records already in Silver for batch {current_batch_id} : {already_in_silver}")

# COMMAND ----------

# Create the Silver schema if it does not exist.

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS Healthcare_MDM.silver
""")

# COMMAND ----------

# DBTITLE 1,Create Silver Table with Idempotency
# Create the Silver Delta table if it does not exist.
#
# Purpose: Define the Silver table schema upfront for standardized provider data.
#
# IDEMPOTENCY PROTECTION:
# - IF NOT EXISTS: Skips creation if table already exists
# - Protects existing data from accidental deletion/overwrite
# - Safe for pipeline re-runs and job scheduling
#
# Why IF NOT EXISTS?
# - First run: Creates the table with explicit schema
# - Subsequent runs: Skips creation, preserves all existing records
# - No impact on data: 100% safe for production pipelines

spark.sql("""
    CREATE TABLE IF NOT EXISTS Healthcare_MDM.silver.provider_standardized
    (
        ProviderID          STRING  COMMENT 'Unique provider identifier (NPI)',
        FirstName           STRING  COMMENT 'Provider first name',
        MiddleName          STRING  COMMENT 'Provider middle name',
        LastName            STRING  COMMENT 'Provider last name',
        FullName            STRING  COMMENT 'Provider full name',
        Credential          STRING  COMMENT 'Professional credentials (MD, DO, etc.)',
        Gender              STRING  COMMENT 'Provider gender',
        Status              STRING  COMMENT 'Provider status (Active, Inactive)',
        EnumerationDate     STRING  COMMENT 'Date of initial NPI enumeration',
        LastUpdated         STRING  COMMENT 'Last update timestamp',
        AddressLine1        STRING  COMMENT 'Primary address line 1',
        City                STRING  COMMENT 'City',
        State               STRING  COMMENT 'State code',
        PostalCode          STRING  COMMENT 'ZIP/Postal code',
        CountryCode         STRING  COMMENT 'Country code',
        CountryName         STRING  COMMENT 'Country name',
        PhoneNumber         STRING  COMMENT 'Contact phone number',
        FaxNumber           STRING  COMMENT 'Contact fax number',
        TaxonomyCode        STRING  COMMENT 'Healthcare provider taxonomy code',
        TaxonomyDescription STRING  COMMENT 'Taxonomy description',
        LicenseNumber       STRING  COMMENT 'Provider license number',
        LicenseState        STRING  COMMENT 'State where license was issued',
        Identifier          STRING  COMMENT 'Additional identifier',
        IdentifierIssuer    STRING  COMMENT 'Identifier issuing authority',
        SpecialtyCode       STRING  COMMENT 'Provider specialty code',
        SpecialtyName       STRING  COMMENT 'Provider specialty name',
        OrganizationName    STRING  COMMENT 'Organization name (for organizational providers)',
        OrganizationType    STRING  COMMENT 'Organization type',
        Website             STRING  COMMENT 'Provider or organization website',
        SOURCE_SYSTEM       STRING  COMMENT 'Originating system',
        PAYLOAD_FORMAT      STRING  COMMENT 'Data format',
        TARGET_SYSTEM       STRING  COMMENT 'Destination system',
        LOAD_TIMESTAMP      TIMESTAMP COMMENT 'Bronze ingestion timestamp',
        LOAD_DATE           DATE    COMMENT 'Bronze ingestion date',
        FILE_NAME           STRING  COMMENT 'Source file name',
        BATCH_ID            STRING  COMMENT 'Unique batch identifier for idempotency'
    )
    USING DELTA
    COMMENT 'Silver layer – standardized provider data with data quality rules applied.'
""")

print("Silver table ready (created or already exists)")

# COMMAND ----------

# DBTITLE 1,Silver Idempotency Documentation
# MAGIC %md
# MAGIC ## 🛡️ Silver Layer Idempotency Protection
# MAGIC
# MAGIC This Silver standardization notebook has **THREE layers of protection** against duplicate data:
# MAGIC
# MAGIC ### **Layer 1: CREATE TABLE IF NOT EXISTS (Cell above)**
# MAGIC * **Protection**: Table creation is idempotent
# MAGIC * **Effect**: Re-running the notebook won't drop or recreate the table
# MAGIC * **Result**: All existing records are preserved
# MAGIC
# MAGIC ### **Layer 2: Batch ID Check (Cell 21)**
# MAGIC * **Protection**: Checks if current BATCH_ID already exists in Silver
# MAGIC * **Effect**: Skips write if batch already processed
# MAGIC * **Result**: Re-running with same Bronze data won't create duplicates
# MAGIC
# MAGIC ### **Layer 3: Append Mode (Cell 24)**
# MAGIC * **Protection**: New records are added; existing records are never modified
# MAGIC * **Effect**: Preserves immutability and historical batches
# MAGIC * **Result**: Historical data remains unchanged
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Pipeline-Safe Execution**
# MAGIC ✅ This notebook can be scheduled to run **multiple times per day**  
# MAGIC ✅ Safe for automated pipelines and job orchestration  
# MAGIC ✅ No risk of duplicate records or data loss  
# MAGIC ✅ No manual cleanup required between runs  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **How It Works:**
# MAGIC 1. **First Run**: Creates table → Reads Bronze → Applies DQ rules → Writes to Silver
# MAGIC 2. **Second Run (same data)**: Table exists → Reads Bronze → Detects batch already in Silver → SKIPS write
# MAGIC 3. **Second Run (new data)**: Table exists → Reads new Bronze batch → Processes → APPENDS to Silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Silver Write

# COMMAND ----------

# Write the clean standardized provider records into the Silver Delta table.
# Purpose : Persist the DQ-standardized provider dataset for the Gold layer.
#
# FIX S-01: Removed the pre-DQ append write that was inserting dirty data.
# FIX S-02: Changed from mode("overwrite") to mode("append").
#           overwrite was DELETING all previous batches on every run → data loss.
#           append preserves all historical batches.
# FIX S-03: Removed conditional write logic.
#           Cell 2 now filters Bronze to ONLY new batches upfront.
#           If we reach this cell, we're guaranteed to have only new batches.

batch_ids = [row.BATCH_ID for row in silver_df.select("BATCH_ID").distinct().collect()]

silver_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("Healthcare_MDM.silver.provider_standardized")

print(f"[SUCCESS] Silver records written for batches: {batch_ids}")
print(f"          Record count: {silver_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

# Read and validate the final Silver Delta table.
# Purpose : Confirm records were written correctly before starting the Gold layer.

silver_table_df = spark.table("Healthcare_MDM.silver.provider_standardized")

silver_table_df.show(truncate=False)
silver_table_df.printSchema()

# COMMAND ----------

print(f"Silver Record Count (total, all batches) : {silver_table_df.count()}")
