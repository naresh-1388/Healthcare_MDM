# Databricks notebook source
# Import the SparkSession class to create and manage Spark DataFrames.

from pyspark.sql import SparkSession

# Import Spark SQL functions required for parsing, flattening and transforming the Bronze provider payload.

from pyspark.sql.functions import col
from pyspark.sql.functions import explode
from pyspark.sql.functions import from_json

# Import Spark SQL data types required to define the JSON schema.

from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import ArrayType


# Create or retrieve the active Spark Session.

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Read the Bronze Delta table.
# Purpose : Load the raw provider payload generated during the Bronze ingestion process.

bronze_df = spark.table("Healthcare_MDM.bronze.provider_raw")

# COMMAND ----------

# Display the Bronze schema.
# Purpose: Verify that the provider payload and metadata columns are available before parsing.

bronze_df.printSchema()

bronze_df.show(5,truncate=False)

# COMMAND ----------

# Define the schema of the JSIB provider payload.
#
# Purpose : Convert the PROVIDER_JSON column into a structured  Spark object that can be queried and transformed.

provider_schema = StructType([

    StructField("resultSize", StringType(), True),
    StructField("entityType", StringType(), True),

    StructField(
        "codBases",
        ArrayType(
            StringType()
        ),
        True
    ),

    StructField(
        "fields",
        ArrayType(
            StructType([
                StructField("method", StringType(), True),
                StructField("name", StringType(), True),
                StructField(
                    "values",
                    ArrayType(
                        StringType()
                    ),
                    True
                ),
                StructField(
                    "fuzzyPrecision",
                    IntegerType(),
                    True
                )
            ])
        ),
        True
    )
])

# COMMAND ----------

# Parse the PROVIDER_JSON column into a structured Spark object using the predefined schema.

parsed_df = bronze_df.withColumn(
    "provider",
    from_json(
        col("PROVIDER_JSON"),
        provider_schema
    )
)

# COMMAND ----------

# Explode the JSIB field collection.
#
# Purpose : Convert every element inside the fields array into a separate row. One provider record will temporarily become multiple rows.
#
# Example: FirstName, LastName, PhoneNumber, City...
#
# Each field becomes an independent record.

exploded_df = parsed_df.withColumn(
    "field",
    explode(
        col("provider.fields")
    )

)

# COMMAND ----------

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

# Display the flattened provider fields.

# Purpose : Verify that every JSIB field has been converted into an individual business record.

flatten_df.show(20,truncate=False)

# COMMAND ----------

flatten_df.select("FIELD_NAME", "FIELD_VALUE").show(50, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Standardization

# COMMAND ----------

# Pivot the flattened provider fields.

# Purpose : Convert multiple provider attribute rows into a single standardized provider record.

# Arrange the business columns in the required enterprise order.

# Metadata columns are placed at the end.

from pyspark.sql.functions import first

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

silver_df = silver_df.drop("PROVIDER_JSON")

# COMMAND ----------

# Display the standardized Silver DataFrame.

# Purpose :Verify that every provider now occupies a single business record.

silver_df.printSchema()
silver_df.show(truncate=False)

# COMMAND ----------

# Create the Silver schema if it does not exist.

# Purpose : Ensure the Silver schema is available before creating the Delta table.

spark.sql("""
CREATE SCHEMA IF NOT EXISTS Healthcare_MDM.silver
""")

# COMMAND ----------

# Write the standardized provider records into the Silver Delta table.
#
# Purpose : Persist the standardized provider dataset for downstream processing.

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable(
        "Healthcare_MDM.silver.provider_standardized"
    )

# COMMAND ----------

# Read the Silver Delta table.

# Purpose :Validate that the provider records have been successfully written.

silver_table_df = spark.table("Healthcare_MDM.silver.provider_standardized")

# COMMAND ----------

# Display all standardized provider records.

# Purpose : Validate the final Silver output.

silver_table_df.show(truncate=False)

# COMMAND ----------

# Display the total number of standardized provider records.

print(f"Silver Record Count : {silver_table_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality
# MAGIC
# MAGIC

# COMMAND ----------

# Standardize all string columns.

# 1. Remove leading spaces.
# 2. Remove trailing spaces.
# 3. Convert empty strings to NULL.
#
# This implementation is dynamic and automatically applies to every string column in the DataFrame.

from pyspark.sql.functions import col
from pyspark.sql.functions import trim
from pyspark.sql.functions import when
from pyspark.sql.types import StringType

for field in silver_df.schema.fields:
    if isinstance(field.dataType, StringType):
        silver_df = silver_df.withColumn(field.name,when(trim(col(field.name)) == "",None)
            .otherwise(trim(col(field.name))
            )
        )

# COMMAND ----------

# Display the standardized provider records.

# Verify that all string columns have been cleaned successfully.

silver_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Null Values count

# COMMAND ----------

# Generate a Data Quality Report.

# Purpose : Count NULL values for every column.

# This report helps business users understand data completeness without rejecting records.

from pyspark.sql.functions import col
from pyspark.sql.functions import count
from pyspark.sql.functions import when

null_report_df = silver_df.select(
    [
        count(
            when(
                col(column).isNull(),
                column
            )
        ).alias(column)
        for column in silver_df.columns
    ]
)
null_report_df.show(
    truncate=False
)

# COMMAND ----------

# Display the total number of standardized
# provider records available in the Silver layer.

print(f"Silver Record Count : {silver_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final Standardized Provider

# COMMAND ----------

# Write the standardized provider records into the Silver Delta table.

# Purpose :Store the final standardized provider dataset.

# This table becomes the input for the Gold layer.

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "Healthcare_MDM.silver.provider_standardized"
    )

# COMMAND ----------

# Read the final Silver Delta table.

# Purpose: Validate that the standardized provider records were successfully written.

silver_table_df = spark.table("Healthcare_MDM.silver.provider_standardized")

# COMMAND ----------

# Display the final Silver table.

# Purpose : Verify the final standardized provider dataset before starting the Gold layer.

silver_table_df.show(truncate=False)

silver_table_df.printSchema()

# COMMAND ----------

# Display the total number of provider records available in the Silver layer.

print(f"Silver Record Count : {silver_table_df.count()}")

# COMMAND ----------

silver_df.count()