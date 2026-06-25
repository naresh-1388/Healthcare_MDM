# Databricks notebook source
from pyspark.sql import SparkSession                                # Import SparkSession to work with Spark DataFrames
from pyspark.sql.functions import current_timestamp                 # Import function to capture the current ingestion timestamp
from pyspark.sql.functions import current_date                      # Import function to capture the current load date
from pyspark.sql.functions import lit                              # Import function to create constant column values
from pyspark.sql.functions import to_json                          # Import function to convert complex columns into JSON strings
from pyspark.sql.functions import struct                           # Import function to combine multiple columns into a single structure


spark = SparkSession.builder.getOrCreate()                         # Create or retrieve the active Spark session

# COMMAND ----------

# Read the processed JSON file.

# Input File : processed_response.json
# Purpose : Load the JSIB payload into a Spark DataFrame.


json_df = spark.read.option(
    "multiline",
    "true"
).json(
    "/Workspace/Users/naresh.mayari@gmail.com/Healthcare_MDM/processed_response.json"
)

# COMMAND ----------

# Created the Bronze DataFrame. and Added metadata columns.

# PROVIDER_JSON
# SOURCE_SYSTEM
# LOAD_TIMESTAMP
# LOAD_DATE
# FILE_NAME

# Purpose : Prepare the dataset for Bronze storage.


from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import current_date
from pyspark.sql.functions import lit
from pyspark.sql.functions import to_json
from pyspark.sql.functions import struct

bronze_df = (
    json_df
    .withColumn(
        "PROVIDER_JSON",
        to_json(struct(*json_df.columns))
    )
    .withColumn(
        "SOURCE_SYSTEM",
        lit("NPPES")
    )
    .withColumn(
        "LOAD_TIMESTAMP",
        current_timestamp()
    )
    .withColumn(
        "LOAD_DATE",
        current_date()
    )
    .withColumn(
        "FILE_NAME",
        lit("processed_response.json")
    )
    .select(
        "PROVIDER_JSON",
        "SOURCE_SYSTEM",
        "LOAD_TIMESTAMP",
        "LOAD_DATE",
        "FILE_NAME"
    )
)

# COMMAND ----------

# Validated the DataFrame.

# Purpose : Verify that the ingestion process produced the expected structure.

bronze_df.printSchema()

bronze_df.show(truncate=False)

# COMMAND ----------

# Created the Bronze schema if it did not already exist.
# Purpose : Ensure the target schema exists before writing data.

spark.sql("""

CREATE SCHEMA IF NOT EXISTS Healthcare_MDM.bronze

""")

# COMMAND ----------

# Wrote the DataFrame into the Delta table.

# Table : Healthcare_MDM.bronze.provider_raw

# Write Mode
# overwrite

# Purpose : Persist provider payloads inside the Bronze layer.

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "Healthcare_MDM.bronze.provider_raw"
    )

# COMMAND ----------

# Read the Bronze Table 
# verifying successful dat Loading

bronze_table_df = spark.table(
    "Healthcare_MDM.bronze.provider_raw"
)

bronze_table_df.show(
    truncate=False
)

# COMMAND ----------

# Validated the record count
# Confirms that all processed records were loaded into the Bronze layer

print(
    f"Bronze Record Count : {bronze_table_df.count()}"
)

# COMMAND ----------

# Ensurees all metadata columns were created as expected 

bronze_table_df.printSchema()