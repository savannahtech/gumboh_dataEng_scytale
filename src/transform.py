import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DateType
from pyspark.sql.functions import substring, from_unixtime, col, when, split
import os


# Initializing the SparkSession
if SparkSession.getActiveSession():
    spark = SparkSession.getActiveSession()
else:
    spark = SparkSession.builder.appName("readJSONdata").getOrCreate()


def transform_data():
    # Define the schema
    json_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("owner", StructType([
            StructField("login", StringType(), True)
        ]), True),
        StructField("num_prs", IntegerType(), True),
        StructField("num_prs_merged", IntegerType(), True),
        StructField("merged_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ])

    # Defining the path to our merged JSON file
    json_file_path = "data/raw_data/repo_zote.json"

    # Using the defined json_schema to read the JSON file into a DataFrame
    df = spark.read.json(json_file_path, schema=json_schema)

    # Calculate the number of PRs for each repository
    prs_count_df = df.groupBy("full_name").count().withColumnRenamed("count", "prs_count")

    # Calculate the number of merged PRs for each repository
    merged_prs_count_df = df.filter(col("merged_at").isNotNull()).groupBy("full_name").count().withColumnRenamed("count", "merged_prs_count")

    df = df.join(prs_count_df, "full_name", "left").join(merged_prs_count_df, "full_name", "left")
    df = df.fillna({"num_prs_merged": 0})

    print("Total Count of Extracted JSON Data:", df.count())

    #df.show()

    # Perform schema transformation
    select_df = df.select(
        split(col("full_name"), "/").getItem(0).alias("Organization Name"),
        col("id").alias("repository_id"),
        col("name").alias("repository_name"),
        col("owner.login").alias("repository_owner"),
        col("prs_count").alias("num_prs"),
        col("num_prs_merged"),
        when(col("num_prs_merged") == 0, "N/A").otherwise(col("updated_at")).alias("merged_at"),
        when((col("num_prs") == col("merged_prs_count")) & (col("owner.login").contains("scytale")), True).otherwise(False).alias("is_compliant")
    )

    # Print the number of rows in the transformed DataFrame
    print("Total Count of Transformed Rows from JSON Data:", select_df.count())

    # Write to parquet file
    select_df.write.mode("overwrite").parquet("data/results")

    # Show the resulting DataFrame
    #select_df.show()

    # close sparksession
    spark.stop()

if __name__ == '__main__':
    transform_data()


