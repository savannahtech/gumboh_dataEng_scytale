import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql.functions import from_unixtime, col

spark = SparkSession.builder.appName('GithubETL').getOrCreate()

def transform_data(github_token):

    # Define the schema for the JSON data
    json_schema = StructType([
        StructField("created_at", StringType(), True),
        StructField("raw", StructType([
            StructField("full_name", StringType(), True),
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("owner", StructType([
                StructField("login", StringType(), True)
            ]), True)
        ]), True),
        StructField("language", StringType(), True),
        StructField("num_prs", IntegerType(), True),
        StructField("num_prs_merged", IntegerType(), True),
        StructField("merged_at", StringType(), True),
        # Add more fields if needed
    ])

    # Read all extract JSON files with the specified schema
    extract_df = spark.read.schema(json_schema).json('data/raw_data')

    # Transformation
    transformed_df = extract_df.withColumn("created_date", F.unix_timestamp(col("created_at"), "yyyy-MM-dd"))
    transformed_df = transformed_df.withColumn("repo_name", F.split(F.col("raw.full_name"), "/").getItem(0))
    transformed_df = transformed_df.filter(
        (col("num_prs") == col("num_prs_merged")) & (F.col("raw.owner.login").contains("scytale"))
    )

    # Select required columns
    result_df = transformed_df.select(
        col("repo_name").alias("Organization Name"),
        col("raw.id").alias("repository_id"),
        col("raw.name").alias("repository_name"),
        col("raw.owner.login").alias("repository_owner"),
        col("num_prs"),
        col("num_prs_merged"),
        from_unixtime(col("merged_at"), 'yyyy-MM-dd').alias("merged_at"),
        ((col("num_prs") == col("num_prs_merged")) & F.col("raw.owner.login").contains("scytale")).alias("is_compliant")
    )

    # Write to parquet file
    result_df.write.mode("overwrite").parquet("data/results")

if __name__ == '__main__':
    transform_data()

