import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql.functions import from_unixtime, col

spark = SparkSession.builder.appName('GithubETL').getOrCreate()

def transform_data():

    # Define the schema for the final table
    json_schema = StructType([
        StructField("Organization Name", StringType(), True),
        StructField("repository_id", StringType(), True),
        StructField("repository_name", StringType(), True),
        StructField("repository_owner", StringType(), True),
        StructField("num_prs", IntegerType(), True),
        StructField("num_prs_merged", IntegerType(), True),
        StructField("merged_at", StringType(), True),
        StructField("is_compliant", BooleanType(), True),
    ])

    # Read all extract JSON files
    extract_df = spark.read.schema(json_schema).json('../data/raw_data')

    # Transformation
    transformed_df = extract_df.withColumn("merged_at", F.unix_timestamp(col("merged_at"), "yyyy-MM-dd"))
    transformed_df = transformed_df.withColumn("repo_name", F.split(F.col("full_name"), "/").getItem(0))
    transformed_df = transformed_df.filter((col("num_prs") == col("num_prs_merged")) & (F.col("repository_owner").contains("scytale")))

    # Select required columns
    result_df = transformed_df.select(
        col("repo_name").alias("Organization Name"),
        col("raw.id").alias("repository_id"),
        col("raw.name").alias("repository_name"),
        col("raw.owner.login").alias("repository_owner"),
        col("The number of PRs for each repository").alias("num_prs"),
        col("The number of Merged PRs for each repository").alias("num_prs_merged"),
        from_unixtime(col("The last date that a PR was merge in"), 'yyyy-MM-dd').alias("merged_at"),
        col("(num_prs == num_prs_merged) AND (repository_owner contains 'scytale')").alias("is_compliant")
    )

    # Write to parquet file
    result_df.write.mode("overwrite").parquet("../data/results")

if __name__ == '__main__':
    transform_data()

