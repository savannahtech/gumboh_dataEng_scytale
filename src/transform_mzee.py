import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql import functions as F
import json
from pyspark.sql.functions import from_unixtime



spark = SparkSession.builder.appName('GithubETL').getOrCreate()

def transform_data():
      
    # Read all extract parquet files 
    extract_df = spark.read.json('data/raw_data')

    # Transformations
    transformed_df = extract_df.withColumn("created_date", F.unix_timestamp(col("created_at"), "yyy-MM-dd"))

    transformed_df = transformed_df.withColumn("repo_name", F.split(F.col("repo_url"), "/").getItem(4))  

    transformed_df = transformed_df.filter(F.col("language").contains("python"))

    # Write to results folder
    transformed_df.write.mode("overwrite").parquet("data/results")

if __name__ == '__main__':
    transform_data()
