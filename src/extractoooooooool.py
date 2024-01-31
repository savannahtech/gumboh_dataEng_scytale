import os
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType
import requests
import json
import datetime
from pyspark.sql.types import StringType, IntegerType


def infer_schema(spark, data):
    # Infer schema dynamically from the first element of the data

    schema = StructType.fromJson(spark.createDataFrame([Row(**item) for item in data[:1]]).schema.json())
    
    return schema

def extract_github_data(token):

    # GitHub organization and repositories
    org_name = 'Scytale-exercise'
    url = f'https://api.github.com/orgs/{org_name}/repos'

    # Set the headers with the token for authentication
    headers = {'Authorization': f'token {token}'}

    # API call to extract repositories
    repos_data = requests.get(url, headers=headers).json()

    # Initialize Spark session
    spark = SparkSession.builder.appName("GitHubDataExtractor").getOrCreate()

    for repo in repos_data:
        # Check if repo is a dictionary
        if isinstance(repo, dict):
            pull_requests_url = f'https://api.github.com/repos/{org_name}/{repo["name"]}/pulls'

            # API call to extract pull requests for each repository
            pull_requests_data = requests.get(pull_requests_url, headers=headers).json()

            if pull_requests_data:
                # Infer schema dynamically
                # pull_request_schema = infer_schema(spark, pull_requests_data)

                pull_request_schema = default_schema = [
                        ('url', StringType(), True),
                        ('id', IntegerType(), True),                

                ]

                # Convert pull requests data to DataFrame
                pull_requests_rows = [Row(**item) for item in pull_requests_data]
                pull_requests_df = spark.createDataFrame(pull_requests_rows, schema=pull_request_schema)

                # Save pull requests data as JSON
                timestamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
                out_dir ='data/raw_data'
                if not os.path.exists(out_dir):
                    os.makedirs(out_dir)

                out_file =  f'pull_requests_data_{timestamp}.json'

                # Save pull requests data
                pull_requests_df.coalesce(1).write.mode("overwrite").json(out_file)

                print(f'Wrote pull requests data to {out_file}')
            else:
                print(f'No pull requests data for repository {repo["name"]}')
        else:
            print(f'Skipping non-dictionary entry: {repo}')

if __name__ == "__main__":
    # Replace 'YOUR_GITHUB_TOKEN' with your actual GitHub personal access token
    token = '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'
    extract_github_data(token)

