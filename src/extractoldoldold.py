from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import requests
import json
import datetime
import os

def extract_github_data(token):

    # GitHub organization and repositories
    org_name = 'Scytale-exercise'
    url = f'https://api.github.com/orgs/{org_name}/repos'

    # Set the headers with the token for authentication
    headers = {'Authorization': f'token {token}'}

    # API call to extract repositories
    repos_data = requests.get(url, headers=headers).json()

    # Print repos_data to understand its structure
    print("Repos Data:", repos_data)

    # Initialize Spark session
    spark = SparkSession.builder.appName("GitHubDataExtractor").getOrCreate()

    for repo in repos_data:
        # Check if repo is a dictionary
        if isinstance(repo, dict):
            repo_name = repo.get('name', '')
            pull_requests_url = f'https://api.github.com/repos/{org_name}/{repo_name}/pulls'

            # API call to extract pull requests for each repository
            pull_requests_data = requests.get(pull_requests_url, headers=headers).json()

            if pull_requests_data:
                # Convert pull requests data to DataFrame
                pull_requests_df = spark.createDataFrame(pull_requests_data)

                # Save pull requests data as JSON
                timestamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
                out_dir = f'../data/raw_data/{org_name}/{repo_name}'
                if not os.path.exists(out_dir):
                    os.makedirs(out_dir)

                out_file = f'{out_dir}/pull_requests_data_{timestamp}.json'

                # Save pull requests data
                pull_requests_df.coalesce(1).write.mode("overwrite").json(out_file)

                print(f'Wrote pull requests data to {out_file}')
            else:
                print(f'No pull requests data for repository {repo_name}')
        else:
            print(f'Skipping non-dictionary entry: {repo}')

if __name__ == "__main__":
    # Replace 'YOUR_GITHUB_TOKEN' with your actual GitHub personal access token
    token = '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'
    extract_github_data(token)

