import os
import requests
import json
from pyspark.sql import Row
from pyspark.sql.functions import col, when, split, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql import SparkSession #I had to keep this since pd was acting up

# starting a SparkSession
if SparkSession.getActiveSession():
    spark = SparkSession.getActiveSession() 
else:
    spark = SparkSession.builder.appName("readJSONdata").getOrCreate()


def merge_json_files(input_dir, output_file):
    merged_data = []

    # List all JSON files in the input directory
    json_files = [file for file in os.listdir(input_dir) if file.endswith(".json")]

    for json_file in json_files:
        file_path = os.path.join(input_dir, json_file)

        # Read each JSON file and append its content to the merged_data list
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                merged_data.extend(data)

        except Exception as e:
            print(f"Error reading JSON file {json_file}: {e}")

    # Write the merged_data to the output file
    try:
        output_file_path = os.path.join(input_dir, output_file)
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(merged_data, f)

        print(f"Merged JSON data saved to: {output_file_path}")

    except Exception as e:
        print(f"Error saving merged JSON data to file: {e}")


def extract_github_data():

    # GitHub organization and repositories
    org_name = 'Scytale-exercise'
    url = f'https://api.github.com/orgs/{org_name}/repos'

    # Set the headers with the token for authentication
    # headers = {'Authorization': f'token{token}'}
    headers = {}

    # API call to extract repositories
    repos_data = requests.get(url, headers=headers).json()

    for repo in repos_data:
        # Check if repo is a dictionary
        if isinstance(repo, dict):
            repo_name = repo.get('name', '')
            pull_requests_url = f'https://api.github.com/repos/{org_name}/{repo_name}/pulls'

            # API call to extract pull requests for each repository
            pull_requests_data = requests.get(url, headers=headers).json()
            #print("Repos Data:", pull_requests_data)

            if pull_requests_data:

                pull_requests_rows = [Row(**item) for item in pull_requests_data]
               
                first_item = pull_requests_rows[0].asDict()
                fields = [StructField(field, StringType(), True) for field in first_item.keys()]
                json_schema = StructType(fields)

                # Convert pull requests data to DataFrame
                pull_requests_df = spark.createDataFrame(pull_requests_rows, schema=json_schema)

                # Save pull requests data as JSON
                out_dir = 'data/raw_data'
                
                #ensure output directory exists if not, create it
                if not os.path.exists(out_dir):
                    os.makedirs(out_dir)

                #make the output file path
                #out_file = f'{out_dir}/pulls_{repo_name}_{timestamp}.json'
                out_file = f'{out_dir}/pulls_{repo_name}.json'

                # Save pull requests data
                try:
                    with open(out_file, 'w', encoding='utf-8') as f:
                        json.dump(pull_requests_data, f)

                    print(f"GitHubData has been pulled and saved in JSON format to: {out_file}")

                except Exception as e:
                    print(f"Error saving JSON data to file: {e}")
                    raise

                print(f'Wrote pull requests data to {out_file}')
                merge_json_files(out_dir, 'repo_zote.json')
            else:
                print(f'No pull requests data for repository {repo_name}')
        else:
            print(f'Skipping non-dictionary entry: {repo}')

    # close sparksession
    #spark.stop()


if __name__ == "__main__":
    extract_github_data()
