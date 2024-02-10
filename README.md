# PySpark GitHub ETL Assignment

This project uses Python to extract GitHub data from a repository, save them as JSON in a local directory called "data/raw_data".

All the repo JSON files are then merged into a single JSON file named "repo_zote.json" that will be sent for transformation.

Then uses PySpark to transform the all the JSON data into a parquet file then saves it in a local directory named "data/results".

To observe separation of concerns, 
- all data is contained in "data" folder.
- all scripts are contained in "src" folder.
- the root directory contains "main.py" that initiates the project.
- the "requirements.txt" file is also included in the root folder.

## Project Structure

- `scytale-repo3/`
  - `main.py` : Entry point of the project.
  - `data/`
    - `raw_data/` : Directory to store raw GitHub pull requests data in JSON format.
    - `results/` : Directory to store the transformed data in Parquet format.
- `scr/`
  - `extract.py` : Script for extracting GitHub data.
  - `transform.py` : Script for transforming the extracted data.

## Prerequisites

- Python 3.x
- PySpark

## Setup

1. Clone the repository:
    git clone https://github.com/Scytale-exercise/scytale-repo3.git

2. Change into the cloned repository directory
    cd scytale-repo3

3. Install dependencies:
    pip install -r requirements.txt

4. To run the project, execute the following commands:
    python3 scytale-repo3/main.py

