# PySpark GitHub ETL Assignment

This project uses PySpark to extract GitHub data from an organization, and it includes scripts for data extraction and transformation.

## Project Structure

- `pyspark_gh/`
  - `main.py` : Entry point of the project.
  - `data/`
    - `prs_data/` : Directory to store raw GitHub pull requests data in JSON format.
    - `results/` : Directory to store the transformed data in Parquet format.
- `scripts/`
  - `extract.py` : Script for extracting GitHub data.
  - `transform.py` : Script for transforming the extracted data.

## Prerequisites

- Python 3.x
- PySpark
- Hadoop

## Setup

1. Clone the repository:

    ```bash
    git clone https://github.com/your-username/your-repository.git
    cd your-repository
    ```

2. Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

3. Set up GitHub token:

    Obtain a GitHub personal access token and replace `'YOUR_GITHUB_TOKEN'` in `main.py` with your actual token.

## Usage

To run the project, execute the following commands:

```bash
python3 pyspark_gh/main.py
