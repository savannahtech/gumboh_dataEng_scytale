import logging
from src.extract import extract_github_data
from src.transform import transform_data

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def main():
    try:
        setup_logging()

        github_token = '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'

        logging.info('Starting GitHub data extraction...')
        extract_github_data(github_token)
        logging.info('GitHub data extraction completed.')

        logging.info('Starting data transformation...')
        transform_data(github_token)
        logging.info('Data transformation completed.')

    except Exception as e:
        logging.error(f'An error occurred: {str(e)}')

if __name__ == "__main__":
    main()

