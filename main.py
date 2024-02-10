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

        logging.info('Starting GitHubData Extraction...')
        #Initialize the extract function
        extract_github_data()
        logging.info('GitHubData Extraction Completed.')

        logging.info('Starting GitHubData Transformation...')
        #Initialize the transform function
        transform_data()
        logging.info('GitHubData Transformation Completed.')

    except Exception as e:
        logging.error(f'An error occurred: {str(e)}')

if __name__ == "__main__":
    main()
