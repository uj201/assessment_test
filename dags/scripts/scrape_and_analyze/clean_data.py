import json
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_data(data):
    logger.info("Inside clean data: %s", data)
    # Code to clean and deduplicate the data
    # Remove any leading or trailing whitespace from headline and article_content
    data['headline'] = data['headline'].str.strip()
    data['article_content'] = data['article_content'].str.strip()
    data['link'] = data['link'].str.strip()

    # Convert headline and article_content to lowercase
    data['headline'] = data['headline'].str.lower()
    data['article_content'] = data['article_content'].str.lower()
    data['link'] = data['link'].str.lower()

    # Remove any duplicate rows based on headline and article_content
    # Code to clean and deduplicate the data
    data.drop_duplicates(subset=['headline', 'article_content'], inplace=True)
    data.sort_values(by='publication_date', ascending=False, inplace=True)

    logger.info(data)
    logger.info("End of clean data:")
    return data
