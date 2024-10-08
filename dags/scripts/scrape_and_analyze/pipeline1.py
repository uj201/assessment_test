import json
import pandas as pd
from bs4 import BeautifulSoup
import requests
import logging
from scripts.scrape_and_analyze.gather_posts import *
from scripts.scrape_and_analyze.clean_data import *
from scripts.scrape_and_analyze.generate_sentiment import *
from scripts.scrape_and_analyze.persist_data import *

# from gather_posts import *
# from clean_data import *
# from generate_sentiment import *
# from persist_data import *

# Create a logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def scrape_and_analyze():
    try:
        # Call for the gather_posts function
        data = gather_posts()
        logger.info('Fetched articles successfully.')

        # Call for the clean_data function
        cleaned_data = clean_data(data)
        logger.info('Cleaned data successfully.')

        # Call for the generate_sentiment function
        df = generate_sentiment(cleaned_data)
        logger.info('Generated sentiment successfully.')

        # Save the data to a database
        persist_data(df)
        logger.info('Persisted data successfully.')

    except Exception as e:
        logger.error(f'An error occurred: {str(e)}')

scrape_and_analyze()
