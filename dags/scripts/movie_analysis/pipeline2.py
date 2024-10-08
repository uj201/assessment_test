import json
import pandas as pd
from bs4 import BeautifulSoup
import requests
import logging

# from query import *
from scripts.movie_analysis.query import *

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def movie_analyze():
    logger.info("Starting movie analysis")
    movie_analysis()
    logger.info("Movie analysis completed")
    return "Movie Analysis Done"

movie_analyze()

