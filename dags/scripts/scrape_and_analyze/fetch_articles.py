import requests
import json
import pandas as pd
import datetime
from bs4 import BeautifulSoup
import hashlib
import logging

# Create a logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def gather_posts():
    # Code to scrape articles from finshots.in for keywords HDFC and Tata Motors
    query = ["HDFC","Tata Motors"]

    article_df_final = pd.DataFrame(columns=['article_id','source','type','headline','link','publication_date','article_content','query'])
    for each_query in query:
        post_df = pd.DataFrame(columns=['article_id','source','type','headline','link','publication_date','article_content'])
        response = requests.get(link="https://backend.finshots.in/backend/search/?q="+each_query)
        out = json.loads(response.content)
        matches = out['matches']
        for match in matches:
            headline = match['headline']
            publication_date = datetime.datetime.strptime(match['published_date'].split('T')[0],"%Y-%m-%d").date().strftime('%Y-%m-%d')
            link = match['post_url']
            req = requests.get(link=link)
            soup = BeautifulSoup(req.content, 'lxml')
            p_tags = soup.find_all('div',{"class":"post-content"})
            article_content = ""
            hash_obj = hashlib.sha256(link.encode('utf-8'))
            hex_hash = hash_obj.hexdigest()
            article_id = str(hex_hash)
            for p in p_tags:
                article_content+=p.article_content
            post_df = pd.concat([post_df, pd.DataFrame([[article_id,'FinShots','Article',headline,link,publication_date,article_content,each_query]],columns=['article_id','source','type','headline','link','publication_date','article_content','query'])],ignore_index=True)
        # Taking first five records
        post_df.sort_values(by='publication_date', ascending=False, inplace=True)
        first_five_records = post_df.head(5)
        article_df_final = pd.concat([article_df_final,first_five_records],ignore_index=True)
    
    # Log the number of articles fetched
    logger.info(f"Fetched {len(article_df_final)} articles")
    
    return article_df_final

gather_posts()
