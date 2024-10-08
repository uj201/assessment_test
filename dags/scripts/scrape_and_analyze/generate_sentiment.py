import random
# from textblob import TextBlob

# To Compute sentiment score
def compute_value(row):
    # sentiment_score = TextBlob(row["article_content"]).sentiment.polarity
    sentiment_score = round(random.uniform(0, 1), 3)
    return sentiment_score  # Example computation

# To generate sentiment and append it to the 
def generate_sentiment(df):
    df['sentiment_score'] = df.apply(compute_value, axis=1)
    return df

