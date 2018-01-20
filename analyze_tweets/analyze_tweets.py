#!/usr/bin/env python3
"""
analyze_tweets.py

This module will perform analytics on top of existing tweet data and write output data to Postgres DB.
"""
from utils import twitter_nlp_utils
from db.postgres_db import PostgresDbMain


class TwitterAnalytics:
    """
    Main TwitterAnalytics class

    """
    def __init__(self, db=None):
        self.tokenize_tweet_text = twitter_nlp_utils.tokenize_tweet_text
        self.get_tweet_sentiment = twitter_nlp_utils.get_tweet_sentiment
        self.db = db or PostgresDbMain()

    def get_tweet_text_data_data(self):
        query = 'SELECT tweet, id_str FROM data_raw.twitter_stream.twitter_raw_data;'
        return self.db.execute(query, return_results=True)

    def get_sentiment_data(self, data):
        processed_data = []
        for row in data:
            tweet_text, tweet_id = row
            processed_data.append((tweet_id, tweet_text, self.get_tweet_sentiment(tweet_text)))
        return processed_data

    def write_to_db(self, insert_sql, data):
        # Write tweet data to database
        self.db.execute(insert_sql, data)


def main():
    """
    Main function to run analyze_tweets.py

    :return: None
    """
    twit_analytics = TwitterAnalytics()
    tweet_text = twit_analytics.get_tweet_text_data_data()
    sentiment_data = twit_analytics.get_sentiment_data(tweet_text)
    # Generate insert SQL statement using parameters as recommended
    # per http://initd.org/psycopg/docs/usage.html#query-parameters
    insert_sentiments_sql = ('INSERT INTO data_raw.twitter_stream.tweet_sentiments\n'
                             '(tweet_id, tweet_text, tweet_sentiment)\n'
                             'VALUES\n'
                             '(%s, %s, %s);')
    twit_analytics.write_to_db(insert_sentiments_sql, sentiment_data)


if __name__ == '__main__':
    main()
