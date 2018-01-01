#!/usr/bin/env python3
"""
test_stream_twitter.py

This module will tests the stream_twitter module using pytest.
"""
from __future__ import print_function
import tweepy

from stream_twitter import stream_twitter
from stream_twitter.stream_twitter import TwitterListener


class TestTwitterListener(TwitterListener):
    def __init__(self, max_tweet_count=1):
        super().__init__(max_tweet_count)

    def write_data_to_db(self):
        # Generate insert SQL statement using parameters as recommended
        # per http://initd.org/psycopg/docs/usage.html#query-parameters
        insert_sql = ('INSERT INTO data_raw.test_twitter_stream.test_twitter_raw_data\n'
                      '(tweet, created_at, timestamp_ms, favorite_count, is_favorite, filter_level, '
                      'id_str, lang, retweet_count, is_retweeted, "source", is_truncated, user_description,'
                      ' user_favorites_count, user_followers_count, user_friends_count, user_id_str, '
                      'user_location, user_name, user_profile_image_url, user_screen_name, '
                      'user_statuses_count, user_time_zone, hashtags)\n'
                      'VALUES\n'
                      '(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,'
                      ' %s, %s, %s);')
        # Write tweet data to database
        self.db.execute(insert_sql, self.processed_data)


def test_streaming_data():
    test_keywords = ['#blacktwitter']
    test_listener = TestTwitterListener()
    print('\n\n************************ Starting Twitter Stream ************************\n')
    twit = stream_twitter.TwitterMain(keywords=test_keywords, listener=test_listener)
    print('TwitterMain initialized.')
    try:
        # Initialize twitter stream
        print('Initialize Twitter stream')
        stream = tweepy.Stream(twit.auth, listener=twit.listener)
        # Start stream and listen/filter for keywords
        print(f'Listening to the stream for the following keywords: {twit.keywords}')
        stream.filter(track=twit.keywords)
    except tweepy.TweepError as e:
        assert False, e
    else:
        assert True
