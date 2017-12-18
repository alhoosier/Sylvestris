#!/usr/bin/env python3
"""
stream_twitter.py

This module will open/persist a connection to Twitter for streaming data (filtered by keywords) to a Postgres DB.
"""
# Standard Python libraries
import argparse
import logging
import sys
from argparse import ArgumentParser

from urllib3.exceptions import ReadTimeoutError

# Application Python Modules
import local_utils
from db_constants import *
from twitter_constants import *

# External Python Libraries
try:
    import simplejson as json  # Use simplejson if available for performance improvements
except ImportError:
    import json
import tweepy
from tweepy.streaming import StreamListener
import psycopg2


class TwitterMain:
    """
    Main Twitter class

    """

    def __init__(self, keywords=None):
        self.keywords = keywords or self.get_keywords_from_cli()
        self.db = PostgresDbMain()
        try:
            # Set up Tweepy API using credentials from twitter_constants
            self.auth = tweepy.OAuthHandler(CONST_CONSUMER_KEY, CONST_CONSUMER_SECRET)
            self.auth.set_access_token(CONST_ACCESS_TOKEN, CONST_ACCESS_TOKEN_SECRET)
            self.api = tweepy.API(self.auth)
        except tweepy.TweepError as e:
            logging.error(e)
            raise SystemExit

    @staticmethod
    def get_keywords_from_cli():
        """
        Get kwargs* from command line and store them in variables for later use.

        :return: args dict that include the kwargs* and their values
        """
        # Parse keywords from command line if provided
        args = sys.argv[1:]

        parser = ArgumentParser(description='Program to stream Twitter data, filtered by keywords, directly to a DB.')
        parser.add_argument('--keywords', dest='keywords', type=str,
                            help='Comma-separated list of keywords to filter the '
                                 'stream listener on.')
        try:
            parsed_args = parser.parse_args(args)
            keywords = list(map(lambda x: x.strip(), parsed_args.keywords.split(',')))

            return keywords
        except argparse.ArgumentError as e:
            logging.error(str(e))
            raise SystemExit

    def get_streaming_data(self):
        """
        Get streaming data from Twitter API via Tweepy and write to the database

        :return:
        """
        logging.info('\n\n************************ Starting Twitter Stream ************************\n')
        # Custom listener class set to write to Postgres DB when data is ingested
        listener = TwitterListener(self.db)

        # Store log start time for later use when calculating tweets_per_sec
        start_log_time = local_utils.get_current_local_date_time()

        # Set up infinite loop to listen to Twitter stream that can be exited safely from the CLI
        try:
            while True:
                # Store loop's start time to calculate tweets_per_sec for this loop
                start_loop_time = local_utils.get_current_local_date_time()
                # Format the current date/time for logging purposes
                start_loop_time_str = start_loop_time.strftime('%Y-%m-%d %I:%M%p')

                try:
                    # Initialize twitter stream
                    logging.info('Started listening to Twitter stream at %s.', start_loop_time_str)
                    stream = tweepy.Stream(self.auth, listener=listener)
                    # Start stream and listen/filter for keywords
                    logging.info('Using the following keywords to filter the stream: %s', self.keywords)
                    stream.filter(track=self.keywords)
                except tweepy.TweepError as e:
                    logging.error(e)
                    raise SystemExit
                except (ReadTimeoutError, KeyboardInterrupt) as e:
                    listener.total_tweet_count += listener.tweet_count
                    # Store the loop's end time to calculate the loop's tweets_per_sec
                    end_loop_time = local_utils.get_current_local_date_time()
                    end_loop_time_str = end_loop_time.strftime('%Y-%m-%d %I:%M%p')
                    tweets_per_sec = local_utils.calc_rate_per_sec(listener.tweet_count, start_loop_time,
                                                                   end_loop_time)
                    if isinstance(e, ReadTimeoutError):
                        logging.info(
                            'Read timeout occurred so restarting stream. %s tweets captured at %.2f tweets per '
                            'second. %s tweets total.',
                            '{:,}'.format(listener.tweet_count), tweets_per_sec,
                            '{:,}'.format(listener.total_tweet_count))
                        listener.tweet_count = 0
                        pass
                    else:
                        logging.info('Listening stopped at %s. %s tweets captured at %.2f tweets per second. ',
                                     end_loop_time_str, '{:,}'.format(listener.total_tweet_count), tweets_per_sec)
                        raise SystemExit
                else:
                    # Store the end time for logging
                    interrupt_time = local_utils.get_current_local_date_time()
                    interrupt_time_str = interrupt_time.strftime('%Y-%m-%d %I:%M%p')
                    listener.total_tweet_count += listener.tweet_count
                    logging.info('Ending stream listener at %s. Captured %s tweets in total.', interrupt_time_str,
                                 '{:,}'.format(listener.total_tweet_count))
        except KeyboardInterrupt:
            # Store end stream time to calculate tweets_per_sec
            end_stream_time = local_utils.get_current_local_date_time()
            # Format the date/time for logging
            end_stream_time_str = end_stream_time.strftime('%Y-%m-%d %I:%M%p')
            tweets_per_sec = local_utils.calc_rate_per_sec(listener.tweet_count, start_log_time, end_stream_time)
            logging.info('Listening stopped at %s. %s tweets captured at %.2f tweets per second. ', end_stream_time_str,
                         '{:,}'.format(listener.total_tweet_count), tweets_per_sec)
            raise SystemExit

    def get_trends(self):
        # trends_place takes a WEOID that corresponds to a geographic location
        # ref: http://docs.tweepy.org/en/v3.5.0/api.html#API.trends_place
        # 1 = global
        # 23424977 = US
        trends = self.api.trends_place(1)

        for trend in trends[0]["trends"]:
            trend_tweets = [trend['name']]
            tweepy_cur = tweepy.Cursor(self.api.search, q=trend['name']).items(3)

            for tweet in tweepy_cur:
                trend_tweets.append(self.get_tweet_html(tweet.id))
                # insert_sql = ('INSERT INTO data_raw.twitter_stream.trend_data\n '
                #               '()'
                #               'VALUES\n'
                #               '(%s,%s,%s,%s, CURRENT_TIMESTAMP)')
                # self.db.execute(insert_sql, trend_tweets)
                logging.info('Trend Tweets: %s', trend_tweets)

    def get_tweet_html(self, tweet_id):
        oembed = self.api.get_oembed(id=tweet_id, hide_media=True, hide_thread=True)

        tweet_html = oembed['html'].strip("\n")

        return tweet_html


class TwitterListener(StreamListener):
    """
    Override tweepy.StreamListener to add database and improve logging and tracking of tweets captured

    Source Credit: http://tableaujunkie.com/post/135404208188/creating-a-live-twitter-feed
    """
    def __init__(self, db):
        self.db = db
        self.data = None
        self.tweet_write_time = None
        self.tweet_count = 0
        self.total_tweet_count = 0
        self.listen_start_time = local_utils.get_current_local_date_time()
        super().__init__()

    def on_data(self, raw_data):
        """
        Override tweepy.StreamListener's on_data method to write incoming data to a database

        :param raw_data: data received from Twitter's API
        :return: None
        """

        # Write to console only and prepare for being overwritten by the next line
        local_utils.send_status_to_console('Tweet found and ingesting data...')

        try:
            # Grab raw_data and ingest as JSON object
            self.data = json.loads(raw_data)
            # Process data and generate SQL to write data to database
            processed_data, insert_sql = self.process_data()
            # Write status to console only and prepare for being overwritten by the next line
            local_utils.send_status_to_console('Writing tweet to database...')
            # Write tweet data to database
            self.db.execute(insert_sql, processed_data)
            # Store tweet write time to track last time a tweet was written to database from the console
            self.tweet_write_time = local_utils.get_current_local_date_time()
            # Write status of tweets captured by stream to console and log file
            self.get_status()
        except ValueError as e:
            logging.error(str(e))
            logging.error('Raw data is not a valid JSON object so no processing can be done. Ignoring data.')
            return

        # Bring back some of the default data checking from the inherited StreamListener class
        if 'limit' in self.data:
            if self.on_limit(self.data['limit']['track']) is False:
                return False
        elif 'disconnect' in self.data:
            if self.on_disconnect(self.data['disconnect']) is False:
                return False
        elif 'warning' in self.data:
            if self.on_warning(self.data['warning']) is False:
                return False
        # Return True to verify data retrieval and processing was successful
        return True

    def process_data(self):
        # This is to avoid the following error when inserting into database:
        # ValueError: A string literal cannot contain NUL (0x00) characters.
        # Example Fix: https://github.com/matrix-org/synapse/pull/2491
        for key, val in self.data.items():
            if '\0' == val:
                self.data[key] = 'NULL'

        # The following includes custom processing that does not exist in tweepy.StreamListener
        # Convert JSON keys into variables that correspond to database columns
        tweet = self.data.get('text')
        created_at = self.data.get('created_at')
        favorite_count = self.data.get('favorite_count')
        is_favorite = self.data.get('favorited')
        filter_level = self.data.get('filter_level')
        id_str = self.data.get('id_str')
        lang = self.data.get('lang')
        retweet_count = self.data.get('retweet_count')
        is_retweeted = self.data.get('retweeted')
        source = self.data.get('source')
        timestamp_ms = self.data.get('timestamp_ms')
        is_truncated = self.data.get('truncated')
        user_description = self.data.get('user', {}).get('description')
        user_favorites_count = self.data.get('user', {}).get('favourites_count')
        user_followers_count = self.data.get('user', {}).get('followers_count')
        user_friends_count = self.data.get('user', {}).get('friends_count')
        user_id_str = self.data.get('user', {}).get('id_str')
        user_location = self.data.get('user', {}).get('location')
        user_name = self.data.get('user', {}).get('name')
        user_profile_image_url = self.data.get('user', {}).get('profile_image_url')
        user_screen_name = self.data.get('user', {}).get('screen_name')
        user_statuses_count = self.data.get('user', {}).get('statuses_count')
        user_time_zone = self.data.get('user', {}).get('time_zone')
        # Flatten nested hashtags object into a delimited string
        hashtags = self.data.get('entities', {}).get('hashtags')
        if hashtags:
            hashtag_str = ', '.join(map(lambda x: x.get('text'), hashtags))
        else:
            hashtag_str = None

        processed_data = (tweet, created_at, timestamp_ms, favorite_count, is_favorite, filter_level, id_str, lang,
                          retweet_count, is_retweeted, source, is_truncated, user_description, user_favorites_count,
                          user_followers_count, user_friends_count, user_id_str, user_location, user_name,
                          user_profile_image_url, user_screen_name, user_statuses_count, user_time_zone, hashtag_str)
        # Generate insert SQL statement using parameters as recommended
        # per http://initd.org/psycopg/docs/usage.html#query-parameters
        insert_sql = ('INSERT INTO data_raw.twitter_stream.twitter_raw_data\n'
                      '(tweet, created_at, timestamp_ms, favorite_count, is_favorite, filter_level, '
                      'id_str, lang, retweet_count, is_retweeted, "source", is_truncated, user_description, '
                      'user_favorites_count, user_followers_count, user_friends_count, user_id_str, '
                      'user_location, user_name, user_profile_image_url, user_screen_name, '
                      'user_statuses_count, user_time_zone, hashtags)\n'
                      'VALUES\n'
                      '(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,  %s, '
                      '%s, %s);')
        return processed_data, insert_sql

    def get_status(self):
        # Increment tweet_count to track # of tweets captured so far
        self.tweet_count += 1
        # Handle the plurality of "tweet" when building the tweet count status line to track progress
        if self.tweet_count == 1:
            tweet_count_status = f"{self.tweet_count} tweet successfully written to database."
        else:
            tweets_per_sec = local_utils.calc_rate_per_sec(self.tweet_count, self.listen_start_time,
                                                           self.tweet_write_time)
            tweet_write_time_str = self.tweet_write_time.strftime('%Y-%m-%d %I:%M%p')
            tweet_count_status = f"{self.tweet_count:,} tweets successfully written to database at " \
                                 f"{tweets_per_sec:.2f} tweets per second. Last tweet written at " \
                                 f"{tweet_write_time_str}."

        # Write to console only and prepare for being overwritten by the next line
        local_utils.send_status_to_console(tweet_count_status)

    def on_error(self, status_code):
        """
        Return different error messages depending on Twitter's status code.

        :param status_code: Twitter HTTP status codes per https://developer.twitter.com/en/docs/basics/response-codes
        :return: returns false to disconnect the stream within the on_data method
        """
        if status_code == 420:
            logging.error('Rate limit was hit so disconnecting stream.')
            return False
        elif status_code == 401:
            logging.error('Unauthorized request was made so disconnecting stream.')
        else:
            return False


class PostgresDbMain:
    """
    Postgres Db Main class

    """
    # DSN generated from db_constants
    dsn = f'host={CONST_DB_HOST} port={CONST_DB_PORT} ' \
          f'user={CONST_DB_USER} password={CONST_DB_PASS}' \
          f' dbname={CONST_DB_NAME} sslmode={CONST_DB_SSL_MODE}'

    def __init__(self):
        self.conn = self.get_connection()

    def get_connection(self):
        """
        Connect to a Postgres DB and create a session with the provided connection details and return a connection
        object.

        :return: database connection object
        """
        try:
            logging.info('Getting database connection...')
            db_conn = psycopg2.connect(self.dsn)
            logging.info('Connection to database has been established.')
            return db_conn
        except psycopg2.Error as e:
            logging.error(e)
            raise SystemExit

    def execute(self, exec_statement, exec_vars=None):
        """
        Execute a given statement against a database from the provided connection.

        :param exec_vars: tuple or dictionary of variables to pass into exec_statement
            (Ref: http://initd.org/psycopg/docs/usage.html#query-parameters)
        :param exec_statement: SQL database statement to execute
        :return: None
        """
        try:
            db_cursor = self.conn.cursor()
            db_cursor.execute(exec_statement, exec_vars)
            self.conn.commit()
        # If error has to do with the connection/operation timing out then reset connection and try again
        except (psycopg2.DatabaseError, ConnectionError) as e:
            if isinstance(e, ConnectionError) or (isinstance(e, psycopg2.DatabaseError)
                                                  and 'Operation timed out' in str(e)):
                try:
                    self.conn.close()
                    self.conn = self.get_connection()
                    self.execute(exec_statement, exec_vars)
                except psycopg2.Error as e:
                    logging.error(e)
                    raise SystemExit
        except psycopg2.Error as e:
            logging.error(e)
            raise SystemExit

    def __del__(self):
        self.conn.close()


def main():
    """
    Main function to run program.

    :return: None
    """

    # Initialize logging
    local_utils.configure_logging(log_file_path='logs')

    # Initialize TwitterMain class
    twit = TwitterMain()

    # Start streaming data until the program is interrupted
    twit.get_streaming_data()


if __name__ == '__main__':
    main()
