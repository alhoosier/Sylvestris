#!/usr/bin/env python3
"""
stream_twitter.py

This module will open/persist a connection to Twitter for streaming data (filtered by keywords) to a Postgres DB.
"""

import sys
import os
from contextlib import contextmanager
from urllib3.exceptions import ReadTimeoutError
from datetime import datetime
try:
    import simplejson as json
except ImportError:
    import json
import argparse
from argparse import ArgumentParser
import yaml
import logging
import tweepy
from tweepy.streaming import StreamListener
import psycopg2
from psycopg2 import pool as pg_pool

DB_POOL = None
DB_CONN = None
DB_DSN = None
DB_CONFIG_PATH = 'stream_twitter/conf/pg_db_conn.yml'
TWITTER_CONFIG_PATH = 'stream_twitter/conf/twitter_conn.yml'


def get_cli_args(args):
    """
    Get kwargs* from command line and store them in variables for later use.

    :return: args dict that include the kwargs* and their values
    """
    parser = ArgumentParser(description='Program to stream Twitter data, filtered by keywords, directly to a DB.')
    parser.add_argument('--kw-filter', dest='keywords', type=str, help='Comma-separated list of keywords to filter the '
                                                                       'stream listener on.')
    try:
        parsed_args = parser.parse_args(args)
        filter_keywords = list(map(lambda x: x.strip(), parsed_args.keywords.split(',')))
        return filter_keywords
    except argparse.ArgumentError as e:
        logging.error(str(e))
        raise SystemExit


def configure_logging():
    """
    Configures program to log different formatted outputs based on the log level.

    :return: None
    """
    # Get the current date/time to include in the log file name
    current_dt = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    # Configure logging to a file for DEBUG messages or higher within the logs/ directory
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=os.path.abspath('stream_twitter/logs/{current_dt}.log'.format(current_dt=current_dt)),
                        filemode='w')
    # Define a handler that outputs to INFO messages or higher to console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # Set a simpler format for the console to avoid clutter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s')
    # Tell console handler to use the above defined format
    console.setFormatter(formatter)
    # Add the console handler to the root logger
    logging.getLogger('').addHandler(console)


def get_config(config_file_path):
    """
    Read and return the contents of a YAML configuration file as a dict.

    :param config_file_path: relative or absolute path of configuration file
    :return: config: contents of configuration file as dict object
    """
    try:
        with open(config_file_path) as config_file:
            config = yaml.safe_load(config_file)
        return config
    except (IOError, FileNotFoundError) as e:
        logging.error(str(e))
        raise SystemExit


def get_db_dsn(config_path=None):
    """
    Get DB DSN string from the DB configuration file

    :param config_path: path to the DB configuration file
    :return: DB DSN string
    """
    if not config_path:
        db_config = get_config(os.path.abspath(DB_CONFIG_PATH))
    else:
        db_config = get_config(os.path.abspath(config_path))

    # Unpack DB configuration variables
    db_host = db_config.get('host')
    db_port = db_config.get('port')
    db_user = db_config.get('username')
    db_user_pass = db_config.get('password')
    db_name = db_config.get('db_name')
    db_ssl_mode = db_config.get('ssl_mode')

    dsn = f'host={db_host} port={db_port} user={db_user} password={db_user_pass} dbname={db_name} sslmode={db_ssl_mode}'

    return dsn


def get_tweepy_api(consumer_key, consumer_secret, access_token, access_token_secret):
    """
    Connect to Twitter using the provided authentication details and Tweepy's API.
    This was built in reference to Tweepy's API Guide: http://docs.tweepy.org/en/v3.5.0/auth_tutorial.html

    :param consumer_key: consumer access key for accessing Twitter's API
    :param consumer_secret: consumer secret to authenticate consumer
    :param access_token: access token key for application's permission to Twitter's API
    :param access_token_secret: access token secret to authenticate application
    :return: tweepy_api: Tweepy API object
    """
    try:
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        tweepy_api = tweepy.API(auth)
        return tweepy_api
    except tweepy.TweepError as e:
        logging.error(str(e))
        raise SystemExit


@contextmanager
def get_db_connection():
    """
    Connect to a Postgres DB and create a session with the provided connection details and return a connection object.

    :return: database connection object
    """
    global DB_DSN
    if not DB_DSN:
        DB_DSN = get_db_dsn()

    global DB_POOL
    if not DB_POOL:
        DB_POOL = pg_pool.SimpleConnectionPool(1, 10, DB_DSN)
        # DB_CONN = psycopg2.connect(DB_DSN)

    global DB_CONN
    if not DB_CONN:
        try:
            logging.info('Getting connection from pool...')
            DB_CONN = DB_POOL.getconn()
        except psycopg2.InterfaceError as e:
            if 'connection already closed' in str(e):
                # If db connection is already closed then get another from the pool
                logging.info('Connection already closed so closing and fetching another from the pool.')
                DB_POOL.putconn(DB_CONN)
                DB_CONN = DB_POOL.getconn()
                yield DB_CONN
            else:
                logging.error(str(e))
                raise SystemExit
        except psycopg2.Error as e:
            logging.error(e)
            DB_CONN.rollback()
            DB_CONN.close()
            raise SystemExit
        else:
            logging.info('Connection to database established.')
            yield DB_CONN
            DB_CONN.commit()
    else:
        yield DB_CONN


def db_execute(exec_statement, exec_vars=None):
    """
    Execute a given statement against a database from the provided connection.

    :param exec_vars: tuple or dictionary of variables to pass into exec_statement
        (Ref: http://initd.org/psycopg/docs/usage.html#query-parameters)
    :param exec_statement: SQL database statement to execute
    :return: None
    """
    with get_db_connection() as conn:
        try:
            db_cursor = conn.cursor()
            db_cursor.execute(exec_statement, exec_vars)
            conn.commit()
        except (psycopg2.DatabaseError, ConnectionError) as e:
            if isinstance(e, ConnectionError) or (isinstance(e, psycopg2.DatabaseError)
                                                  and 'Operation timed out' in str(e)):
                # If operation timed out or a connection error occurred then attempt to get a new connection
                # else quit program
                logging.info(e)
                # Reset global DB connection
                global DB_CONN
                DB_CONN = None
                # Attempt db_execute again which should pull another connection from the pool
                db_execute(exec_statement, exec_vars)
            else:
                logging.error(str(e))
                raise SystemExit


def write_to_console(write_str):
    """
    Write string to console while replacing previous line in console
    :param write_str: sting to write to console
    :return: None
    """
    # Clear out terminal line using ANSI escape code
    sys.stdout.write('\x1b[2K\r')
    # Sandwich write_str between tab-space and carriage return for alignment and cursor placement to overwrite later
    sys.stdout.write(f'\t{write_str}\r')
    sys.stdout.flush()


class MyDbStreamListener(StreamListener):
    """
    Override tweepy.StreamListener to require db_dsn at initialization and modify/add methods for writing data to a
    database.
    Source Credit: http://tableaujunkie.com/post/135404208188/creating-a-live-twitter-feed
    """
    tweet_count = 0
    total_tweet_count = 0

    def on_data(self, raw_data):
        """
        Override tweepy.StreamListener's on_data method to write incoming data to a database
        :param raw_data: data received from Twitter's API
        :return: None
        """

        # Write to console only and prepare for being overwritten by the next line
        write_to_console('Tweet found and ingesting data...')

        # Grab raw_data and ingest as JSON object
        try:
            data = json.loads(raw_data)
        except ValueError as e:
            logging.error(str(e))
            logging.error('Raw data is not a valid JSON object so no processing can be done. Ignoring data.')
            return

        # The following includes custom processing that does not exist in tweepy.StreamListener
        # Convert JSON keys into variables that correspond to database columns
        tweet = data.get('text')
        created_at = data.get('created_at')
        favorite_count = data.get('favorite_count')
        is_favorite = data.get('favorited')
        filter_level = data.get('filter_level')
        id_str = data.get('id_str')
        lang = data.get('lang')
        retweet_count = data.get('retweet_count')
        is_retweeted = data.get('retweeted')
        source = data.get('source')
        timestamp_ms = data.get('timestamp_ms')
        is_truncated = data.get('truncated')
        user_description = data.get('user', {}).get('description')
        user_favorites_count = data.get('user', {}).get('favourites_count')
        user_followers_count = data.get('user', {}).get('followers_count')
        user_friends_count = data.get('user', {}).get('friends_count')
        user_id_str = data.get('user', {}).get('id_str')
        user_location = data.get('user', {}).get('location')
        user_name = data.get('user', {}).get('name')
        user_profile_image_url = data.get('user', {}).get('profile_image_url')
        user_screen_name = data.get('user', {}).get('screen_name')
        user_statuses_count = data.get('user', {}).get('statuses_count')
        user_time_zone = data.get('user', {}).get('time_zone')
        # Flatten nested hashtags object into a delimited string
        hashtags = data.get('entities', {}).get('hashtags')
        if hashtags:
            hashtag_str = ', '.join(map(lambda x: x.get('text'), hashtags))
        else:
            hashtag_str = None

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
        # Tuple of SQL variables for parameters in the insert SQL statement above
        data_vars = (tweet, created_at, timestamp_ms, favorite_count, is_favorite, filter_level, id_str, lang,
                     retweet_count, is_retweeted, source, is_truncated, user_description, user_favorites_count,
                     user_followers_count, user_friends_count, user_id_str, user_location, user_name,
                     user_profile_image_url, user_screen_name, user_statuses_count, user_time_zone, hashtag_str)

        # Write to console only and prepare for being overwritten by the next line
        write_to_console('Writing tweet to database...')
        # Store tweet write time to track last time a tweet was written to database from the console
        tweet_write_time = datetime.now().strftime('%Y-%m-%d %I:%M%p')
        # Write tweet data to database by executing the INSERT SQL statement string from above with provided parameters
        db_execute(insert_sql, data_vars)

        # Increment tweet_count to track # of tweets captured so far
        self.tweet_count += 1

        # Handle the plurality of "tweet" when building the tweet count status line to track progress
        if self.tweet_count == 1:
            tweet_count_status = f"{self.tweet_count} tweet successfully written to database."
        else:
            tweet_count_status = f"{self.tweet_count} tweets successfully written to database. Last tweet written" \
                                 f" at: {tweet_write_time}"

        # Write to console only and prepare for being overwritten by the next line
        write_to_console(tweet_count_status)

        # Bring back some of the default data checking from the inherited StreamListener class
        if 'limit' in data:
            if self.on_limit(data['limit']['track']) is False:
                return False
        elif 'disconnect' in data:
            if self.on_disconnect(data['disconnect']) is False:
                return False
        elif 'warning' in data:
            if self.on_warning(data['warning']) is False:
                return False
        # Return True to verify data retrieval and processing was successful
        return True

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


def main(argv=None):
    """
    Main function to run program.

    :return: None
    """

    # Parse arguments from command line if provided
    argv = argv or sys.argv[1:]
    filter_keywords = get_cli_args(argv)

    # Initialize logging
    configure_logging()
    logging.info('\n\n************************ Starting Twitter Streaming Script ************************\n')
    logging.info('Using the following keywords to filter the stream: %s', filter_keywords)

    # Get default configuration paths
    twitter_config = get_config(TWITTER_CONFIG_PATH)

    # Unpack twitter configuration variables
    consumer_key = twitter_config.get('consumer_key')
    consumer_secret = twitter_config.get('consumer_secret')
    access_token = twitter_config.get('access_token')
    access_token_secret = twitter_config.get('access_token_secret')

    # Generate Tweepy API object for authentication and connecting to Twitter
    tweepy_api = get_tweepy_api(consumer_key,
                                consumer_secret,
                                access_token,
                                access_token_secret
                                )
    # Custom listener class set to write to Postgres DB when data is ingested
    listener = MyDbStreamListener()

    # Store log start time for later use when calculating tweets_per_sec
    start_log_time = datetime.now()

    # Set up infinite loop to listen to Twitter stream that can be exited safely from the CLI
    try:
        while True:
            # Store loop's start time to calculate tweets_per_sec for this loop
            start_loop_time = datetime.now()
            # Format the current date/time for logging purposes
            start_loop_time_str = start_loop_time.strftime('%Y-%m-%d %I:%M%p')

            try:
                # Initialize twitter stream
                logging.info('Started listening to Twitter stream at: %s', start_loop_time_str)
                stream = tweepy.Stream(auth=tweepy_api.auth, listener=listener)
                # Start stream and listen/filter for keywords
                stream.filter(track=filter_keywords)
            except tweepy.TweepError as e:
                logging.error(str(e))
                raise SystemExit
            except (ReadTimeoutError, KeyboardInterrupt) as e:
                listener.total_tweet_count += listener.tweet_count
                # Store the loop's end time to calculate the loop's tweets_per_sec
                end_loop_time = datetime.now()
                end_loop_time_str = end_loop_time.strftime('%Y-%m-%d %I:%M%p')
                tweets_per_sec = listener.tweet_count / (end_loop_time - start_loop_time).seconds
                if isinstance(e, ReadTimeoutError):
                    logging.info('Read timeout occurred so restarting stream. %d tweets captured at %.2f tweets per '
                                 'second. %d tweets total.',
                                 listener.tweet_count, tweets_per_sec, listener.total_tweet_count)
                    listener.tweet_count = 0
                    pass
                else:
                    logging.info('Listening stopped at: %s. %d tweets captured at %.2f tweets per second. ',
                                 end_loop_time_str, listener.total_tweet_count, tweets_per_sec)
                    raise SystemExit
            else:
                # Store the end time for logging
                interrupt_time = datetime.now()
                interrupt_time_str = interrupt_time.strftime('%Y-%m-%d %I:%M%p')
                listener.total_tweet_count += listener.tweet_count
                logging.info('Ending stream listener at %s. Captured %d tweets in total.', interrupt_time_str,
                             listener.total_tweet_count)
    except KeyboardInterrupt:
        # Store end stream time to calculate tweets_per_sec
        end_stream_time = datetime.now()
        # Format the date/time for logging
        end_stream_time_str = end_stream_time.strftime('%Y-%m-%d %I:%M%p')
        tweets_per_sec = listener.tweet_count / (end_stream_time - start_log_time).seconds
        logging.info('Listening stopped at: %s. %d tweets captured at %.2f tweets per second. ', end_stream_time_str,
                     listener.total_tweet_count, tweets_per_sec)
        raise SystemExit


if __name__ == '__main__':
    main()
