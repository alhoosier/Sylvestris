#!/usr/bin/env python3
"""
test_stream_twitter.py

This module will test the stream_twitter module using pytest.
"""

import os
import stream_twitter.stream_twitter as stream_twitter
import pytest

def test_get_cli_args():
    """
    Test and verify comma-delimited strings are parsed properly

    :return: True
    """
    assert stream_twitter.get_cli_args(['--kw-filter', '#This, #Is, #A, #Test']) == ['#This', '#Is', '#A', '#Test']


def test_invalid_get_cli_args():
    """
    Test and verify that strings not delimited by commas are not parsed properly

    :return: True
    """
    # get_cli_args only accepts comma-delimited strings
    test_param = '#This|#Is|#A|#Test'
    assert stream_twitter.get_cli_args(['--kw-filter', test_param]) != ['#This', '#Is', '#A', '#Test']


def test_configure_logging():
    """
    Test and verify the logging can be configured without issue

    :return: True
    """
    assert stream_twitter.configure_logging() is None


def test_get_config():
    """
    Test and verify that a YAML configuration file can be ingested and converted into a dictionary.

    :return: True
    """
    config_file_path = os.path.abspath('../conf/pg_db_conn_template.yml')
    assert stream_twitter.get_config(config_file_path)


def test_invalid_get_config():
    """
    Test and verify that the get_config function will cause program to exit if the configuration does not exist.

    :return: True
    """
    config_file_path = os.path.abspath('../conf/foo_bar.yml')
    with pytest.raises(SystemExit):
        stream_twitter.get_config(config_file_path)


def test_get_db_dsn():
    """
    Test and verify that DB DSN strings are built properly

    :return: True
    """
    config_file_path = os.path.abspath('../conf/pg_db_conn_template.yml')
    assert stream_twitter.get_db_dsn(config_file_path) == 'host=<DB_HOST> port=<DB_PORT> user=<DB_USER> ' \
                                                          'password=<DB_PASSWORD> dbname=<DB_NAME> ' \
                                                          'sslmode=<DB_SSL_MODE>'
