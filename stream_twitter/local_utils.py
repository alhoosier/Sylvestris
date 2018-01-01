#!/usr/bin/env python3
"""
local_utils.py

This module will contain generic utility functions that can be used application-wide.
"""
# Standard Python Libraries
import os
import sys
import time
import logging
from datetime import datetime, timezone

# External Python Libraries
import pytz


def get_current_local_date_time(timezone_name='America/Los_Angeles'):
    """
    Return the current timestamp in local time zone (US Pacific is default)

    :return: current timestamp in local time zone (US Pacific is default)
    """
    if time.tzname == ('PST', 'PDT'):
        return datetime.now()
    else:
        local_tz = pytz.timezone(timezone_name)
        return datetime.now().replace(tzinfo=timezone.utc).astimezone(tz=local_tz)


def configure_logging(log_file_path=None, log_filemode='w', file_log_level=logging.INFO):
    """
    Configures program to log different formatted outputs based on the log level.

    :param log_file_path: path to directory where log files should be stored
    :param log_filemode: filemode to use when saving logs to file (ref:) (default: w)
    :param file_log_level: logging level to capture within log files (default: logging.INFO)

    :return: None
    """
    # Get the current date/time to include in the log file name
    current_dt = get_current_local_date_time()
    # Configure logging to a file for DEBUG messages or higher within the logs/ directory if log_file_path is provided
    if log_file_path:
        logging.basicConfig(level=file_log_level,
                            format='%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s',
                            datefmt='%Y-%m-%d %I:%M:%S %p %Z',
                            filename=os.path.abspath(os.path.join(log_file_path,
                                                                  '{current_dt}.log'.format(current_dt=current_dt))),
                            filemode=log_filemode)
    # Define a handler that outputs to INFO messages or higher to console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # Set a simpler format for the console to avoid clutter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s')
    # Tell console handler to use the above defined format
    console.setFormatter(formatter)
    # Add the console handler to the root logger
    logging.getLogger('').addHandler(console)


def send_status_to_console(status_str):
    """
    Send status to console while replacing previous line in console

    :param status_str: status string to write to console
    :return: None
    """
    # Clear out terminal line using ANSI escape code
    sys.stdout.write('\x1b[2K\r')
    # Sandwich write_str between tab-space and carriage return for alignment and cursor placement to overwrite later
    sys.stdout.write(f'\t{status_str}\r')
    sys.stdout.flush()


def calc_rate_per_sec(val, start_time, end_time):
    """
    Calculate the rate per second for a given value and start/stop time

    :param val: value to calculate rate across date range for
    :param start_time: start datetime object
    :param end_time: end datetime object
    :return: tweets_per_sec
    """
    try:
        rate = val / (end_time - start_time).seconds
    except ZeroDivisionError:
        rate = 0.0
    return rate
