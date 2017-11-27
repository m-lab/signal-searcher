#!/usr/bin/env python
# Copyright 2016 The MLab Signal Searcher Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Looks for problems in a specified subset of Measurement Lab (MLab) data.

Signal Searcher is designed to comb through Internet performance data looking
for systemic problems. It then creates a prioritized report of all the problems
it finds.

For more information, try:
  python signal_searcher.py --help
"""

import argparse
import datetime
import sys

# Google cloud libraries are organized in a way that confuses the linter.
# pylint: disable=no-name-in-module
from google.cloud import bigquery
# pylint: enable=no-name-in-module

import cyclic
import dateparser
import btreader
import year_over_year


def parse_date(string):
    """Parses a date from a string or throws an exception.

    ArgumentParser is built expecting the custom argument parsers to throw
    exceptions when the parse fails, so we adapt dateparser to conform to that
    convention.
  
    Args:
        s: a string to parse into a date
  
    Returns:
        A datetime.datetime object
  
    Raises:
        ValueError: on unparseable input
    """
    date = dateparser.parse(string)
    if date is None:
        raise ValueError("can't parse %s into a date" % string)
    else:
        return date


def parse_command_line(cli_args):
    """Parses command-line arguments.

    Prints help and exits if the user asks for that, and prints an error message
    and exits if the command-line arguments were bad in some way.  Otherwise,
    returns a tuple of the values of the parsed arguments.
  
    Args:
        cli_args: Optional array of strings to parse. Uses sys.argv by default.
  
    Returns:
        A dictionary of the parsed args
    """
    # Parse the command line
    parser = argparse.ArgumentParser(
        description='Analyze mLab data to find interesting and important '
                    'signals'
    )
    parser.add_argument(
        '--start',
        default=datetime.datetime(2009, 6, 6, 0, 0, 0),
        metavar='DATETIME',
        type=parse_date,
        help='The beginning of the time period to search '
        '(defaults to the beginning of the MLab data set)')
    parser.add_argument(
        '--end',
        default=datetime.datetime.now(),
        metavar='DATETIME',
        type=parse_date,
        help='The end of the time period to search '
        '(defaults to the current time)')
    parser.add_argument(
        '--credentials',
        default='',
        metavar='PATH',
        help='The path to local Google Cloud credentials (defaults to blank)')
    parser.add_argument(
        '--bigtable',
        metavar='TABLE_NAME',
        default='client_asn_client_loc_by_day',
        help='The bigtable name to look for the data'
        '(defaults to client_asn_client_loc_by_day)')
    parser.add_argument(
        '--bigquery',
        metavar='TABLE_NAME',
        default=None,
        help='The name of the biqguery table in which to store problems.'
        '(defaults None, indicating no saving is desired)')
    try:
        args = parser.parse_args(cli_args)
    except ValueError as error:
        parser.error(error.message)
    return args


def main(argv):  # pragma: no-cover
    # Parse the command-line
    args = parse_command_line(argv[1:])

    # Read the data
    problems = []
    for key, timeseries in btreader.read_timeseries(args.bigtable, args.start,
                                                    args.end):
        # Look for problems
        problems_found = year_over_year.find_problems(key, timeseries)
        if problems_found:
            problems.extend(problems_found)
            # Print each problem as it is discovered
            for problem in problems_found:
                print problem

    # Once all the problems have been discovered, store them in the requested manner.
    if args.bigquery:
        pass


if __name__ == '__main__':  # pragma: no-cover
    main(sys.argv)
