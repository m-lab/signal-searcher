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

import cyclic
import dateparser
import mlabreader
import netaddr
import report


def parse_date(s):
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
  d = dateparser.parse(s)
  if d is None:
    raise ValueError("can't parse %s into a date" % s)
  else:
    return d


def parse_command_line(cli_args=None):
  """Parses command-line arguments.

  Prints help and exits if the user asks for that, and prints an error message
  and exits if the command-line arguments were bad in some way.  Otherwise,
  returns a tuple of the values of the parsed arguments.

  Args:
      cli_args: Optional array of strings to parse. Uses sys.argv by default.

  Returns:
      A tuple of (start_time, end_time, netblocks)
  """
  if cli_args is None:
    cli_args = sys.argv[1:]

  # Parse the command line
  parser = argparse.ArgumentParser(
      description='Analyze mLab data to find interesting and important signals')
  parser.add_argument(
      'netblocks',
      metavar='NETBLOCK',
      type=netaddr.IPNetwork,
      nargs='+',
      help='netblock(s) of interest for signal searcher')
  parser.add_argument(
      '--start',
      default=[datetime.datetime(datetime.datetime.now().year, 1, 1, 0, 0)],
      metavar='DATETIME',
      type=parse_date,
      nargs=1,
      help='The beginning of the time period to search '
      '(defaults to the beginning of the current year)')
  parser.add_argument(
      '--end',
      default=[datetime.datetime.now()],
      metavar='DATETIME',
      type=parse_date,
      nargs=1,
      help='The end of the time period to search '
      '(defaults to the current time)')
  try:
    args = parser.parse_args(cli_args)
  except (netaddr.core.AddrFormatError, ValueError) as e:
    parser.error(e.message)
  return (args.start[0], args.end[0], args.netblocks)


def main():
  # Parse the command-line
  start, end, netblocks = parse_command_line()

  # Read the data
  timeseries = mlabreader.read_timeseries(netblocks, start, end)

  # Look for problems
  cycle_problems = cyclic.find_problems(timeseries)

  # Compile a report about the problems
  final_report = report.Report(cycle_problems)
  print final_report.cli_report()


if __name__ == '__main__':
  main()
