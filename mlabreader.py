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

"""Reads data from the MLab BigQuery instance.

If you run this, we recommend having the gmail address associated with your
query be a member of the group discuss@measurementlab.net, because then the
resulting query charges will go to MLab instead of you.
"""

import collections
import datetime

import bigquery_talk
import query

import netaddr
import pytz


import datetime
from math import pi
from math import sin
import random
import unittest

MlabDataEntry = collections.namedtuple(
    'MlabDataEntry', ['time', 'upload_speed', 'download_speed', 'min_latency'])

def _get_bigquery_call_object(credentials_path, headless=False):
  # get authenticated service object for talking to BigQuery
  auth_handler = bigquery_talk.GoogleAPIAuthHandler(credentials_path, headless)
  auth_service = auth_handler.get_authenticated_google_service()
  return bigquery_talk.BigQueryCall(auth_service, 'mlab-oti')

def _get_data(netblock, metric, start_time, end_time, credentials):
  query_string = query.build_metric_median_query(metric, start_time, end_time, netblock)
  query_object = _get_bigquery_call_object(credentials)
  resp_ticket = query_object.run_asynchronous_query(query_string)
  response = resp_ticket.wait_for_query_results()
  return response

def _fix_data_length(upload, download, latency):
  """ Makes the three data sources the same length in case of missing data.

  Adds None to the end of lists that are missing data.

  Args:
    upload: List of upload values.
    download: List of download values.
    latency: List of latency values.

  Returns:
    Integer length of all the data.
  """
  length = max(len(upload), len(download), len(latency))
  for data in [upload, download, latency]:
    while len(data) < length:
      data.append(None)

  return length

def read_timeseries(netblocks, start_time, end_time, credentials):
  """Reads timeseries data from MLab's BiqQuery data.

  Args:
    netblocks: List of netaddr.IPNetwork instances
    start_time: datetime.datetime instance
    end_time: datetime.dateime instance
    credentials: path to Google cloud credentials

  Returns:
    Data in a dictionary in the following form:
      IPNetwork('10.2.0.0/16'): [], IPNetwork('1.2.0.0/16'): [MlabDataEntry(time=0,


  """
  data = {}
  for block in netblocks:
    data[block] = []
    t = datetime.datetime(start_time.year, start_time.month, start_time.day,
                          start_time.hour, 0)

    upload_data = _get_data(block, 'upload', start_time, end_time, credentials)
    download_data = _get_data(block, 'download', start_time, end_time, credentials)
    latency_data = _get_data(block, 'minimum_rtt', start_time, end_time, credentials)

    length = _fix_data_length(upload_data, download_data, latency_data)

    for hour in range(0, length):
        data[block].append(MlabDataEntry(
                time=hour,
                upload_speed=upload_data[hour],
                download_speed=download_data[hour],
                min_latency=latency_data[hour]))

  return data

def main():

  netblocks = [netaddr.IPNetwork('1.2.0.0/16'), netaddr.IPNetwork('10.2.0.0/16')]
  start_time = datetime.datetime(2014, 1, 1, tzinfo=pytz.utc)
  end_time = datetime.datetime(2014, 2, 1, tzinfo=pytz.utc)

  data = read_timeseries(netblocks, start_time, end_time, "/Users/LavalleF/.config/gcloud/credentials")

  print "finished getting data"
  print data



if __name__ == '__main__':
  main()
