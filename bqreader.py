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

import mlabdata
import bigquery_talk
import query


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
        { IPNetwork('1.2.0.0/16'): MlabDataEntry }
  """
  data = {}
  for block in netblocks:
    data[block] = []
    t = datetime.datetime(start_time.year, start_time.month, start_time.day,
                          start_time.hour, 0)

    upload_data = _get_data(block, 'upload', start_time, end_time, credentials)
    download_data = _get_data(block, 'download', start_time, end_time, credentials)
    latency_data = _get_data(block, 'minimum_rtt', start_time, end_time, credentials)

    length = _fix_data_length(upload_data[1], download_data[1], latency_data[1])

    for hour in range(0, length):
        data[block].append(mlabdata.MlabDataEntry(
                time=upload_data[0],
                upload_speed=upload_data[1][hour],
                download_speed=download_data[1][hour],
                min_latency=latency_data[1][hour]))

  return data
