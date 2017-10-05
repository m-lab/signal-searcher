#!/usr/bin/env python
# Copyright 2017 The MLab Signal Searcher Authors
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


"""Reads data from the MLab Cloud Bigtables."""


import collections
import datetime
import logging
import struct

import grpc

# pylint: disable=no-name-in-module
from google.cloud import bigtable
# pylint: enable=no-name-in-module


InternetData = collections.namedtuple(
    'InternetData',
    ['key', 'table', 'time', 'upload', 'download', 'rtt', 'samples'])


def read_timeseries(table_name, _start=None, _end=None):
    #Valid table names:
    assert table_name in ['client_asn_by_day',
                          'client_asn_client_loc_by_day',
                          'client_loc_by_day',
                          'server_asn_by_day',
                          'server_asn_client_asn_by_day',
                          'server_asn_client_asn_client_loc_by_day',
                          'server_asn_client_loc_by_day']
    response = _connect_to_table(table_name)
    data = []
    key_no_date = None
    try:
        while True:
            try:
                response.consume_next()
            except grpc.RpcError as err:
                logging.warning('Could not consume_next: %s, assuming error '
                                'was transient and reconnecting.', err)
                response = _connect_to_table(table_name, key_no_date)
                data = []
                key_no_date = None
                continue
            for key in sorted(response.rows):
                row = response.rows[key].to_dict()
                new_key, entry = _parse_key_and_data(key, row, table_name)
                if new_key is None or entry is None:
                    continue
                if new_key != key_no_date:
                    if key_no_date and data:
                        yield key_no_date, data
                    key_no_date = new_key
                    data = []
                data.append(entry)

            response.rows.clear()
    except StopIteration:
        pass
    if key_no_date:
        yield key_no_date, data


def _connect_to_table(name, start_key=None):  # pragma: no cover
    client = bigtable.Client(project='mlab-oti', admin=False)
    # TODO: verify that the permissions on this table are correct
    instance = client.instance('mlab-data-viz-prod')
    table = instance.table(name)
    return table.read_rows(start_key=start_key)


def _parse_key(key):
    if key.count('|') < 1:
        logging.warning('bad key "%s"', key)
        return None, None
    key_no_date, date = key.rsplit('|', 1)
    if date.count('-') != 2:
        logging.warning('bad date in key "%s"', key)
        return None, None
    year, month, day = date.split('-')
    try:
        date = datetime.date(int(year, 10), int(month, 10), int(day, 10))
    except ValueError as verr:
        logging.warning('ValueError: %s (%s)', verr, key)
        return None, None
    return key_no_date, date


def _parse_key_and_data(key, data, table_name):
    key_no_date, date = _parse_key(key)
    if not key_no_date or not date:
        return None, None

    for required in ['data:download_speed_mbps_median',
                     'data:upload_speed_mbps_median', 'data:rtt_avg',
                     'data:count']:
        if required not in data:
            logging.debug('no %s entry for %s', required, key)
            return None, None
    download = data['data:download_speed_mbps_median'][0].value
    download = struct.unpack('>d', download)[0]
    upload = data['data:upload_speed_mbps_median'][0].value
    upload = struct.unpack('>d', upload)[0]
    rtt = data['data:rtt_avg'][0].value
    rtt = struct.unpack('>d', rtt)[0]
    samples = int(data['data:count'][0].value)
    return key_no_date, InternetData(key=key_no_date, table=table_name,
                                     time=date, download=download,
                                     upload=upload, rtt=rtt, samples=samples)
