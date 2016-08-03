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

import collections
import datetime

MlabDataEntry = collections.namedtuple(
    'MlabDataEntry', ['time', 'upload_speed', 'download_speed', 'min_latency'])


def read_timeseries(netblocks, start_time, end_time):
  """ Reads timeseries data from MLab's BiqQuery data. """
  data = {}
  for block in netblocks:
    data[block] = []
    t = datetime.datetime(start_time.year, start_time.month, start_time.day,
                          start_time.hour, 0)
    while t < end_time:
      data[block].append(
          MlabDataEntry(
              time=t, upload_speed=5, download_speed=10, min_latency=100))
      t += datetime.timedelta(hours=1)
  return data
