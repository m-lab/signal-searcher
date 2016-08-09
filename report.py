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

class Report(object):
  def __init__(self, source, netblock, severity, volume_affected, text):
    """ Create a new report of an Internet performance problem.

    Args:
        source: The module reporting the problem
        netblock: The netblock where the problem occurred
        severity: The problem severity, on a scale from 0 (bad) to 10 (okay)
        volume_affected: How many ips were affected * number of days
        text: A description of the issue
    """
    self._source = source
    self._netblock = netblock
    self._severity = severity
    self._volume_affected = volume_affected
    self._text = text
