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

import datetime
import unittest

import mock

import btreader


class BtReaderTest(unittest.TestCase):
    def test_parse_key(self):
        key, date = btreader._parse_key('AS12 | nausny | 2017-08-09')
        self.assertEqual(key, 'AS12 | nausny ')
        self.assertEqual(date, datetime.date(2017, 8, 9))


if __name__ == '__main__':
    unittest.main()
