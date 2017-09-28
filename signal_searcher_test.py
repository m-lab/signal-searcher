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

# pylint: disable=missing-docstring

import datetime
import unittest

import netaddr
import signal_searcher


class SignalSearcherTestCase(unittest.TestCase):

    def test_parse_date_on_good_data(self):
        parsed = signal_searcher.parse_date('12 April 2015')
        date = datetime.datetime(year=2015, month=4, day=12)
        self.assertEqual(parsed, date)

    def test_parse_date_throws_on_error(self):
        saw_error = False
        try:
            parsed = signal_searcher.parse_date('12 Monkeys')
            self.fail('this line should never execute: ' + str(parsed))
        except ValueError:
            saw_error = True
        self.assertTrue(saw_error)

    def test_argument_parsing(self):
        args = signal_searcher.parse_command_line([
            '--start="January 2, 2015"',
            '--end',
            '2015-1-3',
        ])
        self.assertEqual(args.start, datetime.datetime(year=2015, month=1, day=2))
        self.assertEqual(args.end, datetime.datetime(year=2015, month=1, day=3))
