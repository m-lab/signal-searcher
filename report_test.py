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

# pylint: disable=protected-access, missing-docstring

import datetime
import unittest
import netaddr
import report


class ReportTests(unittest.TestCase):
    def setUp(self):
        self.empty_report = report.Report()
        self.problem = report.Problem(
            netaddr.IPNetwork('10.0.0.0/8'),
            datetime.timedelta(hours=48),
            9,
            'not really a problem')
        self.single_report = report.Report([self.problem])

    def test_size_makes_sense(self):
        self.assertEqual(
            report._netblock_size(netaddr.IPNetwork('10.0.0.0/32')), 1)
        self.assertEqual(
            report._netblock_size(netaddr.IPNetwork('10.0.0.0/24')), 256)

    def test_str(self):
        self.assertEqual(
            str(self.problem),
            'Problem with 10.0.0.0/8 lasting for 2 days, 0:00:00 (severity 9): '
            'not really a problem')

    def test_cli_on_empty_report(self):
        self.assertEqual(self.empty_report.cli_report(), 'No problems found.')

    def test_cli_on_single_report(self):
        self.assertEqual(len(self.single_report.cli_report().splitlines()), 2)


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
