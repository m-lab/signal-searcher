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

import datetime
import unittest

import degradation
import netaddr


class TestDegradationProblemFinder(unittest.TestCase):


  def test_bad_slope(self):
    self.assertTrue(degradation._bad_slope(1, 'min_latency'))
    self.assertFalse(degradation._bad_slope(-1, 'min_latency'))
    self.assertTrue(degradation._bad_slope(-1, 'download_speed'))
    self.assertFalse(degradation._bad_slope(1, 'download_speed'))
    self.assertTrue(degradation._bad_slope(-1, 'upload_speed'))
    self.assertFalse(degradation._bad_slope(1, 'upload_speed'))
    with self.assertRaises(ValueError):
      degradation._bad_slope(1, 'invalid_metric')


  def test_find_slope(self):
    a = [i + 100 for i in range(730)]
    self.assertAlmostEqual(degradation._find_slope(a), 1)


  def test_find_problems_finds_bad_last_week(self):
    # A good year
    performance = [i for i in range(365 * 24)]
    # But the slope is negative for the last week
    hours_per_week = 24 * 7
    performance[len(performance) - hours_per_week:] = reversed(performance[len(
        performance) - hours_per_week:])
    problems = list(
        degradation._performance_degradation(
            netaddr.IPNetwork('10.0.0.0/8'),
            performance,
            datetime.timedelta(days=365),
            'download_speed',
            'Download Speed'))
    self.assertEqual(len(problems), 1)
    self.assertEqual(problems[0]._duration, datetime.timedelta(days=7))


  def test_find_problems_finds_bad_last_month(self):
    # A good year
    performance = [i for i in range(365 * 24)]
    # But the slope is negative for the last month
    hours_per_month = 24 * 31
    performance[len(performance) - hours_per_month:] = reversed(performance[len(
        performance) - hours_per_month:])
    problems = list(
        degradation._performance_degradation(
            netaddr.IPNetwork('10.0.0.0/8'),
            performance,
            datetime.timedelta(days=365),
            'download_speed',
            'Download Speed'))
    self.assertEqual(len(problems), 1)
    self.assertTrue(problems[0]._duration >= datetime.timedelta(days=30))
    self.assertTrue(problems[0]._duration <= datetime.timedelta(days=31))


  def test_find_problems_finds_bad_last_week_of_latency(self):
    # A good year
    performance = list(reversed([i for i in range(365 * 24)]))
    # But the slope is positive for the last week
    hours_per_week = 24 * 7
    performance[len(performance) - hours_per_week:] = reversed(performance[len(
        performance) - hours_per_week:])
    problems = list(
        degradation._performance_degradation(
            netaddr.IPNetwork('10.0.0.0/8'),
            performance,
            datetime.timedelta(days=365),
            'min_latency',
            'Latency'))
    self.assertEqual(len(problems), 1)
    self.assertEqual(problems[0]._duration, datetime.timedelta(days=7))


if __name__ == '__main__':
  unittest.main()
