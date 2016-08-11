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
from math import pi
from math import sin
import random
import unittest

import cyclic
import mlabreader
import netaddr


def flat_dataset(start, n):
  return [mlabreader.MlabDataEntry(
      start + datetime.timedelta(hours=i), 1000, 10000, 100) for i in range(n)]


def cyclic_dataset(start, n):
  return [mlabreader.MlabDataEntry(
      start + datetime.timedelta(hours=i),
      1000 + 1000 * sin(float(i) / 24.0 * 2 * pi),
      10000 + 10000 * sin(float(i + 8) / 24.0 * 2 * pi),
      100 + 100 * sin(float(i + 16) / 24.0 * 2 * pi)) for i in range(n)]


def slightly_cyclic_noisy_dataset(start, n):
  return [mlabreader.MlabDataEntry(
      start + datetime.timedelta(hours=i),
      1000 + random.random() * 10,
      10000 + 10000 * sin(float(i + 8) / 24.0 * 2 * pi) + 10 * random.random(),
      100 + random.random()) for i in range(n)]


class TestCyclicProblemFinder(unittest.TestCase):

  def setUp(self):
    self.start_time = datetime.datetime(2016, 1, 1, 0, 0)

  def test_flat_has_no_problems(self):
    # Flat data should have no pattern
    flat_data = {
        netaddr.IPNetwork('10.0.0.0/8'): flat_dataset(self.start_time, 2000)
    }
    self.assertEqual(len(cyclic.find_problems(flat_data)), 0)

  def test_cyclic_data_has_many_problems(self):
    # Should have one report for each attribute
    cyclic_data = {
        netaddr.IPNetwork('1.1.0.0/16'): cyclic_dataset(self.start_time, 2000)
    }
    self.assertEqual(len(cyclic.find_problems(cyclic_data)), 3)

  def test_short_cyclic_data_has_many_problems(self):
    # Should have one report for each attribute
    short_cyclic_data = {
        netaddr.IPNetwork('5.1.0.0/16'): cyclic_dataset(self.start_time, 100)
    }
    self.assertEqual(len(cyclic.find_problems(short_cyclic_data)), 3)

  def test_partially_cyclic_has_one_problem(self):
    # Should have one report, because just one attribute has a 24 hour pattern
    partially_cyclic_data = {
        netaddr.IPNetwork('1.2.0.0/16'): slightly_cyclic_noisy_dataset(
            self.start_time, 2000)
    }
    self.assertEqual(len(cyclic.find_problems(partially_cyclic_data)), 1)

  def test_too_short_data_never_has_problems(self):
    # Should have no reports, despite the cycles, because it is too short.
    cyclic_data = {
        netaddr.IPNetwork('1.1.0.0/16'): cyclic_dataset(self.start_time, 23)
    }
    self.assertEqual(len(cyclic.find_problems(cyclic_data)), 0)


if __name__ == '__main__':
  unittest.main()
