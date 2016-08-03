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

from math import sin, pi

import cyclic
import datetime
import mlabreader
import netaddr
import unittest


def create_flat_dataset(start, n):
  return [mlabreader.MlabDataEntry(
      start + datetime.timedelta(hours=i), 1000, 10000, 100) for i in range(n)]


def create_cyclic_dataset(start, n):
  return [mlabreader.MlabDataEntry(
      start + datetime.timedelta(hours=i),
      1000 + 1000 * sin(float(i) / 24.0 * 2 * pi),
      10000 + 10000 * sin(float(i + 8) / 24.0 * 2 * pi),
      100 + 100 * sin(float(i + 16) / 24.0 * 2 * pi)) for i in range(n)]


class TestCyclicProblemFinder(unittest.TestCase):

  def setUp(self):
    start_time = datetime.datetime(2016, 1, 1, 0, 0)
    self.flat_data = {
        netaddr.IPNetwork('10.0.0.0/8'): create_flat_dataset(start_time, 2000)
    }
    self.cyclic_data = {
        netaddr.IPNetwork('1.1.0.0/16'): create_cyclic_dataset(start_time, 2000)
    }
    self.short_cyclic_data = {
        netaddr.IPNetwork('5.1.0.0/16'): create_cyclic_dataset(start_time, 100)
    }

  def test_size_makes_sense(self):
    self.assertEqual(cyclic.netblock_size(netaddr.IPNetwork('10.0.0.0/32')), 1)
    self.assertEqual(
        cyclic.netblock_size(netaddr.IPNetwork('10.0.0.0/24')), 256)

  def test_flat_has_no_problems(self):
    self.assertEqual(len(cyclic.find_problems(self.flat_data)), 0)

  def test_cyclic_data_has_many_problems(self):
    self.assertNotEqual(len(cyclic.find_problems(self.cyclic_data)), 0)

  def test_short_cyclic_data_has_many_problems(self):
    self.assertNotEqual(len(cyclic.find_problems(self.short_cyclic_data)), 0)


if __name__ == '__main__':
  unittest.main()