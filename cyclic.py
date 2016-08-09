#!/usr/bin/env pytho
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

import report
import math
import numpy


def netblock_size(block):
  return 1 + (block.last - block.first)

# A tunable parameter to help adjust the sensitivity of this test. As the
# required SPIKE_RATIO grows, the test becomes less vulnerable to noise, but may
# miss some signals. Adjust this parameter up or down experimentally.
SPIKE_RATIO = 2.0

# A tunable parameter to indicate the lowest value that we might want to
# consider as a valid signal. Power below this value can't count as a signal,
# no matter how low the average might be. Adjust this parameter experimentally.
SQUELCH_THRESHOLD = .001


def power_spike_at_24_hours(timeseries):
  """ Looks for the presence of a 24-hour cycles in the timeseries.

  Looks to see how much power there is under the frequency corresponding to
  24 hours. A relatively large amount of power there would mean that the
  timeseries has a 24-hour component signal. 
  
  Args:
      timeseries: A list with one datapoint per hour, in order.

  Returns:
      True if a strong 24-hour signal was found, False otherwise
  """
  if len(timeseries) < 24:
    return False

  # Perform an FFT
  spectrum = numpy.fft.rfft(timeseries)
  # Square the result to get the power curve
  power = [abs(x)**2 for x in spectrum]
  # Compare the value at the index corresponding to 24 hours to the average
  # value. If it is more than SPIKE_SIZE times the average value, then we will
  # count that as a a spike. We calculate the average after removing the DC
  # component of the signal.
  avg = numpy.average(power[1:])
  # Frequency at index N is N/len(array).  We want to find the power when the
  # frequency is 1/24 (one event per 24 hours, aka "daily"), which means we
  # need to look at index:
  #   N/len(array) = 1/24
  #           24*N = len(array)
  #              N = len(array) / 24
  day_index = int(len(timeseries) / 24.0)
  return ((power[day_index] > SQUELCH_THRESHOLD) and
          (power[day_index] / avg > SPIKE_RATIO))


def find_problems(timeseries):
  message_template = """%s are fluctuating on a 24-hour cycle. This is frequently a leading indicator of congestion, and suggests that at least part of the network in question is underprovisioned for its peak load"""

  problems_found = []
  for netblock in timeseries:
    # The size of a problem is equal to the number of IP addresses affected
    # times the time range over which that effect happened.  This way a severe
    # problem for a few IP addresses that happened for a long time can be
    # quantitatively compared with a mild problem over a lot of IP address for
    # a shorter time.
    problem_size = netblock_size(netblock) * (len(timeseries) / 24.0)
    upload = [d.upload_speed for d in timeseries[netblock]]
    if power_spike_at_24_hours(upload):
      problems_found.append(
          report.Report("Cyclic performance finder", netblock, 4, problem_size,
                        message_template % "Upload speeds"))
    download = [d.download_speed for d in timeseries[netblock]]
    if power_spike_at_24_hours(download):
      problems_found.append(
          report.Report("Cyclic performance finder", netblock, 4, problem_size,
                        message_template % "Download speeds"))
    rtt = [d.min_latency for d in timeseries[netblock]]
    if power_spike_at_24_hours(rtt):
      problems_found.append(
          report.Report("Cyclic performance finder", netblock, 4, problem_size,
                        message_template % "Round-trip times"))

  return problems_found
