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
"""Look for evidence of performance fluctuating on a 24-hour cycle.

This module looks for evidence of daily periodicity in a timeseries. When a
network has performance that varies diurnally, then that is a strong suggestion
that the network is near overload, as network usage varies diurnally for most
eyeball networks.  In the MLab report on interconnections, it was found that
performance varying on a 24-hour basis was a leading indicator of severe
congestion problems.
"""

import datetime
import numpy
import report

# A tunable parameter to help adjust the sensitivity of this test. As the
# required SPIKE_RATIO grows, the test becomes less vulnerable to noise, but may
# miss some signals. Adjust this parameter up or down experimentally.
SPIKE_RATIO = 2.0

# A tunable parameter to indicate the lowest value that we might want to
# consider as a valid signal. Power below this value can't count as a signal,
# no matter how low the average might be. Adjust this parameter experimentally.
SQUELCH_THRESHOLD = .001


def _power_spike_at_24_hours(timeseries):
  """Looks for the presence of a 24-hour cycles in the timeseries.

  Looks to see how much power there is under the frequency corresponding to
  24 hours. A relatively large amount of power there would mean that the
  timeseries has a 24-hour component signal.

  Args:
      timeseries: The timeseries in question, one datapoint per hour, in order

  Returns:
      Whether or not a strong 24-hour signal was found
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
  """Discover any problems relating to cyclic patterns in the data.

  Args:
      timeseries: the data read from MLab BigQuery

  Returns:
      a list of CycleProblems, possibly empty
  """
  problems_found = []
  for netblock, series in timeseries.iteritems():
    duration = datetime.timedelta(hours=len(series))
    for metric, metric_name in [('upload_speed', 'Upload speeds'),
                                ('download_speed', 'Download speeds'),
                                ('min_latency', 'Round-trip times')]:
      field_index = series[0]._fields.index(metric)
      assert field_index >= 0, 'Bad metric name %s' % metric
      single_metric_series = [d[field_index] for d in series]
      if _power_spike_at_24_hours(single_metric_series):
        problems_found.append(CycleProblem(netblock, duration, metric_name))

  return problems_found


class CycleProblem(report.Problem):
  """A report for the discovery of cyclic performance characteristics."""
  priority = 4  # all CycleProblems have the same priority for now

  def __init__(self, netblock, duration, metric):
    message = ('%s are fluctuating on a 24-hour cycle. This is frequently a '
               'leading indicator of congestion, and suggests that at least '
               'part of the network in question is underprovisioned for its '
               'peak load') % metric
    super(CycleProblem, self).__init__(netblock, duration,
                                       CycleProblem.priority, message)
