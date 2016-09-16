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
"""Look for evidence of performance degradation over a long period of time.

This module takes a timeseries attempts to find subseries within that series
that represent periods of time when the underlying network performance got
worse instead of better.  It's a pretty crude method, and only finds problems
when they have been problems for a while, but it produces charts and graphs
that are the easiest to understand.
"""

import datetime
import numpy
import report


def _find_slope(timeseries):
  """Slope of the fit of the timeseries to a linear data model."""
  # Fit the data to a line (a.k.a. a polynomial of degree 1)
  slope, _ = numpy.polyfit(list(range(len(timeseries))), timeseries, 1)
  return slope


def _bad_slope(slope, metric):
  """Returns True if the slope indicates a degradation for the metric."""
  if metric == 'min_latency':
    return slope > 0  # Latency climbing over time is bad
  elif metric in ('download_speed', 'upload_speed'):
    return slope < 0  # Declining throughput over time is bad
  else:
    raise ValueError('Unknown metric: ' + metric)


def _recursive_performance_degradation(netblock, timeseries, metric,
                                       metric_name, time_periods, start, end):
  """Recursively look for performance problems at finer and finer timescales.

  Rather than chopping up the original timeseries, and recursing on that, we
  recurse on indices into the original. This reduces our memory usage, but more
  importantly it removes the implicit requirement that each subsequent time
  period divide cleanly into the previous one.

  Args:
    netblock: the netblock being investigated
    timeseries: the hour-by-hour performance of that netblock
    metric: the name of the metric of interest
    metric_name: a human readable version of that name
    time_periods: a list of the timescales at which to examine the data
    start: the index at which the time period of interest starts
    end: the index at which the time period of interest ends

  Yields:
    zero or more PerformanceDegradationProblems
  """
  # Base case
  if not time_periods:
    return
  # Recursive case
  time_periods = time_periods[:]
  period_size = time_periods.pop()
  period_index = 0
  # For each chunk, starting from the most recent one, either yield the
  # existence of a problem or recursively look for problems at a finer
  # timescale.
  while period_index * period_size < end - start:
    chunk_start = max(end - ((period_index + 1) * period_size), 0)
    chunk_end = end - (period_index * period_size)
    chunk_duration = datetime.timedelta(hours=(chunk_end - chunk_start))
    # Don't investigate performance degradation at a timescale finer than a
    # day. Too much noise and too many transient network conditions.
    if chunk_duration.days > 1:
      chunk = timeseries[chunk_start:chunk_end]
      slope = _find_slope(chunk)
      if _bad_slope(slope, metric):
        yield PerformanceDegradationProblem(netblock, chunk_duration, metric_name)
      else:
        for problem in _recursive_performance_degradation(netblock, timeseries,
                                                          metric, metric_name,
                                                          time_periods,
                                                          chunk_start, chunk_end):
          yield problem
    period_index += 1


def _performance_degradation(netblock, timeseries, duration, metric,
                             metric_name):
  """Search for sustained performance degradations in our performance data.

  We look for these on three timescales: yearly, monthly, and weekly. This code
  returns PerformanceDegradationProblems, but it doesn't do needless work. In
  particular, if it finds a problem on a large timescale, then it will not look
  for the same problems on a smaller timescale.

  Args:
      netblock: the netblock in question
      timeseries: the sequence of measured values
      duration: the timedelta this series covers
      metric: the metric of interest
      metric_name: a human-readable name for the metric of interest

  Yields:
      all the PerformanceDegradationProblems it finds
  """
  # Defense against too-small datasets that can yield only noise
  if duration < datetime.timedelta(days=1):
    return
  # Fit the data to a line, and then see if the line's slope is good or bad.
  slope = _find_slope(timeseries)
  if _bad_slope(slope, metric):
    yield PerformanceDegradationProblem(netblock, duration, metric_name)
  else:
    # If there's no overall problem, look for yearly, monthly, and weekly
    # problems, in that order.
    hours_per_year = 365 * 24
    hours_per_month = hours_per_year // 12
    hours_per_week = 7 * 24

    for problem in _recursive_performance_degradation(
        netblock, timeseries, metric, metric_name,
        [hours_per_week, hours_per_month, hours_per_year], 0, len(timeseries)):
      yield problem


def find_problems(timeseries):
  """Discover any sustained performance degradation problems.

  Args:
      timeseries: the data read from MLab BigQuery

  Returns:
      a list of PerformanceDegradationProblems, possibly empty
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
      problems_found.extend(
          list(_performance_degradation(netblock, single_metric_series, duration,
                                   metric, metric_name)))
  return problems_found


class PerformanceDegradationProblem(report.Problem):
  """A report for the display of performance degradation problems."""

  def __init__(self, netblock, duration, metric):
    if duration >= datetime.timedelta(days=364):
      priority = 1
    elif duration >= datetime.timedelta(days=28):
      priority = 3
    else:
      priority = 5
    message = ('{metric} saw degraded performance from its historical values. '
               'There was a performance degradation sustained over a period of '
               '{duration}. This suggests that something has broken, or '
               'that the conditions of the network have changed enough that '
               'the current design no longer works to effectively serve '
               'traffic.').format(
                   metric=metric, duration=duration)
    super(PerformanceDegradationProblem, self).__init__(netblock, duration,
                                                        priority, message)
