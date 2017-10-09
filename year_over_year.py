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
"""Finds instances where the year-over-year performance fell.

The find_problems function discovers year-long (or more!) in a datastream
dropped by more than a threshold amount.
"""

import collections
import logging

import numpy 

from mlabdata import Problem


IntermediateProblem = collections.namedtuple(
    'IntermediateProblem', ['start', 'end', 'metric', 'severity'])


ONEYEAR = 365
TWOYEARS = 2 * ONEYEAR

def _find_year_over_year_problems(single_metric_series, metric):
    previous_year = sum(single_metric_series[:ONEYEAR])
    current_year = sum(single_metric_series[ONEYEAR:TWOYEARS])
    for i in range(TWOYEARS, len(single_metric_series)):
        if float(current_year) / float(previous_year) < .75:
            severity = float(current_year) / float(previous_year)
            yield IntermediateProblem(i - TWOYEARS, i, metric, severity)
        assert previous_year > 0
        assert current_year > 0
        previous_year -= single_metric_series[i - TWOYEARS]
        previous_year += single_metric_series[i - ONEYEAR]
        current_year -= single_metric_series[i - ONEYEAR]
        current_year += single_metric_series[i]


def _analyze_stream(key, data, analysis_method):
    if not data:
        logging.info('No data')
        return []
    problems_found = []
    for metric, _metric_name in [
        ('download', 'Download speeds'),
        #('upload', 'Upload speeds'),
        #('rtt', 'Round-trip times'),
    ]:
        field_index = data[0]._fields.index(metric)
        assert field_index >= 0, 'Bad metric name %s' % metric
        single_metric_series = [d[field_index] for d in data]
        problems_found.extend(analysis_method(single_metric_series, metric))

    if not problems_found:
        logging.info('No problems found')
        return []

    coalesced_problems = []
    current_problems = [problems_found[0]]
    for problem in problems_found[1:]:
        if problem.start <= current_problems[-1].end and \
                current_problems[-1].metric == problem.metric:
            current_problems.append(problem)
        else:
            coalesced_problems.append(current_problems)
            current_problems = [problem]
    coalesced_problems.append(current_problems)

    actual_problems = []
    for problem_list in coalesced_problems:
        start = problem_list[0].start
        end = problem_list[-1].end
        count = sum(x.samples for x in data[start:end])
        severity = sum(x.severity
                       for x in problem_list) / float(len(problem_list))
        actual_problems.append(
            Problem(key.strip(), data[start].table, data[start].time, data[end]
                    .time, severity, count, 'stuff dropped'))
    actual_problems = list(
        sorted(
            actual_problems, key=lambda x: x.severity * x.test_count))
    return actual_problems


def find_problems(key, data):
    return _analyze_stream(key, data, _find_year_over_year_problems)
    
