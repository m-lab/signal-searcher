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
"""Problem descriptions and the compilation of Problems into Reports.

This module has two classes of interest to exporters: Problem and Report. Each
Problem is a specific instance of a performance problem observed in the
specified data, and each Report is a compilation of many problems.  The
Problems are in charge of keeping track of their own descriptions, and the
Report is in charge of prioritization of Problems to make the overall
explanation make sense.

Every individual test for problems should likely implement their own subclass
of report.Problem, but it is unlikely that an individual test for a problem
would need to implement its own Report.
"""


def _netblock_size(block):
  """A helper method that should exist as part of the IPNetwork class."""
  return 1 + (block.last - block.first)


class Problem(object):
  """Holds a single instance of a discovered problem.

  Designed to be extended by its children to hold specific kinds of problems.
  """

  def __init__(self, netblock, duration, severity, text):
    """Create a new report of a single Internet performance problem.

    Args:
        netblock: The netblock where the problem occurred
        duration: How long the problem went on
        severity: The problem severity, on a scale from 0 (bad) to 10 (okay)
        text: A description of the issue
    """
    self._netblock = netblock
    self._duration = duration
    self._severity = severity
    self._text = text

  def __str__(self):
    return "Problem with %s lasting for %s (severity %d): %s" % (
        str(self._netblock), str(self._duration), self._severity, self._text)


class Report(object):
  """The compilation of many problems, turned into an explanation."""

  def __init__(self, *problems):
    self._problems = []
    for problem_list in problems:
      self._problems.extend(problem_list)

  def cli_report(self):
    if not self._problems:
      return "No problems found."
    else:
      return "Problems found:\n" + "\n".join("  " + str(x)
                                             for x in self._problems)
