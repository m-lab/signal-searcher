
#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Copyright 2016 Measurement Lab
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
import logging
import pytz

METRICS = ['download', 'upload', 'minimum_rtt']


def unix_timestamp_to_utc_datetime(unix_timestamp):
    return datetime.datetime.fromtimestamp(unix_timestamp, tz=pytz.utc)

def _seconds_to_microseconds(seconds):
    return seconds * 1000000

def _is_server_to_client_metric(metric):
    return metric in ('download', 'minimum_rtt')

class QueryConditionals(object):
    """Generates a dictionary of conditional statements for building a query.

    The dictionary contains statements that ensure that a query is valid and has
    a specific start and end time, metric, and client ip blocks.

    Attributes:
        _conditional_dict: A dictionary representation of the conditions for the
            query. Can hold entries for log time and client ip blocks.
    """
    # NDT test is supposed to last 10 seconds, give some buffer for tests that
    # ended slighly before 10 seconds.
    MIN_DURATION = _seconds_to_microseconds(9)

    # Tests that last > 1 hour are likely erroneous.
    MAX_DURATION = _seconds_to_microseconds(3600)

    # A test that did not exchange at least 8,192 bytes is likely erroneous.
    MIN_BYTES = 8192

    # web100 state variable constants from
    # http://www.web100.org/download/kernel/tcp-kis.txt
    STATE_CLOSED = 1
    STATE_ESTABLISHED = 5
    STATE_TIME_WAIT = 11

    # For RTT metrics, exclude results of tests with 10 or fewer round trip time
    # samples, because there are not enough samples to accurately estimate the
    # RTT.
    MIN_RTT_SAMPLES = 10

    def __init__(self, start_time, end_time, client_ip_blocks):
        """Initializes a QueryConditional object.

        Args:
            start_time: datetime.datetime instance of starting time range.
            end_time: datetime.datetime instance of end of time range.
            client_ip_blocks: List of tuples of netaddr.IPNetwork objects.
                ex:
                    [(IPNetwork('1.2.0.0/16'), IPNetwork('5.1.0.0/16'))]
        """
        self._conditional_dict = {}
        # Must have completed the TCP three-way handshake.
        self._conditional_dict['complete_tcp'] = (
            '(web100_log_entry.snap.State = {state_closed}\n'
            '\t\tOR (web100_log_entry.snap.State >= {state_established}\n'
            '\t\t\tAND web100_log_entry.snap.State <= {state_time_wait}))').format(
                state_closed=QueryConditionals.STATE_CLOSED,
                state_established=QueryConditionals.STATE_ESTABLISHED,
                state_time_wait=QueryConditionals.STATE_TIME_WAIT)

        # Add non metric specific conditions
        self._add_log_time_conditional(start_time, end_time)
        self._add_client_ip_blocks_conditional(client_ip_blocks)

    def _add_log_time_conditional(self, start_time_datetime, end_time_datetime):
        utc_absolutely_utc = unix_timestamp_to_utc_datetime(0)
        start_time = int((start_time_datetime - utc_absolutely_utc
                         ).total_seconds())
        end_time = int((end_time_datetime - utc_absolutely_utc).total_seconds())

        new_statement = (
            '(web100_log_entry.log_time >= {start_time})'
            ' AND (web100_log_entry.log_time < {end_time})').format(
                start_time=start_time, end_time=end_time)

        self._conditional_dict['log_time'] = new_statement

    def _add_client_ip_blocks_conditional(self, client_ip_blocks):
        # remove duplicates, warn if any are found
        unique_client_ip_blocks = list(set(client_ip_blocks))
        if len(client_ip_blocks) != len(unique_client_ip_blocks):
            self.logger.warning('Client IP blocks contained duplicates.')

        # sort the blocks for the sake of consistent query generation
        unique_client_ip_blocks = sorted(unique_client_ip_blocks,
                                         key=lambda block: block[0])

        self._conditional_dict['client_ip_blocks'] = []
        for start_addr, end_addr in client_ip_blocks:
            new_statement = (
                'PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN '
                '{start_addr} AND {end_addr}').format(
                    start_addr=str(start_addr),
                    end_addr=str(end_addr))
            self._conditional_dict['client_ip_blocks'].append(new_statement)

    def _create_metric_specific_dict(self, metric):
        """Adds metric specific conditions to the conditional dictionary.

        Args:
            metric: Name of metric. Should be one of upload, download, or
                minimum_rtt

        Returns:
            A dictionary with metric specific conditions.
        """
        metric_dict = {}
        data_direction_conditional = ''

        if _is_server_to_client_metric(metric):
            data_direction = 1
        else:
            data_direction = 0
            data_direction_conditional += (
                '\n\tAND connection_spec.data_direction IS NOT NULL')
        metric_dict['data_direction'] = (
            'connection_spec.data_direction = %d' % data_direction +
            data_direction_conditional)

        valid_metric_conditional = []
        if _is_server_to_client_metric(metric):
            # Must leave slow start phase of TCP, indicated by reaching
            # congestion at least once.
            valid_metric_conditional.append('web100_log_entry.snap.CongSignals > 0')
            # Must send at least the minimum number of bytes.
            valid_metric_conditional.append('web100_log_entry.snap.HCThruOctetsAcked >= %d' %
                              QueryConditionals.MIN_BYTES)
            # Must last for at least the minimum test duration.
            valid_metric_conditional.append(
                ('(web100_log_entry.snap.SndLimTimeRwin +\n'
                 '\t\tweb100_log_entry.snap.SndLimTimeCwnd +\n'
                 '\t\tweb100_log_entry.snap.SndLimTimeSnd) >= %u') % QueryConditionals.MIN_DURATION)
            # Must not exceed the maximum test duration.
            valid_metric_conditional.append(
                ('(web100_log_entry.snap.SndLimTimeRwin +\n'
                 '\t\tweb100_log_entry.snap.SndLimTimeCwnd +\n'
                 '\t\tweb100_log_entry.snap.SndLimTimeSnd) < %u') % QueryConditionals.MAX_DURATION)

            # Exclude results of tests with fewer than 10 round trip time samples,
            # because there are not enough samples to accurately estimate the RTT.
            if metric == 'minimum_rtt':
                valid_metric_conditional.append(
                    'web100_log_entry.snap.CountRTT > %u' % QueryConditionals.MIN_RTT_SAMPLES)
        else:
            # Must receive at least the minimum number of bytes.
            valid_metric_conditional.append(
                'web100_log_entry.snap.HCThruOctetsReceived >= %u' % QueryConditionals.MIN_BYTES)
            # Must last for at least the minimum test duration.
            valid_metric_conditional.append('web100_log_entry.snap.Duration >= %u' % QueryConditionals.MIN_DURATION)
            # Must not exceed the maximum test duration.
            valid_metric_conditional.append('web100_log_entry.snap.Duration < %u' % QueryConditionals.MAX_DURATION)
        metric_dict[metric] = '\n\tAND '.join(valid_metric_conditional)
        return metric_dict

    def get_conditional_dict(self, metric):
        if metric not in METRICS:
            raise NotImplementedError()
        metric_dict = self._conditional_dict.copy()
        metric_dict.update(self._create_metric_specific_dict(metric))
        return metric_dict


class SubQueryGenerator(object):

    def __init__(self, metric, start_time, end_time, client_ip_blocks):
        self._conditionals = QueryConditionals(start_time, end_time, client_ip_blocks).get_conditional_dict(metric)
        self._metric = metric
        self._query = self._create_query_string()

    @property
    def query(self):
        return self._query

    def _create_query_string(self):
        built_query_format = ('SELECT\n'
                                '\t{select_clauses}\n'
                              'FROM\n'
                                '\t{table}\n'
                              'WHERE\n'
                                '\t{conditional_list}')

        conditional_list_string = ''
        conditional_list_string += self._conditionals['data_direction']
        conditional_list_string += '\n\t AND %s' % (
            self._conditionals[self._metric])
        conditional_list_string += '\n\tAND %s' % self._conditionals['log_time']

        client_ip_blocks_joined = ' OR\n\t\t'.join(self._conditionals[
            'client_ip_blocks'])
        conditional_list_string += '\n\tAND (%s)' % client_ip_blocks_joined

        built_query_string = built_query_format.format(
            select_clauses=self._create_select_clauses(self._metric),
            table='plx.google:m_lab.ndt.all',
            conditional_list=conditional_list_string)

        return built_query_string

    def _create_select_clauses(self, metric):
        if metric == 'download':
          return ('web100_log_entry.log_time AS timestamp,\n'
            '\t8 * (web100_log_entry.snap.HCThruOctetsAcked /\n'
            '\t\t(web100_log_entry.snap.SndLimTimeRwin +\n'
            '\t\t web100_log_entry.snap.SndLimTimeCwnd +\n'
            '\t\t web100_log_entry.snap.SndLimTimeSnd)) AS download_mbps')
        elif metric == 'upload':
          return ('web100_log_entry.log_time AS timestamp,\n'
            '\t8 * (web100_log_entry.snap.HCThruOctetsReceived /\n'
            '\t\t web100_log_entry.snap.Duration) AS upload_mbps')
        elif metric == 'minimum_rtt':
           return ('web100_log_entry.log_time AS timestamp,\n'
                '\tweb100_log_entry.snap.MinRTT AS minimum_rtt_ms')
        else:
           raise ValueError('bad metric: ' + metric)

def build_metric_median_query(metric, start_time, end_time, client_ip_blocks):
    """Builds a query to calculate the median of a metric by time and ip block.

    Args:
        metric: One of upload, download and minimum rtt.
        start_time: Datetime instance representing beginning of range.
        end_time: Datetime instance representing end of range.
        client_ip_blocks: List of tuples of ip blocks.

    Returns:
        String representation of a query.
    """
    built_query_format = ('SELECT\n'
                            '\t{median_select}\n'
                          'FROM\n'
                            '\t{subquery_table}')

    subquery = SubQueryGenerator(metric, start_time, end_time, client_ip_blocks)
    subquery_string = '(%s)' % subquery.query

    if metric == 'minimum_rtt':
        metric_with_unit = metric+'_ms'
    else:
        metric_with_unit = metric+'_mbps'
    select = 'NTH( 51, QUANTILES({m}, 101)) AS median_{m}'.format(m=metric_with_unit)

    return built_query_format.format(median_select=select, subquery_table=subquery_string)
