#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Copyright 2014 Measurement Lab
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
import netaddr
import pytz
import unittest

import query

class NormalizedStringTestCase(unittest.TestCase):

  def assertNormalizedStringsEqual(self, string1, string2):
    self.assertEqual(' '.join(string1.split()), ' '.join(string2.split()))


class QueryConditionalsTest(NormalizedStringTestCase):

  def setUp(self):
    start_time = datetime.datetime(2014, 1, 1, tzinfo=pytz.utc)
    end_time = datetime.datetime(2014, 2, 1, tzinfo=pytz.utc)
    client_ip_block = netaddr.IPNetwork('1.0.0.0/16')
    self.conditional = query.QueryConditionals(start_time, end_time, client_ip_block)

    self.generate_expected_nonmetric_dict(start_time, end_time, client_ip_block)

  def datetime_to_seconds(self, dt):
    return int((dt - datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds())

  def generate_expected_nonmetric_dict(self, start_time, end_time, client_ip_block):
    start_time_unix = self.datetime_to_seconds(start_time)
    end_time_unix = self.datetime_to_seconds(end_time)

    ip_range= query.ipnetwork_to_iprange(client_ip_block)
    expected_client_ip_block = (
      'PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN '
      'PARSE_IP(\'{start_addr}\') AND PARSE_IP(\'{end_addr}\')'.format(
          start_addr=str(ip_range[0]),
          end_addr=str(ip_range[1])))

    expected_complete_tcp = (
      '(web100_log_entry.snap.State = 1 '
        'OR (web100_log_entry.snap.State >= 5 '
          'AND web100_log_entry.snap.State <= 11))')

    expected_log_time = (
      '(web100_log_entry.log_time >= {start_time})'
      ' AND (web100_log_entry.log_time < {end_time})').format(
          start_time=start_time_unix, end_time=end_time_unix)

    self.expected_nonmetric_dict = {
      'complete_tcp' : expected_complete_tcp,
      'log_time' : expected_log_time,
      'client_ip_block' : expected_client_ip_block
    }

  def test_download_dictionary_has_expected_metric_conditions(self):
    expected_download = (
      'web100_log_entry.snap.CongSignals > 0 '
        'AND web100_log_entry.snap.HCThruOctetsAcked >= 8192 '
        'AND (web100_log_entry.snap.SndLimTimeRwin + '
          'web100_log_entry.snap.SndLimTimeCwnd + '
          'web100_log_entry.snap.SndLimTimeSnd) >= 9000000 '
        'AND (web100_log_entry.snap.SndLimTimeRwin + '
          'web100_log_entry.snap.SndLimTimeCwnd + '
          'web100_log_entry.snap.SndLimTimeSnd) < 3600000000')

    expected_data_direction = 'connection_spec.data_direction = 1'
    cond_dict = self.conditional.get_conditional_dict('download')

    self.assertNormalizedStringsEqual(cond_dict['download'], expected_download)
    self.assertNormalizedStringsEqual(cond_dict['data_direction'], expected_data_direction)

  def test_upload_dictionary_has_expected_conditions(self):
    cond_dict = self.conditional.get_conditional_dict('upload')

    actual_keys = cond_dict.keys()
    actual_keys.sort()
    self.assertListEqual(actual_keys, ['client_ip_block', 'complete_tcp', 'data_direction', 'log_time', 'upload'])

    expected_upload = (
      'web100_log_entry.snap.HCThruOctetsReceived >= 8192 '
        'AND web100_log_entry.snap.Duration >= 9000000 '
        'AND web100_log_entry.snap.Duration < 3600000000')

    expected_data_direction = (
      'connection_spec.data_direction = 0 '
      'AND connection_spec.data_direction IS NOT NULL')

    # For each key in the dictionary, make sure it has the same value
    self.assertEqual(cond_dict['client_ip_block'], self.expected_nonmetric_dict['client_ip_block'])
    self.assertNormalizedStringsEqual(cond_dict['complete_tcp'], self.expected_nonmetric_dict['complete_tcp'])
    self.assertNormalizedStringsEqual(cond_dict['log_time'], self.expected_nonmetric_dict['log_time'])

    self.assertNormalizedStringsEqual(cond_dict['upload'], expected_upload)
    self.assertNormalizedStringsEqual(cond_dict['data_direction'], expected_data_direction)

  def test_minimum_rtt_dictionary_has_expected_metric_conditions(self):
    expected_minimum_rtt = (
      'web100_log_entry.snap.CongSignals > 0 '
        'AND web100_log_entry.snap.HCThruOctetsAcked >= 8192 '
        'AND (web100_log_entry.snap.SndLimTimeRwin + '
          'web100_log_entry.snap.SndLimTimeCwnd + '
          'web100_log_entry.snap.SndLimTimeSnd) >= 9000000 '
        'AND (web100_log_entry.snap.SndLimTimeRwin + '
          'web100_log_entry.snap.SndLimTimeCwnd + '
          'web100_log_entry.snap.SndLimTimeSnd) < 3600000000 '
        'AND web100_log_entry.snap.CountRTT > 10'
    )

    expected_data_direction = 'connection_spec.data_direction = 1'
    cond_dict = self.conditional.get_conditional_dict('minimum_rtt')
    self.assertNormalizedStringsEqual(cond_dict['minimum_rtt'], expected_minimum_rtt)
    self.assertNormalizedStringsEqual(cond_dict['data_direction'], expected_data_direction)

class SubQueryGeneratorTest(NormalizedStringTestCase):

  def setUp(self):
    self.start_time = datetime.datetime(2014, 1, 1, tzinfo=pytz.utc)
    self.end_time = datetime.datetime(2014, 2, 1, tzinfo=pytz.utc)
    self.client_ip_block = netaddr.IPNetwork('1.0.0.0/16')

  def test_download_subquery_is_correct(self):
    subquery = query.SubQueryGenerator('download', self.start_time, self.end_time, self.client_ip_block)
    expected_query = (
      'SELECT '
        'web100_log_entry.log_time AS timestamp, '
        'DATE(SEC_TO_TIMESTAMP(web100_log_entry.log_time)) AS date, '
        'HOUR(SEC_TO_TIMESTAMP(web100_log_entry.log_time)) AS hour, '
        '8 * (web100_log_entry.snap.HCThruOctetsAcked / '
        '(web100_log_entry.snap.SndLimTimeRwin + '
        'web100_log_entry.snap.SndLimTimeCwnd + '
        'web100_log_entry.snap.SndLimTimeSnd)) AS download_mbps '
      'FROM plx.google:m_lab.ndt.all '
      'WHERE '
        'connection_spec.data_direction = 1 '
        'AND web100_log_entry.snap.CongSignals > 0 '
        'AND web100_log_entry.snap.HCThruOctetsAcked >= 8192 '
        'AND (web100_log_entry.snap.SndLimTimeRwin + '
          'web100_log_entry.snap.SndLimTimeCwnd + '
          'web100_log_entry.snap.SndLimTimeSnd) >= 9000000 '
        'AND (web100_log_entry.snap.SndLimTimeRwin + '
          'web100_log_entry.snap.SndLimTimeCwnd + '
          'web100_log_entry.snap.SndLimTimeSnd) < 3600000000 '
      'AND '
        '(web100_log_entry.log_time >= 1388534400) '
        ' AND (web100_log_entry.log_time < 1391212800) '
      'AND ('
        'PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN PARSE_IP(\'1.0.0.0\') AND PARSE_IP(\'1.0.255.255\'))'
    )
    self.assertNormalizedStringsEqual(subquery.query, expected_query)

  def test_upload_subquery_is_correct(self):
    subquery = query.SubQueryGenerator('upload', self.start_time, self.end_time, self.client_ip_block)
    expected_query = (
      'SELECT '
      'web100_log_entry.log_time AS timestamp, '
      'DATE(SEC_TO_TIMESTAMP(web100_log_entry.log_time)) AS date, '
      'HOUR(SEC_TO_TIMESTAMP(web100_log_entry.log_time)) AS hour, '
      '8 * (web100_log_entry.snap.HCThruOctetsReceived / '
      ' web100_log_entry.snap.Duration) AS upload_mbps '
      'FROM plx.google:m_lab.ndt.all '
      'WHERE '
        'connection_spec.data_direction = 0 '
        'AND connection_spec.data_direction IS NOT NULL '
        'AND web100_log_entry.snap.HCThruOctetsReceived >= 8192 '
        'AND web100_log_entry.snap.Duration >= 9000000 '
        'AND web100_log_entry.snap.Duration < 3600000000 '
      'AND '
        '(web100_log_entry.log_time >= 1388534400) '
        ' AND (web100_log_entry.log_time < 1391212800) '
      'AND ('
        'PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN PARSE_IP(\'1.0.0.0\') AND PARSE_IP(\'1.0.255.255\'))'
      )

    self.assertNormalizedStringsEqual(subquery.query, expected_query)

  def test_rtt_subquery_is_correct(self):
    subquery = query.SubQueryGenerator('minimum_rtt', self.start_time, self.end_time, self.client_ip_block)
    expected_query = (
      'SELECT '
        'web100_log_entry.log_time AS timestamp, '
        'DATE(SEC_TO_TIMESTAMP(web100_log_entry.log_time)) AS date, '
        'HOUR(SEC_TO_TIMESTAMP(web100_log_entry.log_time)) AS hour, '
        'web100_log_entry.snap.MinRTT AS minimum_rtt_ms '
      'FROM plx.google:m_lab.ndt.all '
      'WHERE '
        'connection_spec.data_direction = 1 '
        'AND web100_log_entry.snap.CongSignals > 0 '
        'AND web100_log_entry.snap.HCThruOctetsAcked >= 8192 '
        'AND (web100_log_entry.snap.SndLimTimeRwin + '
          'web100_log_entry.snap.SndLimTimeCwnd + '
          'web100_log_entry.snap.SndLimTimeSnd) >= 9000000 '
        'AND (web100_log_entry.snap.SndLimTimeRwin + '
          'web100_log_entry.snap.SndLimTimeCwnd + '
          'web100_log_entry.snap.SndLimTimeSnd) < 3600000000 '
          'AND web100_log_entry.snap.CountRTT > 10 '
      'AND '
        '(web100_log_entry.log_time >= 1388534400) '
        ' AND (web100_log_entry.log_time < 1391212800) '
      'AND ('
        'PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN PARSE_IP(\'1.0.0.0\') AND PARSE_IP(\'1.0.255.255\'))'
    )
    self.assertNormalizedStringsEqual(subquery.query, expected_query)

  def test_subquery_with_mock_metric_raises_not_implemented_error(self):
    with self.assertRaises(NotImplementedError):
      query.SubQueryGenerator('mock_metric', self.start_time, self.end_time, self.client_ip_block)

class BuildMetricMedianQueryTest(NormalizedStringTestCase):

  def setUp(self):
    self.start_time = datetime.datetime(2014, 1, 1, tzinfo=pytz.utc)
    self.end_time = datetime.datetime(2014, 2, 1, tzinfo=pytz.utc)
    self.client_ip_block = netaddr.IPNetwork('1.0.0.0/16')

  def test_build_valid_download_median_query(self):
    actual = query.build_metric_median_query('download', self.start_time, self.end_time, self.client_ip_block)
    expected_download_subquery = (
      '(SELECT '
        'web100_log_entry.log_time AS timestamp, '
        'DATE(SEC_TO_TIMESTAMP(web100_log_entry.log_time)) AS date, '
        'HOUR(SEC_TO_TIMESTAMP(web100_log_entry.log_time)) AS hour, '
        '8 * (web100_log_entry.snap.HCThruOctetsAcked / '
        '(web100_log_entry.snap.SndLimTimeRwin + '
        'web100_log_entry.snap.SndLimTimeCwnd + '
        'web100_log_entry.snap.SndLimTimeSnd)) AS download_mbps '
      'FROM plx.google:m_lab.ndt.all '
      'WHERE '
        'connection_spec.data_direction = 1 '
        'AND web100_log_entry.snap.CongSignals > 0 '
        'AND web100_log_entry.snap.HCThruOctetsAcked >= 8192 '
        'AND (web100_log_entry.snap.SndLimTimeRwin + '
          'web100_log_entry.snap.SndLimTimeCwnd + '
          'web100_log_entry.snap.SndLimTimeSnd) >= 9000000 '
        'AND (web100_log_entry.snap.SndLimTimeRwin + '
          'web100_log_entry.snap.SndLimTimeCwnd + '
          'web100_log_entry.snap.SndLimTimeSnd) < 3600000000 '
      'AND '
        '(web100_log_entry.log_time >= 1388534400) '
        ' AND (web100_log_entry.log_time < 1391212800) '
      'AND ('
        'PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN PARSE_IP(\'1.0.0.0\') AND PARSE_IP(\'1.0.255.255\')))'
    )

    expected = (
      'SELECT '
        'date, '
        'hour, '
        'NTH( 51, QUANTILES(download_mbps, 101)) AS hourly_median_download_mbps '
      'FROM '
        '{subquery} '
      'GROUP BY date, hour '
      'ORDER BY date, hour'
    ).format(subquery=expected_download_subquery)

    self.assertNormalizedStringsEqual(expected, actual)

  def test_build_valid_upload_median_query(self):
    actual = query.build_metric_median_query('upload', self.start_time, self.end_time, self.client_ip_block)
    expected_upload_subquery = (
      '(SELECT '
        'web100_log_entry.log_time AS timestamp, '
        'DATE(SEC_TO_TIMESTAMP(web100_log_entry.log_time)) AS date, '
        'HOUR(SEC_TO_TIMESTAMP(web100_log_entry.log_time)) AS hour, '
        '8 * (web100_log_entry.snap.HCThruOctetsReceived / '
        'web100_log_entry.snap.Duration) AS upload_mbps '
      'FROM plx.google:m_lab.ndt.all '
      'WHERE '
        'connection_spec.data_direction = 0 '
        'AND connection_spec.data_direction IS NOT NULL '
        'AND web100_log_entry.snap.HCThruOctetsReceived >= 8192 '
        'AND web100_log_entry.snap.Duration >= 9000000 '
        'AND web100_log_entry.snap.Duration < 3600000000 '
      'AND '
        '(web100_log_entry.log_time >= 1388534400) '
        'AND (web100_log_entry.log_time < 1391212800) '
      'AND ('
        'PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN PARSE_IP(\'1.0.0.0\') AND PARSE_IP(\'1.0.255.255\')))'
      )

    expected = (
      'SELECT '
        'date, '
        'hour, '
        'NTH( 51, QUANTILES(upload_mbps, 101)) AS hourly_median_upload_mbps '
      'FROM '
        '{subquery} '
      'GROUP BY date, hour '
      'ORDER BY date, hour'
    ).format(subquery=expected_upload_subquery)

    self.assertNormalizedStringsEqual(expected, actual)

  def test_build_valid_rtt_median_query(self):
    actual = query.build_metric_median_query('minimum_rtt', self.start_time, self.end_time, self.client_ip_block)
    expected_rtt_subquery = (
      '(SELECT '
      'web100_log_entry.log_time AS timestamp, '
      'DATE(SEC_TO_TIMESTAMP(web100_log_entry.log_time)) AS date, '
      'HOUR(SEC_TO_TIMESTAMP(web100_log_entry.log_time)) AS hour, '
      'web100_log_entry.snap.MinRTT AS minimum_rtt_ms '
      'FROM plx.google:m_lab.ndt.all '
      'WHERE '
      'connection_spec.data_direction = 1 '
      'AND web100_log_entry.snap.CongSignals > 0 '
        'AND web100_log_entry.snap.HCThruOctetsAcked >= 8192 '
        'AND (web100_log_entry.snap.SndLimTimeRwin + '
          'web100_log_entry.snap.SndLimTimeCwnd + '
          'web100_log_entry.snap.SndLimTimeSnd) >= 9000000 '
        'AND (web100_log_entry.snap.SndLimTimeRwin + '
          'web100_log_entry.snap.SndLimTimeCwnd + '
          'web100_log_entry.snap.SndLimTimeSnd) < 3600000000 '
          'AND web100_log_entry.snap.CountRTT > 10 '
      'AND '
        '(web100_log_entry.log_time >= 1388534400) '
        ' AND (web100_log_entry.log_time < 1391212800) '
      'AND ('
        'PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN PARSE_IP(\'1.0.0.0\') AND PARSE_IP(\'1.0.255.255\')))'
    )

    expected = (
      'SELECT '
        'date, '
        'hour, '
        'NTH( 51, QUANTILES(minimum_rtt_ms, 101)) AS hourly_median_minimum_rtt_ms '
      'FROM '
        '{subquery} '
      'GROUP BY date, hour '
      'ORDER BY date, hour'
    ).format(subquery=expected_rtt_subquery)

    self.assertNormalizedStringsEqual(expected, actual)

class IpnetworkToIpRangeQueryTest(unittest.TestCase):

  def test_build_valid_rtt_median_query(self):
    ip_network = netaddr.IPNetwork('1.0.0.0/24')
    actual = query.ipnetwork_to_iprange(ip_network)
    self.assertEqual(actual[0], netaddr.IPAddress('1.0.0.0'))
    self.assertEqual(actual[1], netaddr.IPAddress('1.0.0.255'))

  def test_build_valid_rtt_median_query(self):
    ip_network = netaddr.IPNetwork('10.0.0.0/16')
    actual = query.ipnetwork_to_iprange(ip_network)
    self.assertEqual(actual[0], netaddr.IPAddress('10.0.0.0'))
    self.assertEqual(actual[1], netaddr.IPAddress('10.0.255.255'))

if __name__ == '__main__':
  unittest.main()
