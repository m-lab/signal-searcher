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


"""Reads data from the MLab Cloud Bigtables."""

import datetime
import unittest

import mock

import btreader

def ValueArray(data):
    """Bigtable cells are lists where each element has a .value."""
    class ValueHaver(object):
        def __init__(self, d):
            self.value = d
    return [ValueHaver(data)]


class FakeRow(object):
    def __init__(self, dict_data):
        self.dict_data = dict_data

    def to_dict(self):
        return self.dict_data


class FakeResponse(object):
    def __init__(self, asns, loc, start_date, end_date):
        self.asns = asns
        self.loc = loc
        self.start_date = start_date
        self.current_date = self.start_date
        self.max_date = end_date
        self.asn_index = 0
        self.rows = {}

    def consume_next(self):
        if self.asn_index >= len(self.asns):
            raise StopIteration('Done with fake bigtable data')
        assert len(self.rows) == 0
        for _ in range(25):
            if self.current_date > self.max_date:
                self.asn_index += 1
                self.current_date = self.start_date
                if self.asn_index >= len(self.asns):
                    return
            key = '%s | %s | %s' % (self.asns[self.asn_index], self.loc,
                                    str(self.current_date))
            self.rows[key] = FakeRow(
                {'data:download_speed_mbps_median': ValueArray('?\xeb\xb1\xd3K\xb4{\x96'),
                 'data:upload_speed_mbps_median': ValueArray('?\xeb\xb1\xd3K\xb4{\x96'),
                 'data:rtt_avg': ValueArray('?\xeb\xb1\xd3K\xb4{\x96'),
                 'data:count': ValueArray('50')})
            self.current_date += datetime.timedelta(days=1)
        

class BtReaderTest(unittest.TestCase):
    def test_parse_key(self):
        key, date = btreader._parse_key('AS12 | nausny | 2017-08-09')
        self.assertEqual(key, 'AS12 | nausny ')
        self.assertEqual(date, datetime.date(2017, 8, 9))

    def test_parse_key_bad_key(self):
        key, date = btreader._parse_key('BAD')
        self.assertEqual(key, None)
        self.assertEqual(date, None)

    def test_parse_key_bad_date(self):
        key, date = btreader._parse_key('AS12 | 2017-02')
        self.assertEqual(key, None)
        self.assertEqual(date, None)

    def test_parse_key_sneakily_bad_date(self):
        key, date = btreader._parse_key('AS12 | 2017-02-30')
        self.assertEqual(key, None)
        self.assertEqual(date, None)

    def test_parse_key_and_data(self):
        key, data = btreader._parse_key_and_data(
                'AS12 | nausny | 2017-08-09',
                # Contains raw byte versions of doubles, just like bigtable.
                {'data:download_speed_mbps_median': ValueArray('?\xeb\xb1\xd3K\xb4{\x96'),
                 'data:upload_speed_mbps_median': ValueArray('?\xeb\xb1\xd3K\xb4{\x96'),
                 'data:rtt_avg': ValueArray('?\xeb\xb1\xd3K\xb4{\x96'),
                 'data:count': ValueArray('50')},
                'client_asn_client_loc_by_day')
        self.assertEqual(key, 'AS12 | nausny ')
        self.assertEqual(data.samples, 50)
        self.assertEqual(data.time, datetime.date(2017, 8, 9))
        self.assertAlmostEqual(data.download, .865, 3)
        self.assertAlmostEqual(data.upload, .865, 3)
        self.assertAlmostEqual(data.rtt, .865, 3)

    def test_parse_key_and_data_bad_key(self):
        key, data = btreader._parse_key_and_data(
                'AS12',
                # Contains raw byte versions of doubles, just like bigtable.
                {'data:download_speed_mbps_median': ValueArray('?\xeb\xb1\xd3K\xb4{\x96'),
                 'data:upload_speed_mbps_median': ValueArray('?\xeb\xb1\xd3K\xb4{\x96'),
                 'data:rtt_avg': ValueArray('?\xeb\xb1\xd3K\xb4{\x96'),
                 'data:count': ValueArray('50')},
                'client_asn_client_loc_by_day')
        self.assertEqual(key, None)
        self.assertEqual(data, None)

    def test_parse_key_and_data_missing_fields(self):
        key, data = btreader._parse_key_and_data(
                'AS12 | nausny | 2017-08-09',
                # Contains raw byte versions of doubles, just like bigtable.
                {'data:download_speed_mbps_median': ValueArray('?\xeb\xb1\xd3K\xb4{\x96'),
                 'data:count': ValueArray('50')},
                'client_asn_client_loc_by_day')
        self.assertEqual(key, None)
        self.assertEqual(data, None)

    @mock.patch.object(btreader, '_connect_to_table')
    def test_read_timeseries(self, mock_connect):
        start_date = datetime.date(2017, 8, 9)
        end_date = datetime.date(2018, 3, 1)
        mock_connect.return_value = FakeResponse(['AS12', 'AS999'], 'nausny',
                                                 start_date, end_date)
        (key1, data1), (key2, data2) = list(
                btreader.read_timeseries('client_asn_client_loc_by_day'))
        self.assertEqual(key1, 'AS12 | nausny ')
        self.assertEqual(list(sorted(data1)), data1)
        self.assertEqual(key2, 'AS999 | nausny ')
        self.assertEqual(list(sorted(data2)), data2)
        for datum in data1:
            self.assertAlmostEqual(datum.download, .865, 3)
        for datum in data2:
            self.assertAlmostEqual(datum.download, .865, 3)
        self.assertEqual(len(data1), (end_date - start_date).days + 1)
        self.assertEqual(len(data2), (end_date - start_date).days + 1)


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
