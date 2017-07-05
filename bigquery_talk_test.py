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

# pylint: disable=missing-docstring, invalid-name

import bigquery_talk
import query

import datetime
import mock
import netaddr
import pytz
import unittest

import apiclient


class MockHttpError(apiclient.errors.HttpError):

    def __init__(self):
        self.resp = mock.Mock()
        self.uri = ''
        self.content = ''


class GoogleAPIAuthHandlerTest(unittest.TestCase):

    def setUp(self):
        self.GoogleAPIAuthHandler = bigquery_talk.GoogleAPIAuthHandler(
            'mock_credentials_path')

        storage_patch = mock.patch.object(
            bigquery_talk.Storage, 'get', autospec=True)
        self.addCleanup(storage_patch.stop)
        self.storage_mock = storage_patch.start()

        service_build_patch = mock.patch.object(
            bigquery_talk, 'build', autospec=True)
        self.addCleanup(service_build_patch.stop)
        service_build_patch.start()

        os_patch = mock.patch.object(bigquery_talk.os.path, 'join')
        self.addCleanup(os_patch.stop)
        os_patch.start()

        self.mock_http = mock.Mock()
        http_patch = mock.patch.object(
            bigquery_talk.httplib2, 'Http', return_value=self.mock_http)
        self.addCleanup(http_patch.stop)
        http_patch.start()

        runflow_patch = mock.patch.object(bigquery_talk, 'run_flow')
        self.addCleanup(runflow_patch.stop)
        self.runflow_mock = runflow_patch.start()

    def test_get_authenticated_google_service_when_credentials_exist(self):
        mock_credentials = mock.Mock(name='mock_credentials', invalid=False)
        self.storage_mock.return_value = mock_credentials
        self.GoogleAPIAuthHandler.get_authenticated_google_service()

        self.runflow_mock.assert_not_called()
        mock_credentials.authorize.assert_called_with(self.mock_http)

    @mock.patch.object(bigquery_talk, 'flow_from_clientsecrets', autospec=True)
    def test_get_authenticated_google_service_credentials_need_to_be_obtained(
            self, mock_clientsecrets):
        mock_credentials = mock.Mock()
        self.runflow_mock.return_value = mock_credentials

        self.storage_mock.return_value = None
        self.GoogleAPIAuthHandler.get_authenticated_google_service()

        self.runflow_mock.has_been_called()
        mock_credentials.authorize.assert_called_with(self.mock_http)


class BigQueryCallTest(unittest.TestCase):

    def setUp(self):
        self.project_id = 'mock_project_id'
        self.mock_auth_service = mock.Mock(name='auth')
        self.BigQueryCall = bigquery_talk.BigQueryCall(self.mock_auth_service,
                                                       self.project_id)

    def test_run_asynchronous_query_raise_BigQueryCommunicationError(self):
        with self.assertRaises(bigquery_talk.BigQueryCommunicationError):

            mock_query_body = {
                'configuration': {
                    'query': {
                        'kind': 'bigquery#queryRequest',
                        'query': 'mock_query_string',
                        'timeoutMS': 15000
                    }
                }
            }

            mock_response = {'jobReference': {'jobId': 'mock_job_id'}}
            mock_request = mock.Mock()
            mock_request.execute.return_value = mock_response

            mock_job = mock.Mock()
            mock_job.insert.side_effect = MockHttpError
            self.mock_auth_service.jobs.return_value = mock_job

            self.BigQueryCall.run_asynchronous_query(mock_query_body)


class BigQueryCallHandlerTest(unittest.TestCase):

    def setUp(self):
        self.call_handler = bigquery_talk.BigQueryCallHandler(
            mock.Mock(), 'mock_job_id', 'mock_project_id')

        logger_patch = mock.patch.object(bigquery_talk, 'logger')
        self.addCleanup(logger_patch.stop)
        logger_patch.start()

        time_patch = mock.patch.object(bigquery_talk.time, 'sleep')
        self.addCleanup(time_patch.stop)
        time_patch.start()

        get_status_patch = mock.patch.object(bigquery_talk.BigQueryCallHandler,
                                             '_get_query_status')
        self.addCleanup(get_status_patch.stop)
        self.mock_get_status = get_status_patch.start()

    @mock.patch.object(bigquery_talk.BigQueryCallHandler,
                       '_collect_query_results')
    def test_wait_for_query_results_done_on_first_iteration(self,
                                                            collect_patch):
        self.mock_get_status.return_value = 'DONE'
        self.call_handler.wait_for_query_results()

        self.mock_get_status.assert_called_once_with()
        collect_patch.has_been_called()

    def test_wait_for_query_results_exception_returns_none(self):
        self.mock_get_status.side_effect = MockHttpError
        mock_results = self.call_handler.wait_for_query_results()
        self.assertIsNone(mock_results)

    def test_wait_for_query_results_unrecognized_status_raises_exception(self):
        with self.assertRaises(Exception):
            self.mock_get_status.return_value = 'mock'
            self.call_handler.wait_for_query_results()

    @mock.patch.object(bigquery_talk.BigQueryCallHandler,
                       '_collect_query_results')
    def test_wait_for_query_results_pending_on_first_iteration(self,
                                                               collect_patch):
        self.mock_get_status.side_effect = ['PENDING', 'DONE']
        self.call_handler.wait_for_query_results()

        self.assertEqual(self.mock_get_status.call_count, 2)
        collect_patch.assert_called_once_with()

    @mock.patch.object(bigquery_talk.BigQueryCallHandler,
                       '_collect_query_results')
    def test_wait_for_query_results_running_on_first_iteration(self,
                                                               collect_patch):
        self.mock_get_status.side_effect = ['RUNNING', 'DONE']
        self.call_handler.wait_for_query_results()

        self.assertEqual(self.mock_get_status.call_count, 2)
        collect_patch.assert_called_once_with()


class BigQueryCallQueryIntegrationTest(unittest.TestCase):

    def setUp(self):
        self.start_time = datetime.datetime(2014, 1, 1, tzinfo=pytz.utc)
        self.end_time = datetime.datetime(2014, 2, 1, tzinfo=pytz.utc)
        self.client_ip_block = netaddr.IPNetwork('1.0.0.0/16')
        self.mock_auth_service = mock.Mock(name='auth_service')
        self.bq_call = bigquery_talk.BigQueryCall(self.mock_auth_service,
                                                  'mock_project_id')

    def test_query_BigQuery_with_the_expected_upload_query_string(self):
        mock_request = mock.Mock()
        mock_request.execute.return_value = {
            'jobReference': {
                'jobId': 'mock_job_id'
            }
        }
        mock_job = mock.Mock()
        mock_job.insert.return_value = mock_request
        self.mock_auth_service.jobs.return_value = mock_job

        query_string = query.build_metric_median_query(
            'upload', self.start_time, self.end_time, self.client_ip_block)
        call_handler = self.bq_call.run_asynchronous_query(query_string)

        expected_job_body = {
            'configuration': {
                'query': {
                    'kind': 'bigquery#queryRequest',
                    'query': query_string,
                    'timeoutMS': 15000
                }
            }
        }
        mock_job.insert.assert_called_with(
            body=expected_job_body, projectId='mock_project_id')

    def test_query_BigQuery_with_the_expected_download_query_string(self):
        mock_request = mock.Mock()
        mock_request.execute.return_value = {
            'jobReference': {
                'jobId': 'mock_job_id'
            }
        }
        mock_job = mock.Mock()
        mock_job.insert.return_value = mock_request
        self.mock_auth_service.jobs.return_value = mock_job

        query_string = query.build_metric_median_query(
            'download', self.start_time, self.end_time, self.client_ip_block)
        call_handler = self.bq_call.run_asynchronous_query(query_string)

        expected_job_body = {
            'configuration': {
                'query': {
                    'kind': 'bigquery#queryRequest',
                    'query': query_string,
                    'timeoutMS': 15000
                }
            }
        }
        mock_job.insert.assert_called_with(
            body=expected_job_body, projectId='mock_project_id')

    def test_query_BigQuery_with_the_expected_minimum_rtt_query_string(self):
        mock_request = mock.Mock()
        mock_request.execute.return_value = {
            'jobReference': {
                'jobId': 'mock_job_id'
            }
        }
        mock_job = mock.Mock()
        mock_job.insert.return_value = mock_request
        self.mock_auth_service.jobs.return_value = mock_job

        query_string = query.build_metric_median_query(
            'minimum_rtt', self.start_time, self.end_time, self.client_ip_block)
        call_handler = self.bq_call.run_asynchronous_query(query_string)

        expected_job_body = {
            'configuration': {
                'query': {
                    'kind': 'bigquery#queryRequest',
                    'query': query_string,
                    'timeoutMS': 15000
                }
            }
        }
        mock_job.insert.assert_called_with(
            body=expected_job_body, projectId='mock_project_id')


class TuplefyResponseTest(unittest.TestCase):

    def test_tuplefy_response_complete_but_offset_data(self):
        data = {
            'rows': [{
                'f': [{
                    'v': '2014-01-01'
                }, {
                    'v': '3'
                }, {
                    'v': '0.3'
                }]
            }, {
                'f': [{
                    'v': '2014-01-01'
                }, {
                    'v': '4'
                }, {
                    'v': '0.4'
                }]
            }, {
                'f': [{
                    'v': '2014-01-01'
                }, {
                    'v': '5'
                }, {
                    'v': '0.5'
                }]
            }, {
                'f': [{
                    'v': '2014-01-01'
                }, {
                    'v': '6'
                }, {
                    'v': '0.6'
                }]
            }, {
                'f': [{
                    'v': '2014-01-01'
                }, {
                    'v': '7'
                }, {
                    'v': '0.7'
                }]
            }, {
                'f': [{
                    'v': '2014-01-01'
                }, {
                    'v': '8'
                }, {
                    'v': '0.8'
                }]
            }, {
                'f': [{
                    'v': '2014-01-01'
                }, {
                    'v': '9'
                }, {
                    'v': '0.9'
                }]
            }, {
                'f': [{
                    'v': '2014-01-01'
                }, {
                    'v': '10'
                }, {
                    'v': '0.10'
                }]
            }, {
                'f': [{
                    'v': '2014-01-01'
                }, {
                    'v': '11'
                }, {
                    'v': '0.11'
                }]
            }, {
                'f': [{
                    'v': '2014-01-01'
                }, {
                    'v': '12'
                }, {
                    'v': '0.12'
                }]
            }, {
                'f': [{
                    'v': '2014-01-01'
                }, {
                    'v': '13'
                }, {
                    'v': '0.13'
                }]
            }, {
                'f': [{
                    'v': '2014-01-01'
                }, {
                    'v': '14'
                }, {
                    'v': '0.14'
                }]
            }]
        }

        actual = bigquery_talk.tuplefy_response(data)
        expected = [
            (datetime.date(2014, 1, 1), 0, None),
            (datetime.date(2014, 1, 1), 1, None),
            (datetime.date(2014, 1, 1), 2, None),
            (datetime.date(2014, 1, 1), 3, 0.3),
            (datetime.date(2014, 1, 1), 4, 0.4),
            (datetime.date(2014, 1, 1), 5, 0.5),
            (datetime.date(2014, 1, 1), 6, 0.6), (datetime.date(
                2014, 1, 1), 7, 0.7), (datetime.date(2014, 1, 1), 8, 0.8),
            (datetime.date(2014, 1, 1), 9, 0.9), (datetime.date(
                2014, 1, 1), 10, 0.1), (datetime.date(2014, 1, 1), 11, 0.11),
            (datetime.date(2014, 1, 1), 12, 0.12), (datetime.date(
                2014, 1, 1), 13, 0.13), (datetime.date(2014, 1, 1), 14, 0.14),
            (datetime.date(2014, 1, 1), 15, None), (datetime.date(
                2014, 1, 1), 16, None), (datetime.date(2014, 1, 1), 17, None),
            (datetime.date(2014, 1, 1), 18, None), (datetime.date(
                2014, 1, 1), 19, None), (datetime.date(2014, 1, 1), 20, None),
            (datetime.date(2014, 1, 1), 21, None), (datetime.date(
                2014, 1, 1), 22, None), (datetime.date(2014, 1, 1), 23, None)
        ]

        self.assertListEqual(actual, expected)

    def test_tuplefy_response_incomplete_data_skipping_a_full_day(self):
        data = {
            'rows': [{
                'f': [{
                    'v': '2014-01-04'
                }, {
                    'v': '3'
                }, {
                    'v': '4.3'
                }]
            }, {
                'f': [{
                    'v': '2014-01-04'
                }, {
                    'v': '19'
                }, {
                    'v': '4.19'
                }]
            }, {
                'f': [{
                    'v': '2014-01-06'
                }, {
                    'v': '11'
                }, {
                    'v': '6.11'
                }]
            }, {
                'f': [{
                    'v': '2014-01-06'
                }, {
                    'v': '13'
                }, {
                    'v': '6.13'
                }]
            }]
        }

        expected = [
            (datetime.date(2014, 1, 4), 0, None),
            (datetime.date(2014, 1, 4), 1, None),
            (datetime.date(2014, 1, 4), 2, None),
            (datetime.date(2014, 1, 4), 3, 4.3),
            (datetime.date(2014, 1, 4), 4, None),
            (datetime.date(2014, 1, 4), 5, None),
            (datetime.date(2014, 1, 4), 6, None), (datetime.date(
                2014, 1, 4), 7, None), (datetime.date(2014, 1, 4), 8, None),
            (datetime.date(2014, 1, 4), 9, None), (datetime.date(
                2014, 1, 4), 10, None), (datetime.date(2014, 1, 4), 11, None),
            (datetime.date(2014, 1, 4), 12, None), (datetime.date(
                2014, 1, 4), 13, None), (datetime.date(2014, 1, 4), 14, None),
            (datetime.date(2014, 1, 4), 15, None), (datetime.date(
                2014, 1, 4), 16, None), (datetime.date(2014, 1, 4), 17, None),
            (datetime.date(2014, 1, 4), 18, None), (datetime.date(
                2014, 1, 4), 19, 4.19), (datetime.date(2014, 1, 4), 20, None),
            (datetime.date(2014, 1, 4), 21, None), (datetime.date(
                2014, 1, 4), 22, None), (datetime.date(2014, 1, 4), 23, None),
            (datetime.date(2014, 1, 5), 0, None), (datetime.date(
                2014, 1, 5), 1, None), (datetime.date(2014, 1, 5), 2, None),
            (datetime.date(2014, 1, 5), 3, None), (datetime.date(
                2014, 1, 5), 4, None), (datetime.date(2014, 1, 5), 5, None),
            (datetime.date(2014, 1, 5), 6, None), (datetime.date(
                2014, 1, 5), 7, None), (datetime.date(2014, 1, 5), 8, None),
            (datetime.date(2014, 1, 5), 9, None), (datetime.date(
                2014, 1, 5), 10, None), (datetime.date(2014, 1, 5), 11, None),
            (datetime.date(2014, 1, 5), 12, None), (datetime.date(
                2014, 1, 5), 13, None), (datetime.date(2014, 1, 5), 14, None),
            (datetime.date(2014, 1, 5), 15, None), (datetime.date(
                2014, 1, 5), 16, None), (datetime.date(2014, 1, 5), 17, None),
            (datetime.date(2014, 1, 5), 18, None), (datetime.date(
                2014, 1, 5), 19, None), (datetime.date(2014, 1, 5), 20, None),
            (datetime.date(2014, 1, 5), 21, None), (datetime.date(
                2014, 1, 5), 22, None), (datetime.date(2014, 1, 5), 23, None),
            (datetime.date(2014, 1, 6), 0, None), (datetime.date(
                2014, 1, 6), 1, None), (datetime.date(2014, 1, 6), 2, None),
            (datetime.date(2014, 1, 6), 3, None), (datetime.date(
                2014, 1, 6), 4, None), (datetime.date(2014, 1, 6), 5, None),
            (datetime.date(2014, 1, 6), 6, None), (datetime.date(
                2014, 1, 6), 7, None), (datetime.date(2014, 1, 6), 8, None),
            (datetime.date(2014, 1, 6), 9, None), (datetime.date(
                2014, 1, 6), 10, None), (datetime.date(2014, 1, 6), 11, 6.11),
            (datetime.date(2014, 1, 6), 12, None), (datetime.date(
                2014, 1, 6), 13, 6.13), (datetime.date(2014, 1, 6), 14, None),
            (datetime.date(2014, 1, 6), 15, None), (datetime.date(
                2014, 1, 6), 16, None), (datetime.date(2014, 1, 6), 17, None),
            (datetime.date(2014, 1, 6), 18, None), (datetime.date(
                2014, 1, 6), 19, None), (datetime.date(2014, 1, 6), 20, None),
            (datetime.date(2014, 1, 6), 21, None), (datetime.date(
                2014, 1, 6), 22, None), (datetime.date(2014, 1, 6), 23, None)
        ]

        actual = bigquery_talk.tuplefy_response(data)
        self.assertListEqual(actual, expected)

    def test_tuplefy_response_incomplete_data_with_gaps_at_the_day_border(self):
        data = {
            'rows': [{
                'f': [{
                    'v': '2014-01-04'
                }, {
                    'v': '3'
                }, {
                    'v': '4.3'
                }]
            }, {
                'f': [{
                    'v': '2014-01-04'
                }, {
                    'v': '19'
                }, {
                    'v': '4.19'
                }]
            }, {
                'f': [{
                    'v': '2014-01-04'
                }, {
                    'v': '23'
                }, {
                    'v': '4.23'
                }]
            }, {
                'f': [{
                    'v': '2014-01-05'
                }, {
                    'v': '11'
                }, {
                    'v': '5.11'
                }]
            }, {
                'f': [{
                    'v': '2014-01-05'
                }, {
                    'v': '14'
                }, {
                    'v': '5.14'
                }]
            }, {
                'f': [{
                    'v': '2014-01-05'
                }, {
                    'v': '19'
                }, {
                    'v': '5.19'
                }]
            }]
        }

        actual = bigquery_talk.tuplefy_response(data)

        expected = [
            (datetime.date(2014, 1, 4), 0, None),
            (datetime.date(2014, 1, 4), 1, None),
            (datetime.date(2014, 1, 4), 2, None),
            (datetime.date(2014, 1, 4), 3, 4.3),
            (datetime.date(2014, 1, 4), 4, None),
            (datetime.date(2014, 1, 4), 5, None),
            (datetime.date(2014, 1, 4), 6, None), (datetime.date(
                2014, 1, 4), 7, None), (datetime.date(2014, 1, 4), 8, None),
            (datetime.date(2014, 1, 4), 9, None), (datetime.date(
                2014, 1, 4), 10, None), (datetime.date(2014, 1, 4), 11, None),
            (datetime.date(2014, 1, 4), 12, None), (datetime.date(
                2014, 1, 4), 13, None), (datetime.date(2014, 1, 4), 14, None),
            (datetime.date(2014, 1, 4), 15, None), (datetime.date(
                2014, 1, 4), 16, None), (datetime.date(2014, 1, 4), 17, None),
            (datetime.date(2014, 1, 4), 18, None), (datetime.date(
                2014, 1, 4), 19, 4.19), (datetime.date(2014, 1, 4), 20, None),
            (datetime.date(2014, 1, 4), 21, None), (datetime.date(
                2014, 1, 4), 22, None), (datetime.date(2014, 1, 4), 23, 4.23),
            (datetime.date(2014, 1, 5), 0, None), (datetime.date(
                2014, 1, 5), 1, None), (datetime.date(2014, 1, 5), 2, None),
            (datetime.date(2014, 1, 5), 3, None), (datetime.date(
                2014, 1, 5), 4, None), (datetime.date(2014, 1, 5), 5, None),
            (datetime.date(2014, 1, 5), 6, None), (datetime.date(
                2014, 1, 5), 7, None), (datetime.date(2014, 1, 5), 8, None),
            (datetime.date(2014, 1, 5), 9, None), (datetime.date(
                2014, 1, 5), 10, None), (datetime.date(2014, 1, 5), 11, 5.11),
            (datetime.date(2014, 1, 5), 12, None), (datetime.date(
                2014, 1, 5), 13, None), (datetime.date(2014, 1, 5), 14, 5.14),
            (datetime.date(2014, 1, 5), 15, None), (datetime.date(
                2014, 1, 5), 16, None), (datetime.date(2014, 1, 5), 17, None),
            (datetime.date(2014, 1, 5), 18, None), (datetime.date(
                2014, 1, 5), 19, 5.19), (datetime.date(2014, 1, 5), 20, None),
            (datetime.date(2014, 1, 5), 21, None), (datetime.date(
                2014, 1, 5), 22, None), (datetime.date(2014, 1, 5), 23, None)
        ]

        self.assertListEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
