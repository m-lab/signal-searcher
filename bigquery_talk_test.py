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
import bigquery_talk

import mock
import unittest

import apiclient

class MockHttpError(apiclient.errors.HttpError):

    def __init__(self):
      self.resp = mock.Mock()
      self.uri = ''
      self.content = ''

class GoogleAPIAuthHandlerTest(unittest.TestCase):

  def setUp(self):
    self.GoogleAPIAuthHandler = bigquery_talk.GoogleAPIAuthHandler('mock_credentials_path')

    storage_patch = mock.patch.object(bigquery_talk.Storage, 'get', autospec=True)
    self.addCleanup(storage_patch.stop)
    self.storage_mock = storage_patch.start()

    service_build_patch = mock.patch.object(bigquery_talk, 'build', autospec=True)
    self.addCleanup(service_build_patch.stop)
    service_build_patch.start()

    os_patch = mock.patch.object(bigquery_talk.os.path, 'join')
    self.addCleanup(os_patch.stop)
    os_patch.start()

    self.mock_http = mock.Mock()
    http_patch = mock.patch.object(bigquery_talk.httplib2, 'Http', return_value=self.mock_http)
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
  def test_get_authenticated_google_service_credentials_need_to_be_obtained(self, mock_clientsecrets):
    mock_credentials = mock.Mock()
    self.runflow_mock.return_value = mock_credentials

    self.storage_mock.return_value = None
    self.GoogleAPIAuthHandler.get_authenticated_google_service()

    self.runflow_mock.has_been_called()
    mock_credentials.authorize.assert_called_with(self.mock_http)

class BigQueryCallTest(unittest.TestCase):

  def setUp(self):
    self.project_id = 'mock_project_id'
    self.mock_auth_service = mock.Mock(name="auth")
    self.BigQueryCall = bigquery_talk.BigQueryCall(self.mock_auth_service, self.project_id)

  def test_run_asynchronous_query_raise_BigQueryCommunicationError(self):
    with self.assertRaises(bigquery_talk.BigQueryCommunicationError):

      mock_query_body = {
        'configuration':{
            'query': {
                'kind': 'bigquery#queryRequest',
                'query': 'mock_query_string',
                'timeoutMS': 15000 }}}

      mock_response = {'jobReference': {'jobId': 'mock_job_id'} }
      mock_request = mock.Mock()
      mock_request.execute.return_value = mock_response

      mock_job = mock.Mock()
      mock_job.insert.side_effect = MockHttpError
      self.mock_auth_service.jobs.return_value = mock_job

      self.BigQueryCall.run_asynchronous_query(mock_query_body)

class BigQueryCallHandlerTest(unittest.TestCase):

  def setUp(self):
    self.call_handler = bigquery_talk.BigQueryCallHandler(mock.Mock(), 'mock_job_id', 'mock_project_id')

    logger_patch = mock.patch.object(bigquery_talk, 'logger')
    self.addCleanup(logger_patch.stop)
    logger_patch.start()

    time_patch = mock.patch.object(bigquery_talk.time, 'sleep')
    self.addCleanup(time_patch.stop)
    time_patch.start()

    get_status_patch = mock.patch.object(bigquery_talk.BigQueryCallHandler, '_get_query_status')
    self.addCleanup(get_status_patch.stop)
    self.mock_get_status = get_status_patch.start()

  @mock.patch.object(bigquery_talk.BigQueryCallHandler, '_collect_query_results')
  def test_wait_for_query_results_done_on_first_iteration(self, collect_patch):
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

  @mock.patch.object(bigquery_talk.BigQueryCallHandler, '_collect_query_results')
  def test_wait_for_query_results_pending_on_first_iteration(self, collect_patch):
    self.mock_get_status.side_effect = ['PENDING', 'DONE']
    self.call_handler.wait_for_query_results()

    self.assertEqual(self.mock_get_status.call_count, 2)
    collect_patch.assert_called_once_with()

  @mock.patch.object(bigquery_talk.BigQueryCallHandler, '_collect_query_results')
  def test_wait_for_query_results_running_on_first_iteration(self, collect_patch):
    self.mock_get_status.side_effect = ['RUNNING', 'DONE']
    self.call_handler.wait_for_query_results()

    self.assertEqual(self.mock_get_status.call_count, 2)
    collect_patch.assert_called_once_with()


if __name__ == '__main__':
  unittest.main()
