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
import httplib
import httplib2
import logging
import os
import ssl
import time

from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow

logger = logging.getLogger(__name__)

class BigQueryError(Exception):
    pass

class BigQueryCommunicationError(BigQueryError):
    """Indicates that an error occurred trying to communicate with BigQuery.

    This error means that the result of the query is indeterminate
    because the application failed to communicate with BigQuery.
    """
    def __init__(self, message, cause):
        self.cause = cause
        super(BigQueryCommunicationError, self).__init__('%s (%s)' %
                                                         (message, self.cause))

class BigQueryJobFailure(BigQueryError):
    """Indicates that a query completed but was unsuccessful."""

    def __init__(self, http_code, cause):
        self.code = http_code
        super(BigQueryJobFailure, self).__init__(cause)

class TableDoesNotExist(BigQueryError):

    def __init__(self):
        super(TableDoesNotExist, self).__init__()


class GoogleAPIAuthConfig(object):
    """Specifies Google API logging and authentication preferences."""
    logging_level = 'ERROR'
    noauth_local_webserver = False
    auth_host_port = [8080, 8090]
    auth_host_name = 'localhost'

class GoogleAPIAuthHandler(object):
    """Handles all steps of Google API authentication."""

    def __init__(self, credentials_filepath, is_headless=False):
        self.credentials_filepath = credentials_filepath
        self._set_headless_mode(is_headless)
        self._project_id = None

    def _set_headless_mode(self, is_headless):
        GoogleAPIAuthConfig.noauth_local_webserver = is_headless

    def get_authenticated_google_service(self):
        """Retrieves an authenticated Service object.

        Returns:
            Google API Service object.
        """
        # Check to see if credentials are already stored
        storage = Storage(self.credentials_filepath)
        credentials = storage.get()

        http = httplib2.Http()

        # If not, run flow to get credentials
        if credentials is None or credentials.invalid:
            # Build flow
            flow = flow_from_clientsecrets(
                os.path.join(
                    os.path.dirname(__file__), 'resources/client_secrets.json'),
                scope='https://www.googleapis.com/auth/bigquery')

            # Run flow to retrieve credentials
            credentials = run_flow(flow=flow,
                                   storage=storage,
                                   flags=GoogleAPIAuthConfig,
                                   http=http)

        http = credentials.authorize(http)

        # Return GoogleAPI Service object and set project id
        service = build('bigquery', 'v2', http=http)
        self._find_project_id_opportunistically(service)

        logger.info('Authenticated with Google.')

        return service

    def _find_project_id_opportunistically(self, authenticated_service):
        """Finds any Google Cloud project associated with the user account.

        Signal Searcher runs against the M-Lab BigQuery dataset, which is free
        so any project can be specified without prompting charges.

        Returns:
            Project number id.
        """
        projects_handler = authenticated_service.projects()
        projects_list = projects_handler.list().execute()

        if projects_list['totalItems'] == 0:
            raise APIConfigError()
        else:
            project_numeric_id = projects_list['projects'][0]['numericId']

        return project_numeric_id


class BigQueryCall(object):

    def __init__(self, authenticated_service, project_id):
        self._authenticated_service = authenticated_service
        self._project_id = project_id

    def run_asynchronous_query(self, query_string):
        """Run an asynchronous query.

        Args:
            query_string: String of query to be run.

        Returns:
            A BigQueryHandler instance to watch to query job for a result.

        Raises:
            BigQueryCommunicationError: An error occurred while trying to add
            the query to the queue.
        """
        # 15 second timeout
        timeout_ms = 15000
        try:
            job_query_body = {
                'configuration':{
                    'query': {
                        'kind': 'bigquery#queryRequest',
                        'query': query_string,
                        'timeoutMS': timeout_ms }}}

            query_request = self._authenticated_service.jobs().insert(projectId=self._project_id, body=job_query_body)


            query_response = query_request.execute()
            job_id = query_response['jobReference']['jobId']

            logger.info('Started query.')

        except (HttpError, httplib.ResponseNotReady) as e:
            raise BigQueryCommunicationError(
                'Failed to communicate with BigQuery', e)

        return BigQueryCallHandler(self._authenticated_service, job_id, self._project_id)

class BigQueryCallHandler(object):

    def __init__(self, authenticated_service, job_id, project_id):
        self._job = authenticated_service.jobs()
        self._job_id = job_id
        self._project_id = project_id

    def _get_query_status(self):
        query_state = self._job.get(
            projectId=self._project_id,
            jobId=self._job_id).execute()['status']['state']
        return query_state

    def wait_for_query_results(self):
        start_time = datetime.datetime.utcnow()
        while True:
            try:
                status = self._get_query_status()
            except (ssl.SSLError, Exception, AttributeError, HttpError,
                    httplib2.ServerNotFoundError) as caught_error:
                logger.warn(
                    'Encountered error (%s) monitoring for query response could'
                    ' be temporary, not bailing out.', caught_error)
                return None
            if status is not None:
                time_waiting = int((datetime.datetime.utcnow() -
                                    start_time).total_seconds())
                if status == 'RUNNING':
                    logger.info(
                        'Waiting for query to complete, spent %d seconds so '
                        'far.', time_waiting)
                    time.sleep(5)
                elif status == 'PENDING':
                    logger.info(
                        'Waiting for query to submit, spent %d seconds so '
                        'far.', time_waiting)
                    time.sleep(10)
                elif status == 'DONE':
                    logger.info('Query status complete.')
                    return self._collect_query_results()
                    break
                else:
                    raise Exception('UnknownBigQueryResponse')
        return None

    def _collect_query_results(self):
        try:
            response = self._job.getQueryResults(projectId=self._project_id, jobId=self._job_id).execute()
            response_tuple = (str(response['schema']['fields'][0]['name']), int(response['rows'][0]['f'][0]['v']))
            logger.info('Received query results: %s', str(response_tuple))
            return response_tuple
        except HttpError as e:
            if e.resp.status == 404:
                raise TableDoesNotExist()
            elif e.resp.status == 400:
                raise BigQueryJobFailure(e.resp.status, e)
            else:
                raise BigQueryCommunicationError(
                    'Failed to communicate with BigQuery', e)
        except Exception as e:
             raise Exception('UnknownBigQueryResponse')


