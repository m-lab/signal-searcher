"""All of the datatypes that get passed around inside Signal Searcher."""

import collections

InternetData = collections.namedtuple(
    'InternetData',
    ['key', 'table', 'time', 'upload', 'download', 'rtt', 'samples'])

class Problem(
        collections.namedtuple(
            'Problem',
            ['key', 'table', 'start_date', 'end_date', 'severity', 'test_count',
             'description'])):
    """A class to hold a single discovered problem in M-Lab data."""

    def to_url(self):
        """Converts a problem into an M-Lab viz URL."""
        return 'http://127.0.0.1'

# Everything below this line is temporary and should be deleted when the
# migration away from the spike is complete.

# pylint disable=missing-docstring, no-self-use

# Deprecated --- DO NOT USE
MlabDataEntry = collections.namedtuple(
    'MlabDataEntry', ['time', 'upload_speed', 'download_speed', 'min_latency'])

