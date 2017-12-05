"""All of the datatypes that get passed around inside Signal Searcher."""

import collections
import datetime

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
        args = {
            'aggr': 'month',
            'start': self.start_date,
            'end': self.end_date,
            'selected': self.key,
            'metric': 'download'
        }
        args['selected'] = self.key
        if self.table == 'client_asn_by_day':
            comparison = 'clientIsp'
        elif self.table == 'client_loc_by_day':
            comparison = 'location'
        elif self.table == 'client_asn_client_loc_by_day':
            comparison = 'location'
            asn, loc = self.key.split('|')
            args['filter1'] = asn.strip()
            args['selected'] = loc.strip()
        elif self.table == 'server_asn_client_asn_client_loc_by_day':
            # http://viz.measurementlab.net/compare/location?filter1=AS10796x&filter2=AS174&selected=nausnynewyork
            comparison = 'location'
            loc, asn_client, asn_server = self.key.split('|')
            args['filter1'] = asn_client.strip()
            args['filter2'] = asn_server.strip()
            args['selected'] = loc.strip()
        else:
            assert False, 'Bad table: ' + self.table

        return ('http://viz.measurementlab.net/compare/%s?' % comparison) + \
            ('&'.join(str(k) + '=' + str(v) for (k, v) in args.items()))

    def __str__(self):
        return (super(Problem, self).__str__()[:-1] +
                ', url=\'' + self.to_url() + '\')')

    def dict(self):
        """Returns a dictionary containing the tuple data."""
        return {
            'key': self.key,
            'table': self.table,
            'start_date': _date_to_timestamp(self.start_date),
            'end_date': _date_to_timestamp(self.end_date),
            'severity': self.severity,
            'test_count': self.test_count,
            'description': self.description,
            'url': self.to_url()}


def _date_to_timestamp(orig):
    """Converts a datetime.date into a unix timestamp."""
    return (datetime.datetime(orig.year, orig.month, orig.day, 0, 0, 0) -
            datetime.datetime(1970, 1, 1, 0, 0, 0)
           ).total_seconds()


MlabDataEntry = collections.namedtuple(
    'MlabDataEntry', ['time', 'upload_speed', 'download_speed', 'min_latency'])
