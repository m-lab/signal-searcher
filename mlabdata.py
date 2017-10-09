import collections

InternetData = collections.namedtuple(
    'InternetData',
    ['key', 'table', 'time', 'upload', 'download', 'rtt', 'samples'])


# Deprecated --- DO NOT USE
MlabDataEntry = collections.namedtuple(
    'MlabDataEntry', ['time', 'upload_speed', 'download_speed', 'min_latency'])


ProblemTuple = collections.namedtuple('ProblemTuple', [
    'key', 'table', 'start_date', 'end_date', 'severity', 'test_count',
    'description'
])


class Problem(ProblemTuple):
    def to_url(self):
        return 'http://127.0.0.1'
