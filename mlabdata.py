import collections

MlabDataEntry = collections.namedtuple(
    'MlabDataEntry', ['time', 'upload_speed', 'download_speed', 'min_latency'])
