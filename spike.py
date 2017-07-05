import collections
import datetime
import logging
import struct
import sys

import grpc
import numpy

# pylint: disable=no-name-in-module
from google.cloud import bigtable
# pylint: enable=no-name-in-module

InternetData = collections.namedtuple(
    'InternetData',
    ['key', 'table', 'time', 'upload', 'download', 'rtt', 'samples'])

IntermediateProblem = collections.namedtuple(
    'IntermediateProblem', ['start', 'end', 'metric', 'severity'])

Problem = collections.namedtuple('Problem', [
    'key', 'table', 'start_date', 'end_date', 'metric', 'severity', 'test_count'
])


def _find_slope(timeseries):
    """Slope of the fit of the timeseries to a linear data model."""
    # Fit the data to a line (a.k.a. a polynomial of degree 1)
    slope, _ = numpy.polyfit(list(range(len(timeseries))), timeseries, 1)
    return slope


def _bad_slope(slope, metric):
    """Returns True if the slope indicates a degradation for the metric."""
    if metric == 'rtt':
        return slope > 0  # Latency climbing over time is bad
    elif metric in ('download', 'upload'):
        return slope < 0  # Declining throughput over time is bad
    else:
        raise ValueError('Unknown metric: ' + metric)


def connect_to_table(name, start_key=None):
    client = bigtable.Client(project='mlab-oti', admin=False)
    # TODO: verify that the permissions on this table are correct
    instance = client.instance('mlab-data-viz-prod')
    table = instance.table(name)
    return table.read_rows(start_key=start_key)


def parse_key_and_data(key, data, table_name):
    for required in ['data:download_speed_mbps_median',
                     'data:upload_speed_mbps_median', 'data:rtt_avg',
                     'data:count']:
        if required not in data:
            logging.debug('no %s entry for %s', required, key)
            return None, None

    if key.count('|') < 1:
        logging.warning('bad key "%s"', key)
        return None, None
    key_no_date, date = key.rsplit('|', 1)
    if date.count('-') != 2:
        logging.warning('bad date in key "%s"', key)
        return None, None
    year, month, day = date.split('-')
    date = datetime.date(int(year, 10), int(month, 10), int(day, 10))
    download = data['data:download_speed_mbps_median'][0].value
    download = struct.unpack('>d', download)[0]
    upload = data['data:upload_speed_mbps_median'][0].value
    upload = struct.unpack('>d', upload)[0]
    rtt = data['data:rtt_avg'][0].value
    rtt = struct.unpack('>d', rtt)[0]
    samples = int(data['data:count'][0].value)
    return key_no_date, InternetData(key=key_no_date, table=table_name,
            time=date, download=download, upload=upload, rtt=rtt,
            samples=samples)


def read_each_stream(table_name):
    response = connect_to_table(table_name)
    data = []
    key_no_date = None
    try:
        while True:
            try:
                response.consume_next()
            except grpc._channel._Rendezvous as err:
                logging.warning('Could no consume_next: %s, assuming error '
                                'was transient and reconnecting.', err)
                response = connect_to_table(table_name, key_no_date)
                data = []
                key_no_date = None
                continue
            for key in sorted(response.rows):
                row = response.rows[key].to_dict()
                new_key, entry = parse_key_and_data(key, row, table_name)
                if new_key is None or entry is None:
                    continue
                if new_key != key_no_date:
                    yield key_no_date, data
                    key_no_date = new_key
                    data = []
                data.append(entry)

            response.rows.clear()
    except StopIteration:
        pass
    yield key_no_date, data


SEVERE, BAD, MINOR = (5, 1, 0)


def percent_change_to_severity(percent, metric):
    percent = abs(percent)
    if metric in ('upload', 'download'):
        if percent > 100:
            return SEVERE
        elif percent > 10:
            return BAD
        else:
            return MINOR
    elif metric == 'rtt':
        if percent > 20:
            return 5
        elif percent > 10:
            return 1
        else:
            return 0
    else:
        print "Unknown metric", metric


def problem_to_url(problem):
    # http://viz.measurementlab.net/compare/clientIsp?end=2016-07-12&selected=AS13367x&start=2016-06-15
    args = {
        'aggr': 'month',
        'start': problem.start_date,
        'end': problem.end_date,
        'selected': problem.key,
        'metric': problem.metric
    }
    args['selected'] = problem.key
    if problem.table == 'client_asn_by_day':
        comparison = 'clientIsp'
    elif problem.table == 'client_loc_by_day':
        comparison = 'location'
    elif problem.table == 'client_asn_client_loc_by_day':
        comparison = 'location'
        asn, loc = problem.key.split('|')
        args['filter1'] = asn.strip()
        args['selected'] = loc.strip()
    else:
        assert False, 'Bad table: ' + problem.table

    return ('http://viz.measurementlab.net/compare/%s?' % comparison) + \
            ('&'.join(str(k) + '=' + str(v) for (k, v) in args.items()))


def find_slope_problems(single_metric_series, metric):
    for i in range(len(single_metric_series) - 365):
        chunk = single_metric_series[i:i + 365]
        slope = _find_slope(chunk)
        if _bad_slope(slope, metric):
            avg = numpy.mean(chunk)
            expected_loss = 365 * slope
            expected_percent_loss = expected_loss * 100.0 / avg
            severity = percent_change_to_severity(expected_percent_loss, metric)
            if severity == SEVERE:
                yield IntermediateProblem(i, i + 365, metric, severity)


ONEYEAR = 365
TWOYEARS = 2 * ONEYEAR

def find_year_over_year_problems(single_metric_series, metric):
    previous_year = sum(single_metric_series[:ONEYEAR])
    current_year = sum(single_metric_series[ONEYEAR:TWOYEARS])
    for i in range(TWOYEARS, len(single_metric_series)):
        if float(current_year) / float(previous_year) < .75:
            severity = float(current_year) / float(previous_year)
            yield IntermediateProblem(i - TWOYEARS, i, metric, severity)
        assert previous_year > 0
        assert current_year > 0
        previous_year -= single_metric_series[i - TWOYEARS]
        previous_year += single_metric_series[i - ONEYEAR]
        current_year -= single_metric_series[i - ONEYEAR]
        current_year += single_metric_series[i]


def analyze_stream(key, data, method=find_slope_problems):
    if not data:
        return
    problems_found = []
    for metric, _metric_name in [
            ('download', 'Download speeds'),
            #('upload', 'Upload speeds'),
            #('rtt', 'Round-trip times'),
            ]:
        field_index = data[0]._fields.index(metric)
        assert field_index >= 0, 'Bad metric name %s' % metric
        single_metric_series = [d[field_index] for d in data]
        problems_found.extend(method(single_metric_series, metric))

    if not problems_found:
        return

    coalesced_problems = []
    current_problems = [problems_found[0]]
    for problem in problems_found[1:]:
        if problem.start <= current_problems[-1].end and \
                current_problems[-1].metric == problem.metric:
            current_problems.append(problem)
        else:
            coalesced_problems.append(current_problems)
            current_problems = [problem]
    coalesced_problems.append(current_problems)

    actual_problems = []
    for problem_list in coalesced_problems:
        start = problem_list[0].start
        end = problem_list[-1].end
        count = sum(x.samples for x in data[start:end])
        severity = sum(x.severity
                       for x in problem_list) / float(len(problem_list))
        actual_problems.append(
            Problem(key.strip(), data[start].table, data[start].time, data[end]
                    .time, problem_list[0].metric, severity, count))
    actual_problems = list(
        sorted(
            actual_problems, key=lambda x: x.severity * x.test_count))

    for problem in actual_problems:
        print problem.severity, problem.test_count, problem_to_url(problem)


def main(_args):
    # Read each timestream
    for table in (
            #'client_asn_by_day',
            'client_asn_client_loc_by_day',
            #'client_loc_by_day',
            #'server_asn_by_day',
            #'server_asn_client_asn_by_day',
            #'server_asn_client_asn_client_loc_by_day',
            #'server_asn_client_loc_by_day',
    ):
        for key, data in read_each_stream(table):
            analyze_stream(key, data, find_year_over_year_problems)


if __name__ == '__main__':
    main(sys.argv)
