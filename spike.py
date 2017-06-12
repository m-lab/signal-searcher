import collections
import datetime
import logging
import struct
import sys

from google.cloud import bigtable

InternetData = collections.namedtuple(
    'InternetData',
    ['key', 'time', 'upload', 'download', 'rtt', 'samples'])

def read_asn(key_no_date):
    start_day = datetime.date(2009, 1, 1)
    key = '%s|%04d-%02d-%02d' % (key_no_date.ljust(10), start_day.year, start_day.month, start_day.day)
    response = table.read_rows(start_key=key)
    data = []
    try:
        key_part = key_no_date
        while key_part == key_no_date:
            response.consume_next()
            for key in sorted(response.rows):
                row = response.rows[key].to_dict()
                key_part, date = key.rsplit('|', 1)
                key_part = key_part.strip()
                if key_part != key_no_date:
                    break
                if 'data:download_speed_mbps_median' in row: 
                    download = row['data:download_speed_mbps_median'][0].value
                    download = struct.unpack('>d', download)[0]
                else:
                    logging.warning('no download data for %s', key)
                    continue
                if 'data:upload_speed_mbps_median' in row: 
                    upload = row['data:upload_speed_mbps_median'][0].value
                    upload = struct.unpack('>d', upload)[0]
                else:
                    logging.warning('no upload data for %s', key)
                    continue
                if 'data:rtt_avg' in row:
                    rtt = row['data:rtt_avg'][0].value
                    rtt = struct.unpack('>d', rtt)[0]
                else:
                    logging.warning('no rtt data for %s', key)
                    continue
                if 'data:count' in row:
                    samples = row['data:count'][0].value
                else:
                    logging.warning('no sample count data for %s', key)
                    continue
                data.append(InternetData(key=key_part, time=date, download=download, upload=upload, rtt=rtt, samples=samples))
            response.rows.clear()
    except StopIteration:
        pass
    return data


def main(args):
    global table
    # Read an AS's data
    client = bigtable.Client(project='mlab-oti', admin=False)
    instance = client.instance('mlab-data-viz-prod')  # Seems like I shouldn't be allowed to do this?
    table = instance.table('client_asn_by_day')
    data = read_asn('AS13367x')
    print data


if __name__ == '__main__':
    main(sys.argv)
