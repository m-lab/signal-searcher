# Requirements

**Signal Searcher attempts to find interesting and important signals in MLab's Internet
measurement data using computers instead of people.**

[Measurement Lab](mlab) is collecting lots and lots of measurement data from all over the Internet. The project is drowning in data, and too frequently has to work too hard to acquire any of data's derivatives: information, knowledge, and wisdom.  In the past, issues were found because there were operator rumors of bad behavior. Depending on intensive investigations by humans spurred by rumor-mill dross is not a sustainable solution. As a process it is too random to get started reliably, and for its execution it depends on a very scarce resource (researcher time).

Signal Searcher (this repo) is an attempt to create an automated solution that will permit the display and discovery of issues relating to end-to-end Internet performance and aid in Internet performance transparency.  Once performance and measurement has been made transparent, network management can proceed from a firm footing based on open and auditable data. Signal searcher tries to help bootstrap this process by automatically mining the data.

Let's break down Signal Searcher's mission for our data:

> Signal Searcher attempts to find interesting and important signals in MLab's
> Internet measurement data using computers instead of people.

## Interesting

A signal is interesting if:

1. It has patterns that indicate overload
2. It gets worse over time
3. It changes differently than its cohort
4. It is out of control
5. It is surprising

## Important

A signal is important if:

1. It corresponds to an attribute that directly affects a user's Internet
   experience, e.g. latency, download throughput, and upload throughput.

and either

2. It has a large effect on part of the Internet OR
3. It affects a large part of the Internet

## Signal

Each signal is a time-series of measured values. Signals may be (and almost
certainly are) aggregates of time-series data. Let us imagine each value from a
test as a single row in a table of measured values. Each value has lots of
attributes for the test environment and for the measured values that were
recorded. In data mining there is the concept of a [data cube](datacube): a
hypercube where each dimension corresponds to a piece of the key specifying a
data value. To create a signal, you must specify an operation for every
key dimension in the data cube.  That is, for every dimension you must
"transform" (apply an operation to each data value that e.g. aligns timestamps,
transforms IPs to ASNs, or buckets data), "roll up" (combine all data along the
given dimension using some operation like "average" or "median"), or "drill
down" (specify a particular value or range of values for that particular
dimension). For metadata dimensions (dimensions that are neither part of the
key or the value under test), there is another operator: "drop", which means to
ignore that dimension entirely. Once every dimension has an operation
specified, the result will be a table of values. If the operations are
specified properly for our purposes, then the result will be a signal: a
one-dimensional sequence of values.

## MLab's Internet Measurement Data

Our data is network measurement data, collected at the M-Lab measurement points
across the Internet, and stored in [BigQuery](bigquery). The two main
categories of data are measurements of performance and measurements of
topology. Each test is associated with a litany of metadata (although we would
like more metadata in BigQuery) regarding the environment in which the test was
conducted. We will need to be able to join topology and performance metrics if
we would like to perform any sort of tomography about where in the network any
observed problems might lie. Because each performance data test is followed by
a traceroute, the topology data can be considered metadata of the performance
data. However, the topology data is also interesting in its own right and may
also be studied independently of the performance data. 

For our first version, we will restrict ourselves to studying just sources and
destinations of traffic, leaving more complex topology-sensitive analyses for
the future.  In the first version we will also restrict ourselves to just the
three main measurements of interest for end-to-end user experience on the
Internet: Download speed, upload speed, and RTT.

A sample query to get this out of MLab's BigQuery tables might be:
```sql
SELECT
  web100_log_entry.connection_spec.remote_ip AS server_ip_v4,
  web100_log_entry.connection_spec.remote_ip AS client_ip_v4,
  8 * (web100_log_entry.snap.HCThruOctetsAcked /
        (web100_log_entry.snap.SndLimTimeRwin +
         web100_log_entry.snap.SndLimTimeCwnd +
         web100_log_entry.snap.SndLimTimeSnd)) AS download_mbps,
  (web100_log_entry.snap.SumRTT /
     web100_log_entry.snap.CountRTT) AS avg_rtt,
  web100_log_entry.snap.MinRTT AS min_rtt,
  (web100_log_entry.snap.SegsRetrans /
     web100_log_entry.snap.DataSegsOut) AS packet_retransmit_rate
FROM
  [plx.google:m_lab.ndt.all]
WHERE
  web100_log_entry.snap.SndLimTimeSnd IS NOT NULL
  AND web100_log_entry.snap.SndLimTimeCwnd IS NOT NULL
  AND web100_log_entry.snap.SndLimTimeRwin IS NOT NULL
  AND project = 0
  AND web100_log_entry.is_last_entry = True
  AND connection_spec.data_direction = 1
  AND web100_log_entry.snap.CongSignals > 0
  AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
  AND (web100_log_entry.snap.State == 1
    OR (web100_log_entry.snap.State >= 5
        AND web100_log_entry.snap.State <= 11))
  AND (web100_log_entry.snap.SndLimTimeRwin +
       web100_log_entry.snap.SndLimTimeCwnd +
       web100_log_entry.snap.SndLimTimeSnd) >= 9000000
  AND (web100_log_entry.snap.SndLimTimeRwin +
       web100_log_entry.snap.SndLimTimeCwnd +
       web100_log_entry.snap.SndLimTimeSnd) < 3600000000
  AND web100_log_entry.snap.CountRTT > 10
```

## Using computers

We specify a signal by a series of transforms, roll-ups, and drill downs. So
the space we are searching for interesting signals is the set of all possible
transforms, roll-ups, and drill-downs applied to each dimension in our data
set. The size of this search space grows exponentially with the number of
dimensions a measurement has ("the curse of dimensionality") and
multiplicatively with the number of values in each dimension.  Even with
computational help, this is too large a space to use brute force. Instead, we
will specify roll-ups and drill-downs of interest and then use the derived time
series data.

To cover the project's remit of general search, we will train an ML
model on our data and then run any new data through the model to see if the new
results are mispredicted (and therefore "surprising", which can be thought of
as "potentially interesting").

# Design

We use this insight to build a system which goes through our database
looking for data streams of interest that violate at least one of
these principles.  Because of our focus on end-to-end user experience,
we restrict ourselves to searching for signals in metrics which affect
the end-user experience: Download speed, upload speed, and RTT.

Expanding on the initial super-generic statement, we come up with the
following assertions about the properties a healthy and open Internet
should have:

1. Performance for a given ISP or ISP pair should not vary diurnally.
   Diurnal performance variation was discovered to be a leading indicator
   of network congestion problems.

2. Performance should not get worse over time. If we ever find
   performance to be degrading, then that is an interesting signal that
   we should investigate.

3. For a given access ISP, performance should be the same to each
   content ISP in a given metro. If, for access ISP X and content ISPs Y
   and Z, the expected (X,Y) performance is statistically significantly
   different from the (X,Z) performance, then we have discovered an
   Internet bottleneck outside of the last mile in X!

4. Performance measurements should be under statistical control.  If
   the variation in expected performance suddenly spikes, then that is
   an important signal worth investigating.

5. If we have a trained model of past performance, then it should be
   able to predict current performance with high accuracy. If the
   predictions of our model are very wrong, then it is likely that we
   are seeing something new happening on the Internet.

In all of the above, we use the word "performance" and we don't say
whether it should go up or down, because RTT should go down, and bandwidth
should go up, but either way we can still say that performance should
not get worse.

# Signal searcher

Signal searcher will operate in batch mode. The time period between batch
runs will be determined by the runtime of the system - if the batch job
takes days to complete, then it should be run less frequently than if
it only takes minutes.  As command line parameters, signal searcher will
take in the IP ranges of interest and the time period of interest.

The output of signal searcher will be a performance report describing
any observed problems in that specified subset of the data. The report
will clearly have to go through multiple UX iterations, but it will
initially be designed to output graphs similar to those in [the M-Lab
interconnection report][interconnection].

In the explanations below, whenever we specify a time-series, what we mean is
the raw data, bucketed appropriately, with the median operator applied to each
bucket.

## 24-hour cycles

One of the strongest findings in [the report][interconnection] was that
diurnal performance was a strong indicator of network overload. Therefore,
our first metric of interest will be to load the time-varying signal
of a single ISP's performance and to analyze to see if it has a strong
24-hour cycle.

We will do this by performing a Fourier transform of each time series of
performance and then squaring the absolute value of the result to get a power
curve.  We then look in this curve to see if it has a bump at the peak
corresponding to a 24 hour frequency.

## Non-degrading performance

Performance should not get worse over time. If we discover an instance where
performance does seem to get worse over time, then we should investigate.

## Equivalent performance within a metro area

If we discover that the same access ISP has significantly different performance
to different content ISPs within a metro area, then that is strong evidence
that in at least some instances the user's experienced performance is being
affected by a bottleneck that is not part of their last mile.

We can test equivalence using the Kolmogorov-Smirnov test which will estimate
how different two distributions might be. For the time period of interest, we
query to find the distribution of our data and then use the K-S test to
determine how likely it is that the two distributions are the same.

## Statistical control

Use process control theory to evaluate whether the fluctuations of our
time series represent standard levels of randomness around the same
statistical mean or whether they represent the system transitioning to a new,
different, "out of control" state.

This depends on a currently-unpublished paper, and so is blocked until that
work is published.

## Machine learning to find surprises

The state of the art machine learning system is Google's TensorFlow. So, we
should build a TensorFlow system, train and validate it on our data according
to standard best practices, and then ensure that our data set of interest does
not hold any surprises for the trained system. In this context, surprises for
our ML system would imply that some aspect of the data has changed in an
important way.

(N.B. TensorFlow is only available in C++ or Python flavors, so this means
that the project as a whole needs to be written in C++ or written in Python or
to swim upstream. Python it is.)

# Signal searcher's reports

The resulting reports from Signal Searcher should be emitted in priority order.
An item of interest can be of high priority because it affects a large portion
of the Internet, or because it strongly effects a part of the Internet. An item
which does both would then be of the highest priority.  The relationship
between size and severity in determining priority can only be determined
empirically, so we leave the exact ranking function unspecified until we have
several unordered reports to provide us a basis from which to reason.

The format of the reports is unspecified, but should at a minimum contain
graphs of the signals we claim are interesting and an indication of why they
are interesting.

# Implementation

We will begin by using BigQuery resources wastefully. For every site, write a
query to get every NDT result to that site.  Then, run the resulting time
series through each of: The 24-hour tests, the non-degradation test, and the
statistical control test.

Once those three tests are implemented, we should look for equivalent
performance in a metro area by using the GeoIP of each test as well as the
origin AS for each test.  Finally, we will train a TensorFlow model to predict
performance and then run each new measurement through that same model.

The suggested implementation order is:

- [ ] Querying to get a single time series.
- [x] 24-hour cycles
- [x] Initial output reports
- [ ] Performance non-degradation
- [ ] ~~Statistical control~~ (optional step, blocked until publication)
- [ ] Improved output reports
- [ ] Querying to get a performance distribution
- [ ] Equivalent performance within a metro
- [ ] Querying to get everything
- [ ] TensorFlow prediction model
- [ ] Cleaned up output reports

Note that once querying to get a time-series works, implementing the
time-series tests can proceed in parallel.  Also, output reports should be
constructed immediately once we have a single test working, and then should be
refactored once we have multiple tests working, and finally should be cleaned
up and made into something we are proud of once all tests are working.

# Future work

- Apply the tools of granger causation in an attempt to discover what
  patterns end up being leading indicators for these metrics.

- Online signal searcher, which informs of Internet measurement changes in
  real-time by evaluating each incoming measurement and deciding whether
  or not to fire an alert.

- See what other metrics in our data set may be informative. Web100
  has a lot of variables that are largely unexamined; maybe one of them
  holds the key to understanding Internet performance in the large.

- Use paris-traceroute path information to build up more in-depth
  analyses which attempt to localize performance problems in the network.
  In particular, each measurement can be read as an attestation of the
  minimum performance available to each link on the path. By combining
  these measurements we should be able to discover what links in
  poorly-performing paths do not have another measurement attesting to
  their quality.


[interconnection]: https://www.measurementlab.net/blog/2014_interconnection_report/ "ISP Interconnection and its Impact on Consumer Internet Performance"
[mlab]: //www.measurementlab.net
[datacube]: https://en.wikipedia.org/wiki/OLAP_cube
[bigquery]: https://bigquery.cloud.google.com
