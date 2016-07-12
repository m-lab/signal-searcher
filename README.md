Signal searcher's main insight: Performance should not change on a 24
hour cycle, some performance indicators should be the same as others,
performance should not get worse over time, all performance signals
should be in statistical control, and new measurements should not be a
surprise once you have seen all the old measurements.

# Introduction

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
it only takes minutes.  As commandline parameters, signal searcher will
take in the IP ranges of interest and the time period of interest.

The output of signal searcher will be a performance report describing
any observed problems in that specified subset of the data. The report
will clearly have to go through multiple UX iterations, but it will
initally be designed to have graphs similar to those in _The M-Lab
Interconnection Report_.

# Future work

- Apply the tools of granger causation in an attempt to discover what
  patterns end up being leading indicators for these metrics.

- Online signal searcher, which informs of Internet measurement changes in
  real-time by evaluating each incoming measaurement and deciding whether
  or not to fire an alert.

- See what other metrics in our data set may be informative. `web100`
  has a lot of variables that are largely unexamined -- maybe one of
  them holds the key to understanding Internet performance in the large.

- Use paris-traceroute path information to build up more in-depth
  analyses which attempt to localize performance problems in the network.
  In particular, each measurement can be read as an attestation of the
  minimum performance available to each link on the path. By combining
  these measurements we should be able to discover what links in
  poorly-performing paths do not have another measurement attesting to
  their quality.
