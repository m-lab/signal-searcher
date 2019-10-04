package sequencer

import (
	"encoding/binary"
	"math"
	"sort"
	"strconv"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/m-lab/go/rtx"
)

// Meta is the metadata associated with a data sequence. It corresponds to a
// single line that would be draw on a timeseries graph.
type Meta struct {
	ASN, Loc string
}

// Datum represents a single piece of data. Timeseries graphs are constructed
// from one element of this struct.
type Datum struct {
	Count    int
	Download float64
}

// Sequence holds a metadata key and a mapping from date strings to data.
type Sequence struct {
	Key Meta
	Seq map[time.Time]Datum
}

// SortedSlices converts a sequence into parallel arrays of dates and data.
func (s *Sequence) SortedSlices() ([]time.Time, []Datum) {
	var dates []time.Time
	var data []Datum
	for d := range s.Seq {
		dates = append(dates, d)
	}
	sort.Slice(dates, func(i, j int) bool { return dates[i].Before(dates[j]) })
	for _, d := range dates {
		data = append(data, s.Seq[d])
	}
	return dates, data
}

// Sequencer is designed to work with the bigtable API. It takes data a row at a
// time and yields Sequence structs out through a specified channel.
type Sequencer struct {
	currentSequence *Sequence
	output          chan<- *Sequence
}

// New makes a new Sequencer, and returns it as well as the channel along which
// it will write sequences.
func New() (*Sequencer, <-chan *Sequence) {
	c := make(chan *Sequence, 100)
	return &Sequencer{output: c}, c
}

func getMeta(ris []bigtable.ReadItem) (meta Meta, date time.Time) {
	for _, ri := range ris {
		switch ri.Column {
		case "meta:client_asn_number":
			meta.ASN = string(ri.Value)
		case "meta:client_location_key":
			meta.Loc = string(ri.Value)
		case "meta:date":
			var err error
			date, err = time.Parse("2006-01", string(ri.Value))
			rtx.Must(err, "Could not parse %q", string(ri.Value))
		}
	}
	return
}

func getData(ris []bigtable.ReadItem) (datum Datum) {
	for _, ri := range ris {
		switch ri.Column {
		case "data:download_speed_mbps_median":
			bits := binary.BigEndian.Uint64(ri.Value)
			datum.Download = math.Float64frombits(bits)
		case "data:count":
			datum.Count, _ = strconv.Atoi(string(ri.Value)) // Ignore parsing error
		}
	}
	return
}

// ProcessRow processes a single row as required by the bigtable API. It is
// designed to be passed into ReadRows.
func (s *Sequencer) ProcessRow(r bigtable.Row) bool {
	key, date := getMeta(r["meta"])
	if s.currentSequence != nil && s.currentSequence.Key != key {
		s.output <- s.currentSequence
		s.currentSequence = nil
	}
	if s.currentSequence == nil {
		s.currentSequence = &Sequence{
			Key: key,
			Seq: make(map[time.Time]Datum),
		}
	}
	s.currentSequence.Seq[date] = getData(r["data"])
	return true
}

// Done is called after ReadRows is completed. It outputs the last sequence (if
// any) and closes the channel.
func (s *Sequencer) Done() {
	if s.currentSequence != nil {
		s.output <- s.currentSequence
	}
	close(s.output)
}
