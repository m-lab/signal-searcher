package sequencer

import (
	"encoding/binary"
	"math"
	"sort"
	"strconv"

	"cloud.google.com/go/bigtable"
)

type Meta struct {
	ASN, Loc string
}

type Datum struct {
	Count    int
	Download float64
}

type Sequence struct {
	Key Meta
	Seq map[string]Datum
}

func (s *Sequence) SortedSlices() ([]string, []Datum) {
	var dates []string
	var data []Datum
	for d := range s.Seq {
		dates = append(dates, d)
	}
	sort.Strings(dates)
	for _, d := range dates {
		data = append(data, s.Seq[d])
	}
	return dates, data
}

type Sequencer struct {
	currentSequence *Sequence
	output          chan<- *Sequence
}

func New() (*Sequencer, <-chan *Sequence) {
	c := make(chan *Sequence, 100)
	return &Sequencer{output: c}, c
}

func getMeta(ris []bigtable.ReadItem) (meta Meta, date string) {
	for _, ri := range ris {
		switch ri.Column {
		case "meta:client_asn_number":
			meta.ASN = string(ri.Value)
		case "meta:client_location_key":
			meta.Loc = string(ri.Value)
		case "meta:date":
			date = string(ri.Value)
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

func (s *Sequencer) ProcessRow(r bigtable.Row) bool {
	key, date := getMeta(r["meta"])
	if s.currentSequence != nil && s.currentSequence.Key != key {
		s.output <- s.currentSequence
		s.currentSequence = nil
	}
	if s.currentSequence == nil {
		s.currentSequence = &Sequence{
			Key: key,
			Seq: make(map[string]Datum),
		}
	}
	s.currentSequence.Seq[date] = getData(r["data"])
	return true
}

func (s *Sequencer) Done() {
	if s.currentSequence != nil {
		s.output <- s.currentSequence
	}
	close(s.output)
}
