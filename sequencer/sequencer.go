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

type Data struct {
	Count    int
	Download float64
}

type Sequence struct {
	Key Meta
	Seq map[string]Data
}

func (s *Sequence) SortedSlices() ([]string, []Data) {
	var keys []string
	var values []Data
	for k := range s.Seq {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		values = append(values, s.Seq[k])
	}
	return keys, values
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

func getData(ris []bigtable.ReadItem) (data Data) {
	for _, ri := range ris {
		switch ri.Column {
		case "data:download_speed_mbps_median":
			bits := binary.BigEndian.Uint64(ri.Value)
			data.Download = math.Float64frombits(bits)
		case "data:count":
			data.Count, _ = strconv.Atoi(string(ri.Value)) // Ignore parsing error
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
			Seq: make(map[string]Data),
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
