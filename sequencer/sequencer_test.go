package sequencer

import (
	"testing"
	"time"

	"github.com/go-test/deep"

	"cloud.google.com/go/bigtable"
)

func Test_ProcessRow(t *testing.T) {
	rows := []bigtable.Row{
		{
			"meta": []bigtable.ReadItem{
				{Column: "meta:client_asn_number", Value: []byte("AS1")},
				{Column: "meta:client_location_key", Value: []byte("naus")},
				{Column: "meta:date", Value: []byte("2009-01")},
			},
			"data": []bigtable.ReadItem{
				{Column: "data:count", Value: []byte("2")},
				{Column: "data:download_speed_mbps_median", Value: []byte("@k\xe4\xe1\xa4\x05\xa7\xd5")},
			},
		},
		{
			"meta": []bigtable.ReadItem{
				{Column: "meta:client_asn_number", Value: []byte("AS1")},
				{Column: "meta:client_location_key", Value: []byte("naus")},
				{Column: "meta:date", Value: []byte("2009-02")},
			},
			"data": []bigtable.ReadItem{
				{Column: "data:count", Value: []byte("2")},
				{Column: "data:download_speed_mbps_median", Value: []byte("@k\xe4\xe1\xa4\x05\xa7\xd5")},
			},
		},
		{
			"meta": []bigtable.ReadItem{
				{Column: "meta:client_asn_number", Value: []byte("AS2")},
				{Column: "meta:client_location_key", Value: []byte("naus")},
				{Column: "meta:date", Value: []byte("2009-02")},
			},
			"data": []bigtable.ReadItem{
				{Column: "data:count", Value: []byte("2")},
				{Column: "data:download_speed_mbps_median", Value: []byte("@k\xe4\xe1\xa4\x05\xa7\xd5")},
			},
		},
	}
	s, c := New()
	go func() {
		for _, r := range rows {
			s.ProcessRow(r)
		}
		s.Done()
	}()
	s1 := <-c
	if len(s1.Seq) != 2 {
		t.Error("Returned sequence is not of length 2:", s1)
	}
	if s1.Key.ASN != "AS1" {
		t.Error("First sequence should be for AS1, not", s1.Key.ASN)
	}
	s2 := <-c
	if len(s2.Seq) != 1 {
		t.Error("Second sequence should be of length 1")
	}
	if s2.Key.ASN != "AS2" {
		t.Error("First sequence should be for AS2, not", s2.Key.ASN)
	}
	_, ok := <-c
	if ok {
		t.Error("Channel was supposed to be closed, but somehow returned ok =", ok)
	}
}

func TestSequence_SortedSlices(t *testing.T) {
	s := &Sequence{
		Key: Meta{ASN: "AS1", Loc: "naus"},
		Seq: map[time.Time]Datum{
			time.Date(2009, 2, 1, 0, 0, 0, 0, time.UTC): {Count: 3, Download: 4},
			time.Date(2009, 1, 1, 0, 0, 0, 0, time.UTC): {Count: 1, Download: 2},
		},
	}
	dates, data := s.SortedSlices()
	if len(dates) != len(data) {
		t.Error(dates, "and", data, "should be of the same length")
	}
	if dates[0] != time.Date(2009, 1, 1, 0, 0, 0, 0, time.UTC) && dates[1] != time.Date(2009, 1, 1, 0, 0, 0, 0, time.UTC) {
		t.Error("Bad dates")
	}
	if diffs := deep.Equal(data, []Datum{{Count: 1, Download: 2}, {Count: 3, Download: 4}}); diffs != nil {
		t.Error("Bad data:", diffs)
	}
}
