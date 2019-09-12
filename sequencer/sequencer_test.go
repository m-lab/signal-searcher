package sequencer

import (
	"strings"
	"testing"

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
	s2 := <-c
	if len(s2.Seq) != 1 {
		t.Error("Second sequence should be of length 1")
	}
	_, ok := <-c
	if ok {
		t.Error("Channel was supposed to be closed, but somehow returned ok =", ok)
	}
}

func TestSequence_SortedSlices(t *testing.T) {
	s := &Sequence{
		Key: Meta{ASN: "AS1", Loc: "naus"},
		Seq: map[string]Datum{
			"2009-02": {Count: 3, Download: 4},
			"2009-01": {Count: 1, Download: 2},
		},
	}
	dates, data := s.SortedSlices()
	if len(dates) != len(data) {
		t.Error(dates, "and", data, "should be of the same length")
	}
	if strings.Join(dates, " ") != "2009-01 2009-02" {
		t.Error("Bad dates")
	}
}
