package analyzer

import (
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/go-test/deep"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/signal-searcher/sequencer"
)

func Test_mergeArrayIncidents(t *testing.T) {
	type args struct {
	}
	tests := []struct {
		name  string
		input []arrayIncident
		want  []arrayIncident
	}{
		{
			name:  "Empty is okay",
			input: []arrayIncident{},
			want:  []arrayIncident{},
		},
		{
			name:  "One is okay",
			input: []arrayIncident{{1, 2}},
			want:  []arrayIncident{{1, 2}},
		},
		{
			name:  "Merged unmerged merged",
			input: []arrayIncident{{1, 3}, {2, 4}, {4, 9}, {11, 15}, {12, 16}, {13, 17}},
			want:  []arrayIncident{{1, 4}, {4, 9}, {11, 17}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mergeArrayIncidents(tt.input); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mergeArrayIncidents() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFindPerformanceDrops(t *testing.T) {
	flatSequence := map[string]sequencer.Datum{}
	oneBadYear := map[string]sequencer.Datum{}
	for year := 2009; year <= 2020; year++ {
		for month := 1; month <= 12; month++ {
			d := fmt.Sprintf("%d-%02d", year, month)
			flatSequence[d] = sequencer.Datum{
				Download: 100,
				Count:    1,
			}
			if year == 2012 {
				oneBadYear[d] = sequencer.Datum{
					Download: 68,
					Count:    1,
				}
			} else {
				oneBadYear[d] = sequencer.Datum{
					Download: 100,
					Count:    1,
				}
			}
		}
	}

	tests := []struct {
		name  string
		input *sequencer.Sequence
		want  []Incident
	}{
		{
			name:  "Everything is fine",
			input: &sequencer.Sequence{Seq: flatSequence},
			want:  []Incident{},
		},
		{
			name:  "One bad year",
			input: &sequencer.Sequence{Seq: oneBadYear},
			want:  []Incident{{StartDate: "2011-12", EndDate: "2012-12", AffectedCount: 12}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FindPerformanceDrops(tt.input)
			diff := deep.Equal(got, tt.want)
			if diff != nil {
				t.Errorf("FindPerformanceDrops() = %v, want %v, diff=%q", got, tt.want, diff)
			}
		})
	}
}

func TestIncident_URL(t *testing.T) {
	inc := Incident{
		StartDate:     "2013-05",
		EndDate:       "2014-04",
		AffectedCount: 1200,
	}
	meta := sequencer.Meta{
		ASN: "AS1",
		Loc: "ascn",
	}
	u := inc.URL(meta)
	pu, err := url.Parse(u)
	rtx.Must(err, "Bad url produced")
	if !strings.HasSuffix(pu.Path, "/ascn") {
		t.Error("Bad url path", pu)
	}
	values := pu.Query()
	if diff := deep.Equal(values, url.Values{
		"aggr":  {"month"},
		"isps":  {"AS1"},
		"start": {"2012-05-01"},
		"end":   {"2014-04-01"},
	}); diff != nil {
		t.Error("Bad query string:", diff)
	}
}
