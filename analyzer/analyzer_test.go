package analyzer

import (
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

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
			input: []arrayIncident{{1, 2, 0.3, 0.2, 0.14}},
			want:  []arrayIncident{{1, 2, 0.3, 0.2, 0.14}},
		},
		{
			name:  "Merged unmerged merged",
			input: []arrayIncident{{1, 3, 0.3, 0.2, 0.14}, {2, 4, 0.5, 0.2, 0.14}, {4, 9, 0.4, 0.1, 0.06}, {11, 15, 0.8, 0.1, 0.08}, {12, 16, 0.6, 0.08, 0.032}, {13, 17, 0.6, 0.08, 0.032}},
			want:  []arrayIncident{{1, 4, 0.5, 0.2, 0.14}, {4, 9, 0.4, 0.1, 0.06}, {11, 17, 0.8, 0.1, 0.044}},
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
	flatSequence := map[time.Time]sequencer.Datum{}
	oneBadYear := map[time.Time]sequencer.Datum{}
	badYearDownload := 68.0
	goodDownload := 100.0
	for year := 2009; year <= 2020; year++ {
		for month := 1; month <= 12; month++ {
			d := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
			flatSequence[d] = sequencer.Datum{
				Download: goodDownload,
				Count:    1,
			}
			if year == 2012 {
				oneBadYear[d] = sequencer.Datum{
					Download: badYearDownload,
					Count:    1,
				}
			} else {
				oneBadYear[d] = sequencer.Datum{
					Download: goodDownload,
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
			want: []Incident{{
				Start:         time.Date(2011, 12, 1, 0, 0, 0, 0, time.UTC),
				End:           time.Date(2012, 12, 1, 0, 0, 0, 0, time.UTC),
				Severity:      1.0 - (badYearDownload / goodDownload),
				AffectedCount: 12,
			}},
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
		Start:         time.Date(2013, 5, 1, 0, 0, 0, 0, time.UTC),
		End:           time.Date(2014, 4, 1, 0, 0, 0, 0, time.UTC),
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
