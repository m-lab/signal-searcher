package analyzer

import (
	"fmt"
	"math"
	"time"

	"github.com/m-lab/signal-searcher/sequencer"
)

// An Incident represents a piece of a timeseries that contains a user-visible
// problem.
type Incident struct {
	Start, End         time.Time
	AffectedCount      int
	Severity           float64
	GoodPeriodDownload float64
	BadPeriodDownload  float64
}

// URL converts an incident (along with provided Metadata) into a viz URL.
func (i *Incident) URL(m sequencer.Meta) string {
	twelveBeforeStart := i.Start.AddDate(-1, 0, 0)
	return fmt.Sprintf(
		"https://viz.measurementlab.net/location/%s?aggr=month&isps=%s&start=%s&end=%s",
		m.Loc, m.ASN, twelveBeforeStart.Format("2006-01-02"), i.End.Format("2006-01-02"))
}

type arrayIncident struct {
	start, end         int
	severity           float64
	goodPeriodDownload float64
	badPeriodDownload  float64
}

// severity of merged incident should be the max of the incidents merged
func mergeArrayIncidents(a []arrayIncident) (merged []arrayIncident) {
	// add parameter for when incident starts (i from findPerformanceDrops)
	// add param for sequencer (the data which is s.sortedSlices)
	if len(a) <= 1 {
		return a
	}
	current := a[0]
	// current is going to be earlier than a[i]
	for i := 1; i < len(a); i++ {
		if current.end+1 == a[i].end {
			current.end = a[i].end
			current.severity = math.Max(a[i].severity, current.severity)
			// bad period download speed is the average of the two incidents' bad period download speeds
			current.badPeriodDownload = (a[i].badPeriodDownload + current.badPeriodDownload) / 2

		} else {
			merged = append(merged, current)
			current = a[i]
		}
	}
	merged = append(merged, current)
	return
}

// FindPerformanceDrops discovers time periods of a year or greater where
// performance showed more than a 30% average drop.
func FindPerformanceDrops(s *sequencer.Sequence) []Incident {
	dates, data := s.SortedSlices()
	var previous, current sequencer.Datum
	for i := 0; i < 12; i++ {
		previous.Download += data[i].Download
	}
	for i := 12; i < 24; i++ {
		current.Download += data[i].Download
	}
	var arrayIncidents []arrayIncident
	for i := 24; i < len(data); i++ {
		// Update the running sums
		previous.Download = previous.Download - data[i-24].Download + data[i-12].Download
		current.Download = current.Download - data[i-12].Download + data[i].Download
		if previous.Download*.7 > current.Download {
			arrayIncidents = append(arrayIncidents, arrayIncident{start: i - 12, end: i, severity: 1.0 - current.Download/previous.Download, goodPeriodDownload: previous.Download / 12, badPeriodDownload: current.Download / 12})
		}
	}
	arrayIncidents = mergeArrayIncidents(arrayIncidents)
	incidents := []Incident{}
	for _, ai := range arrayIncidents {
		newIncident := Incident{Start: dates[ai.start], End: dates[ai.end], Severity: ai.severity, GoodPeriodDownload: ai.goodPeriodDownload, BadPeriodDownload: ai.badPeriodDownload}
		for i := ai.start; i < ai.end; i++ {
			newIncident.AffectedCount += data[i].Count
		}
		incidents = append(incidents, newIncident)
	}
	return incidents
}
