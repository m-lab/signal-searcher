package analyzer

import (
	"fmt"
	"time"

	"github.com/m-lab/signal-searcher/sequencer"
)

type Incident struct {
	StartDate, EndDate string
	AffectedCount      int
}

func (i *Incident) URL(m sequencer.Meta) string {
	startDate, _ := time.Parse("2006-01", i.StartDate)
	twelveBeforeStart := startDate.Add(-365 * 24 * time.Hour)
	return fmt.Sprintf(
		"http://viz.measurementlab.net/location/%s?aggr=month&isps=%s&start=%s&end=%s-01",
		m.Loc, m.ASN, twelveBeforeStart.Format("2006-01-02"), i.EndDate)
}

type arrayIncident struct {
	start, end int
}

func mergeArrayIncidents(a []arrayIncident) (merged []arrayIncident) {
	if len(a) <= 1 {
		return a
	}
	current := a[0]
	for i := 1; i < len(a); i++ {
		if current.end+1 == a[i].end {
			current.end = a[i].end
		} else {
			merged = append(merged, current)
			current = a[i]
		}
	}
	merged = append(merged, current)
	return
}

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
			arrayIncidents = append(arrayIncidents, arrayIncident{start: i - 12, end: i})
		}
	}
	arrayIncidents = mergeArrayIncidents(arrayIncidents)
	incidents := []Incident{}
	for _, ai := range arrayIncidents {
		newIncident := Incident{StartDate: dates[ai.start], EndDate: dates[ai.end]}
		for i := ai.start; i < ai.end; i++ {
			newIncident.AffectedCount += data[i].Count
		}
		incidents = append(incidents, newIncident)
	}
	return incidents
}
