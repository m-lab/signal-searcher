package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigtable"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/signal-searcher/analyzer"
	"github.com/m-lab/signal-searcher/sequencer"
)

var (
	mainCtx, mainCancel = context.WithCancel(context.Background())
)

func main() {
	client, err := bigtable.NewClient(mainCtx, "mlab-oti", "viz-pipeline")
	rtx.Must(err, "Could not connect to bigtable")
	table := client.Open("client_asn_client_loc_by_month")
	s, c := sequencer.New()
	go func() {
		err = table.ReadRows(mainCtx, bigtable.InfiniteRange(""), s.ProcessRow)
		rtx.Must(err, "Could not read table")
		s.Done()
	}()

	fmt.Println("TestsAffected, AS, LocationCode, StartDate, EndDate, URL")
	for seq := range c {
		if len(seq.Seq) <= 24 {
			continue
		}

		for _, incident := range analyzer.FindPerformanceDrops(seq) {
			fmt.Printf(
				"%d, %s, %s, %s, %s, %s\n",
				incident.AffectedCount, seq.Key.ASN, seq.Key.Loc, incident.StartDate, incident.EndDate, incident.URL(seq.Key))
		}
	}
}
