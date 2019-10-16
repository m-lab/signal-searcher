package main

import (
	"context"
	"flag"
	"fmt"

	"cloud.google.com/go/bigtable"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/signal-searcher/analyzer"
	"github.com/m-lab/signal-searcher/sequencer"
)

var (
	project  = flag.String("project", "mlab-oti", "The name of the cloud project the bigtables are in")
	instance = flag.String("instance", "viz-pipeline", "The name of the cloud bigtable instance to use")

	mainCtx, mainCancel = context.WithCancel(context.Background())

	// A variable to allow injection of a fake bigtable client for testing.
	bigtableNewClient = bigtable.NewClient
)

// This takes monthly download data and discovers instances of year-long
// sustained drops in Internet performance. It is currently hardcoded to only
// work with monthly data. It likely has lots of other issues too. Nevertheless,
// it finds 17,000 results, which is a good start.

func main() {
	defer mainCancel()
	flag.Parse()

	client, err := bigtableNewClient(mainCtx, *project, *instance)
	rtx.Must(err, "Could not connect to bigtable")
	table := client.Open("client_asn_client_loc_by_month")
	s, c := sequencer.New()
	go func() {
		err = table.ReadRows(mainCtx, bigtable.InfiniteRange(""), s.ProcessRow)
		rtx.Must(err, "Could not read table")
		s.Done()
	}()

	fmt.Println("TestsAffected, AS, LocationCode, Start, End, URL")
	for seq := range c {
		if len(seq.Seq) <= 24 {
			continue
		}

		for _, incident := range analyzer.FindPerformanceDrops(seq) {
			fmt.Printf(
				"%d, %s, %s, %s, %s, %s\n",
				incident.AffectedCount, seq.Key.ASN, seq.Key.Loc, incident.Start.Format("2006-01-02"), incident.End.Format("2006-01-02"), incident.URL(seq.Key))
		}
	}
}
