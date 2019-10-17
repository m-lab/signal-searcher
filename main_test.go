package main

import (
	"context"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/bttest"
	"github.com/m-lab/go/rtx"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

func newClientFactory(conn *grpc.ClientConn) func(context.Context, string, string, ...option.ClientOption) (*bigtable.Client, error) {
	return func(ctx context.Context, project, instance string, _ ...option.ClientOption) (*bigtable.Client, error) {
		return bigtable.NewClient(ctx, project, instance, option.WithGRPCConn(conn))
	}
}

func populateFakeDataBT(conn *grpc.ClientConn) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adminClient, err := bigtable.NewAdminClient(ctx, *project, *instance, option.WithGRPCConn(conn))
	rtx.Must(err, "Could not create admin client")
	rtx.Must(adminClient.CreateTable(ctx, "client_asn_client_loc_by_month"), "Could not create table")
	for _, col := range []string{"meta", "data"} {
		rtx.Must(adminClient.CreateColumnFamily(ctx, "client_asn_client_loc_by_month", col), "Could not create %q", col)
	}

	client, err := bigtable.NewClient(ctx, *project, *instance, option.WithGRPCConn(conn))
	rtx.Must(err, "Could not make a client")
	tbl := client.Open("client_asn_client_loc_by_month")

	// Add a too-short sequence
	for month := 1; month < 6; month++ {
		dateString := fmt.Sprintf("2001-%02d", month)
		mut := bigtable.NewMutation()
		mut.Set("meta", "client_asn_number", bigtable.Now(), []byte("AS1"))
		mut.Set("meta", "client_location_key", bigtable.Now(), []byte("mars"))
		mut.Set("meta", "date", bigtable.Now(), []byte(dateString))

		buf := make([]byte, binary.MaxVarintLen64)
		binary.BigEndian.PutUint64(buf, 2)
		mut.Set("data", "download_speed_mbps_median", bigtable.Now(), buf)
		mut.Set("data", "count", bigtable.Now(), []byte("3"))
		rtx.Must(tbl.Apply(ctx, "AS1-mars-"+dateString, mut), "Could not apply mutation")
	}

	// Add a problematic sequence
	for year := 2013; year < 2019; year++ {
		for month := 1; month <= 12; month++ {
			dateString := fmt.Sprintf("%d-%02d", year, month)
			mut := bigtable.NewMutation()
			mut.Set("meta", "client_asn_number", bigtable.Now(), []byte("AS2"))
			mut.Set("meta", "client_location_key", bigtable.Now(), []byte("venus"))
			mut.Set("meta", "date", bigtable.Now(), []byte(dateString))

			buf := make([]byte, binary.MaxVarintLen64)
			if year < 2015 {
				binary.BigEndian.PutUint64(buf, 20)
			} else {
				binary.BigEndian.PutUint64(buf, 10)
			}
			mut.Set("data", "download_speed_mbps_median", bigtable.Now(), buf)
			mut.Set("data", "count", bigtable.Now(), []byte("3"))
			rtx.Must(tbl.Apply(ctx, "AS2-venus-"+dateString, mut), "Could not apply mutation")
		}
	}
}

func TestMain(t *testing.T) {
	// Set flags to values that are definitely NOT production values.
	*project = "mlab-fake-project"
	*instance = "fake-bigtable-instance"

	srv, err := bttest.NewServer("localhost:0")
	rtx.Must(err, "Could not start local BT server")
	defer srv.Close()
	conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
	rtx.Must(err, "Could not connect to local BT server")
	defer conn.Close()

	bigtableNewClient = func(ctx context.Context, project, instance string, _ ...option.ClientOption) (*bigtable.Client, error) {
		return bigtable.NewClient(ctx, project, instance, option.WithGRPCConn(conn))
	}
	populateFakeDataBT(conn)

	oldStdout := os.Stdout
	defer func() {
		os.Stdout = oldStdout
	}()
	tmp, err := ioutil.TempFile("", "TestSignalSearcherMain")
	rtx.Must(err, "Could not create temp file")
	os.Stdout = tmp
	defer os.Remove(tmp.Name())

	main()

	os.Stdout = oldStdout
	tmp.Close()
	tmp, err = os.Open(tmp.Name())
	rtx.Must(err, "Could not read temp file")
	contents, err := csv.NewReader(tmp).ReadAll()
	rtx.Must(err, "Could not serialize a CSV from the output")
	if len(contents) != 2 {
		t.Errorf("The output csv file should only have had two lines, not %v", contents)
	}
}
