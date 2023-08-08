package keboola_test

import (
	"context"
	"fmt"
	"log"

	"github.com/keboola/go-client/pkg/keboola"
)

func ExampleNewAPI() {
	ctx := context.TODO()
	host := "connection.keboola.com"

	// Create API
	api, err := keboola.NewAPI(ctx, host, keboola.WithToken("<my-token>"))
	if err != nil {
		log.Fatal(err)
	}

	// Get default branch
	defaultBranch, err := api.GetDefaultBranchRequest().Send(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Send request
	buckets, err := api.ListBucketsRequest(defaultBranch.ID).Send(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%#v", buckets)
}

func ExampleNewAPIFromIndex() {
	ctx := context.TODO()
	host := "connection.keboola.com"

	// Load services list
	index, err := keboola.APIIndex(ctx, host, keboola.WithToken("<my-token>"))
	if err != nil {
		log.Fatal(err)
	}

	// Create API
	api := keboola.NewAPIFromIndex(host, index)

	// Get default branch
	defaultBranch, err := api.GetDefaultBranchRequest().Send(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Send request
	buckets, err := api.ListBucketsRequest(defaultBranch.ID).Send(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%#v", buckets)
}

func Example_newAPIFromIndexWithComponents() {
	ctx := context.TODO()
	host := "connection.keboola.com"

	// Load services list and components at once
	index, err := keboola.APIIndexWithComponents(ctx, host)
	if err != nil {
		log.Fatal(err)
	}

	// Create API
	api := keboola.NewAPIFromIndex(host, &index.Index, keboola.WithToken("<my-token>"))

	// Get default branch
	defaultBranch, err := api.GetDefaultBranchRequest().Send(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Send request
	buckets, err := api.ListBucketsRequest(defaultBranch.ID).Send(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%#v", buckets)
}
