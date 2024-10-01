package keboola_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
)

func ExampleNewAuthorizedAPI() {
	ctx := context.TODO()
	host := "connection.keboola.com"
	token := "<my-token>"

	// Create API
	api, err := keboola.NewAuthorizedAPI(ctx, host, token)
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

func ExampleNewPublicAPIFromIndex() {
	ctx := context.TODO()
	host := "connection.keboola.com"

	// Load services list
	index, err := keboola.APIIndex(ctx, host)
	if err != nil {
		log.Fatal(err)
	}

	// Create API
	publicAPI := keboola.NewPublicAPIFromIndex(host, index)

	// Authorize
	api := publicAPI.NewAuthorizedAPI("<my-token>", 1*time.Minute)

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
	publicAPI := keboola.NewPublicAPIFromIndex(host, &index.Index)

	// Authorize
	api := publicAPI.NewAuthorizedAPI("<my-token>", 1*time.Minute)

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
