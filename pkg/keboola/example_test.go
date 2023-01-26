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

	// Send request
	branches, err := api.ListBucketsRequest().Send(ctx)
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Printf("%#v", branches)
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

	// Send request
	branches, err := api.ListBucketsRequest().Send(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%#v", branches)
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

	// Send request
	branches, err := api.ListBucketsRequest().Send(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%#v", branches)
}
