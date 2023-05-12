# Keboola Go Client

## Packages

### `request` package

The `request` package provides abstract and immutable definition of an HTTP request by the `request.HTTPRequest`.

One or more HTTP requests can be grouped together to the generic type `request.APIRequest[R]`,
where the `R` is a result type to which HTTP requests are mapped.

Request sending is provided by the `Sender` interface, the `client` package provides its default implementation.

### `client` package

The `client` package provides default implementation of the `request.Sender` interface based on the standard `net/http` package.

### `keboola` package

The `keboola` package provides the `keboola.API` implementation, it covers:
  - [Storage API](https://keboola.docs.apiary.io/#)
  - [Encryption API](https://keboolaencryption.docs.apiary.io/#)
  - [Jobs Queue API](https://app.swaggerhub.com/apis-docs/keboola/job-queue-api)
  - [Sandboxes API](https://sandboxes.keboola.com/documentation)
  - [Scheduler API](https://app.swaggerhub.com/apis/odinuv/scheduler)

Not all API requests are covered, API requests are extended as needed.

## Quick Start

```go
ctx := context.TODO()

// Create API instance
api, err := keboola.NewAPI(
  ctx, 
  "https://connection.keboola.com", 
  keboola.WithTracerProvider(tracerProvider), 
  keboola.WithMeterProvider(meterProvider),
)
if err != nil {
  return err
}

// Send a request
config, err := api.CreateConfigRequest(&keboola.ConfigWithRows{/*...*/}).Send(ctx)
if err != nil {
  return err
}
```

## Development

Clone the repository and run dev container:
```sh
docker-compose run --rm -u "$UID:$GID" --service-ports dev bash
```

Run lint and tests in container:
```sh
make lint
make tests
```

Run HTTP server with documentation:
```sh
make godoc
```

Open `http://localhost:6060/pkg/github.com/keboola/go-client/pkg/` in browser.

## License

MIT licensed, see [LICENSE](./LICENSE) file.
