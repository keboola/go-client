# Keboola Go Client

- Supported Keboola APIs:
  - [Storage API](https://keboola.docs.apiary.io/#)
  - [Encryption API](https://keboolaencryption.docs.apiary.io/#)
  - [Jobs Queue API](https://app.swaggerhub.com/apis-docs/keboola/job-queue-api)
  - [Sandboxes API](https://sandboxes.keboola.com/documentation)
  - [Scheduler API](https://app.swaggerhub.com/apis/odinuv/scheduler)
- Not all API requests are covered, clients are extended as needed.
- The definitions are independent of client implementation, see `Sender` interface.
- Contains `Client`, default `Sender` implementation, based on standard `net/http` package.
- Support retries and tracing/telemetry.

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
