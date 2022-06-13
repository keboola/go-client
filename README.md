# Keboola Go Client

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
