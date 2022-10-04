.PHONY: build

install-tools:
	bash ./scripts/install-tools.sh

lint:
	bash ./scripts/lint.sh

fix:
	bash ./scripts/fix.sh

tests:
	gotestsum --no-color=false --format testname -- -timeout 600s -p 8 -parallel 8 -v -race -coverprofile=/tmp/profile.out ./pkg/...

godoc:
	godoc -http=0.0.0.0:6060
