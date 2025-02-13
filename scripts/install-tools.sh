#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR"

# gotestsum
if ! command -v gotestsum &> /dev/null
then
  ./install-gotestsum.sh -b $(go env GOPATH)/bin
fi

# golangci-lint
if ! command -v golangci-lint &> /dev/null
then
 go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.63.1
fi

# godoc
if ! command -v godoc &> /dev/null
then
  go install golang.org/x/tools/cmd/godoc@latest
fi

# go-mod-upgrade
if ! command -v go-mod-upgrade &> /dev/null
then
  go install github.com/oligot/go-mod-upgrade@latest
fi