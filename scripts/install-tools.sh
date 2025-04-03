#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR"

# Version to install
GOLANGCI_LINT_VERSION="v2.0.2"

# gotestsum
if ! command -v gotestsum &> /dev/null
then
  ./install-gotestsum.sh -b $(go env GOPATH)/bin
fi

# golangci-lint
if ! command -v golangci-lint &> /dev/null
then
    curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin $GOLANGCI_LINT_VERSION
fi

# Verify installation
echo "Verifying golangci-lint installation..."
golangci-lint --version


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