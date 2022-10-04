#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

# Check the most important problems first
if ! go vet ./pkg/...; then
    echo "Please fix ^^^ errors."
    echo
    exit 1
fi

# Fix modules
go mod tidy
go mod vendor

# Fix linters
if golangci-lint run --fix -c "./build/ci/golangci.yml"; then
    echo "Ok. The code looks good."
    echo
else
    echo "Some errors ^^^ cannot be fixed. Please fix them manually."
    echo
    exit 1
fi
