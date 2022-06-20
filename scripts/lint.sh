#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

go mod verify
go vet ./pkg/...
staticcheck -f stylish ./pkg/...
fmtOut=$(gofmt -s -d -l ./pkg);\
if [ ! -z "$fmtOut" ]; then\
  echo "Problems:\n$fmtOut";\
	echo "Go files must be formatted with gofmt ^^^. Please run: \"gofmt -s -d -w ./pkg\"";\
    exit 1;\
fi
echo "OK"
