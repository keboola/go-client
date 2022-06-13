#!/bin/bash
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
