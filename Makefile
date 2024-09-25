.PHONY: format
format:
	go fmt github.com/skivdal/autonormalize/...

.PHONY: run
run:
	go run src/*.go $(filter-out $@, $(MAKECMDGOALS))

build src/main.go:
	CGO_ENABLED=0 go build -o autonormalize -ldflags="-extldflags=-static" src/*.go

# Method 1, https://stackoverflow.com/a/45003119/8966506
%:
	@true

