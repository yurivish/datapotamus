run:
    cd cmd/web && go run .
test:
    go test ./...
lines:
    scc --exclude-file _test.go,_templ.go --exclude-dir internal/flow/sublist
core-lines:
    cd internal/flow && scc --exclude-file _test.go,_templ.go --exclude-dir sublist
