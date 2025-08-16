air:
     go tool air
run:
    cd cmd/web && go run .
lines:
    scc --exclude-file _test.go,_templ.go --exclude-dir internal/core/sublist
core-lines:
    cd internal/core && scc --exclude-file _test.go,_templ.go --exclude-dir sublist
