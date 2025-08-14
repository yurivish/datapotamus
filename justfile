air:
     go tool air
run:
    cd cmd/web && go run .
lines:
    scc --exclude-file _test.go,_templ.go --exclude-dir internal/sublist
