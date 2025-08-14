air:
     go tool air
run:
    cd cmd/web && go run .
lines:
    scc --exclude-file _templ.go,_nats_,sublist
