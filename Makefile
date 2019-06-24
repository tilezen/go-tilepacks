tools:
	go build -mod vendor -o bin/build cmd/build/main.go
	go build -mod vendor -o bin/serve cmd/serve/main.go
