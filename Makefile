.PHONY: all build proto test test-short bench cluster clean docker-up docker-down

all: build

build:
	go build -o bin/kvnode ./cmd/kvnode
	go build -o bin/kvbench ./cmd/kvbench

proto:
	protoc --proto_path=proto \
		--go_out=gen/raftkvpb --go_opt=paths=source_relative \
		--go-grpc_out=gen/raftkvpb --go-grpc_opt=paths=source_relative \
		proto/raftkv.proto

test:
	go test ./... -race -timeout 300s

test-short:
	go test ./... -race -short -timeout 120s

# Start a local 3-node cluster (foreground; ctrl-c stops all nodes).
cluster: build
	./scripts/run-local-cluster.sh

# Run the benchmark against a running local cluster.
bench: build
	./bin/kvbench --addrs localhost:7001,localhost:7002,localhost:7003 \
		--clients 64 --ops 50000 --read-ratio 0.5 --value-size 128

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down -v

clean:
	rm -rf bin data
