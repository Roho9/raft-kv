FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/kvnode ./cmd/kvnode && \
    CGO_ENABLED=0 go build -o /out/kvbench ./cmd/kvbench

FROM alpine:3.20
COPY --from=build /out/kvnode /usr/local/bin/kvnode
COPY --from=build /out/kvbench /usr/local/bin/kvbench
VOLUME /data
ENTRYPOINT ["kvnode"]
