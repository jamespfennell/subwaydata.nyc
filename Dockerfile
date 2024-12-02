FROM golang:1.22 AS builder

WORKDIR /subwaydata.nyc

COPY go.mod ./
COPY go.sum ./

RUN go mod download

COPY . ./

RUN go build -o /usr/bin/subwaydatanyc .

RUN go test ./...

# We use this buildpack image because it already has SSL certificates installed
FROM buildpack-deps:stable

COPY --from=builder /usr/bin/subwaydatanyc /usr/bin

ENTRYPOINT ["website"]
