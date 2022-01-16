FROM golang:1.16 AS builder

WORKDIR /subwaydata.nyc

COPY go.mod ./
COPY go.sum ./

RUN go mod download

COPY . ./

RUN go build -o /usr/bin/website website/website.go
RUN go build -o /usr/bin/updater updater/updater.go

# We use this buildpack image because it already has SSL certificates installed
FROM buildpack-deps:buster-curl

COPY --from=builder /usr/bin/website /usr/bin
COPY --from=builder /usr/bin/updater /usr/bin

ENTRYPOINT ["website"]
