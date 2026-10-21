FROM golang:1.22 AS builder

WORKDIR /subwaydata.nyc

COPY go.mod ./
COPY go.sum ./

RUN go mod download

COPY . ./

RUN ls -lah
RUN go build -o /usr/bin/subwaydatanyc .

RUN go test ./...

# We use this buildpack image because it already has SSL certificates installed
FROM buildpack-deps:stable

COPY --from=builder /usr/bin/subwaydatanyc /usr/bin

# See Hoard Dockerfile for an explanation
ENV GLIBC_TUNABLES=glibc.malloc.arena_max=1:glibc.malloc.mmap_threshold=131072:glibc.malloc.trim_threshold=131072

ENTRYPOINT ["subwaydatanyc"]
