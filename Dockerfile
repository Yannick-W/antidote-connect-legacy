# Building the go executable with the golang image.
FROM golang:1.13.0-stretch AS builder

ENV GO111MODULE=on \
    CGO_ENABLED=1

WORKDIR /build

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY antidote-connect.go .

RUN go build ./antidote-connect.go

WORKDIR /dist
RUN cp /build/antidote-connect ./antidote-connect

RUN ldd antidote-connect | tr -s '[:blank:]' '\n' | grep '^/' | \
    xargs -I % sh -c 'mkdir -p $(dirname ./%); cp % ./%;'
RUN mkdir -p lib64 && cp /lib64/ld-linux-x86-64.so.2 lib64/

# Create the minimal runtime image
FROM scratch

COPY --chown=0:0 --from=builder /dist /

ENTRYPOINT ["/antidote-connect"]
