FROM golang
ADD . /go/src/stan/

WORKDIR /go/src/stan

RUN go mod download
RUN go install github.com/nats-io/stan.go/examples/stan-bench

