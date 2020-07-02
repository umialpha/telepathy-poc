FROM golang

ADD . /go/src/t.poc.v4/

WORKDIR /go/src/t.poc.v4

ENV GO111MODULE=on
RUN go mod download

RUN go install t.poc.v4/dispatcher

RUN go install t.poc.v4/worker
