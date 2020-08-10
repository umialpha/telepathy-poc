FROM golang

ADD . /go/src/dispatcher/

WORKDIR /go/src/dispatcher

ENV GO111MODULE=on
RUN go mod download

RUN go install ./client

RUN go install ./cmd


