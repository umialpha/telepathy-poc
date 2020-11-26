FROM golang

ADD . /go/src/scaler/

WORKDIR /go/src/scaler/

ENV GO111MODULE=on
RUN go mod download

RUN go install ./cmd/server

RUN go install ./cmd/test-worker


