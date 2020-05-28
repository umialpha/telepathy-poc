FROM golang

ADD . /go/src/telepathy.poc/

WORKDIR /go/src/telepathy.poc

ENV GO111MODULE=on
RUN go mod download

RUN go install telepathy.poc/client

RUN go install telepathy.poc/frontend

RUN go install telepathy.poc/backend

RUN go install telepathy.poc/worker
