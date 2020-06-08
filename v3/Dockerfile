FROM golang

ADD . /go/src/t.poc.v3/

WORKDIR /go/src/t.poc.v3

ENV GO111MODULE=on
RUN go mod download

RUN go install ./client

RUN go install ./frontend

RUN go install ./backend

RUN go install ./worker

