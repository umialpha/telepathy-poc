FROM golang
ADD . /go/src/nsq/bench

WORKDIR /go/src/nsq/bench

RUN go mod download
RUN go install .

