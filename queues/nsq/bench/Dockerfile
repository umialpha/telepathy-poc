FROM golang
ADD . /go/src/nsq/

WORKDIR /go/src/nsq

RUN go mod download
RUN go install bench/

