package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type ClientConn struct {
	ch chan []byte
	r  io.Reader
	w  io.Writer
}

func NewClient() *ClientConn {
	c := &ClientConn{
		ch: make(chan []byte),
	}
	conn, err := net.Dial("tcp", "localhost:5389")
	if err != nil {
		fmt.Println(err)
		return nil
	}
	c.r = conn
	c.w = conn
	return c
}

func (c *ClientConn) read() ([]byte, error) {
	var msgSize int32

	// message size
	err := binary.Read(c.r, binary.BigEndian, &msgSize)
	if err != nil {
		return nil, err
	}

	if msgSize < 0 {
		return nil, fmt.Errorf("response msg size is negative: %v", msgSize)
	}
	// message binary data
	buf := make([]byte, msgSize)
	_, err = io.ReadFull(c.r, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (c *ClientConn) write(bytes []byte) {
	c.w.Write(bytes)
	c.flush()
}

type flusher interface {
	Flush() error
}

func (c *ClientConn) flush() error {
	if f, ok := c.w.(flusher); ok {
		return f.Flush()
	}
	return nil
}

func main() {
	c := NewClient()
	go func() {
		resp, err := c.read()
		if err != nil {
			print("Read error", err)
			return
		}
		c.ch <- resp
	}()
	c.write([]byte{'h', 'e'})
}
