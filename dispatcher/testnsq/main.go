package main

import (
	"bufio"
	"fmt"
	"net"
	"time"

	"github.com/nsqio/go-nsq"
)

type myHandler struct {
	topic   string
	channel string
}

func (h *myHandler) HandleMessage(m *nsq.Message) error {
	fmt.Println("consumer handle message")
	if len(m.Body) == 0 {
		// Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
		return nil
	}
	fmt.Println(m.ID)
	fmt.Println(m.NSQDAddress)
	m.DisableAutoResponse()
	go h.delayAck(m)
	// Returning a non-nil error will automatically send a REQ command to NSQ to re-queue the message.
	return nil
}

func (h *myHandler) delayAck(m *nsq.Message) {
	time.Sleep(1 * time.Second)
	conn, err := net.DialTimeout("tcp", m.NSQDAddress, time.Second)
	if err != nil {
		panic(err.Error())
	}
	conn.Write(nsq.MagicV2)
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	ci := make(map[string]interface{})
	ci["client_id"] = "test-ack"
	cmd, _ := nsq.Identify(ci)
	cmd.WriteTo(rw)
	nsq.Subscribe("disp-topic", "disp-channel2").WriteTo(rw)
	rw.Flush()
	nsq.ReadResponse(rw)
	nsq.ReadResponse(rw)

	nsq.Finish(m.ID).WriteTo(rw)
	rw.Flush()
	fmt.Println("delayAck finish")
}

func manualAck() {
	config := nsq.NewConfig()
	config.ClientID = "test-ack"
	nsqc, err := nsq.NewConsumer("disp-topic", "disp-channel2", config)
	defer nsqc.Stop()
	if err != nil {
		panic(err)
	}
	nsqc.AddHandler(&myHandler{})
	nsqc.ConnectToNSQLookupds([]string{"localhost:4161"})
	forever := make(chan int)
	<-forever
}

func main() {
	manualAck()

}
