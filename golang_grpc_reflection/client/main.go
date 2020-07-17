package main

import (
	"context"
	"fmt"

	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/protobuf/proto"
	pb "poc.reflect/proto"
)

func main() {
	cc, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		fmt.Println(err)
		return
	}
	client := grpcreflect.NewClient(context.Background(), grpc_reflection_v1alpha.NewServerReflectionClient(cc))
	//svcNames, err := client.ListServices()
	//fmt.Println(svcNames[0])
	sd, err := client.ResolveService("protos.Greeter")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(sd.GetMethods())
	md := sd.FindMethodByName("SayHello")
	msgInputD := md.GetInputType()
	msg := dynamic.NewMessage(msgInputD)

	stub := grpcdynamic.NewStub(cc)
	request := &pb.HelloRequest{Name: "hi"}
	bytes, _ := proto.Marshal(request)
	msg.Unmarshal(bytes)
	resp, err := stub.InvokeRpc(context.Background(), md, msg)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(resp)

}
