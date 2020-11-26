package main

import (
	"context"
	"fmt"

	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
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
	msgd := md.GetInputType()
	
	
	stub := grpcdynamic.NewStub(cc)
	request := &pb.HelloRequest{Name: "hi"}
	resp, err := stub.InvokeRpc(context.Background(), md, request)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(resp)

}
