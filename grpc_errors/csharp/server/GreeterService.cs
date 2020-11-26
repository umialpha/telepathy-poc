using grpc = global::Grpc.Core;
using Helloworld;
using System.Threading.Tasks;
using System;

namespace server
{
    public class GreeterService : Greeter.GreeterBase

    {
        public GreeterService()
        {
            
        }

        public override Task<HelloReply> SayHello(HelloRequest request, grpc::ServerCallContext context)
        {   
            throw new grpc::RpcException(new grpc::Status(10000, "user defined error"));
            
        }
    }
}