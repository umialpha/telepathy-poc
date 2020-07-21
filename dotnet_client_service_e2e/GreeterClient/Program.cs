// Copyright 2015 gRPC authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Diagnostics;
using System.IO;
using System.Collections;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Grpc.Core;
using Grpc.Reflection.V1Alpha;
using Helloworld;
using static Grpc.Reflection.V1Alpha.ServerReflection;
using grpc = global::Grpc.Core;
using System.Linq;
using System.Security.Authentication.ExtendedProtection;

namespace GreeterClient
{   
    public class RawMessage
    {
        public byte[] msg;

        public static byte[] Serialize(RawMessage req)
        {
            return req.msg;
        }

        public static RawMessage Deserialize(byte[] bytes)
        {
            return new RawMessage()
            {
                msg = bytes
            };
        }
    }





    public class Raw
    {
        public static readonly grpc::Method<RawMessage, RawMessage> RawMethd = new grpc::Method<RawMessage, RawMessage>(
        grpc::MethodType.Unary,
        "helloworld.Greeter",
        "SayHello",
        grpc::Marshallers.Create(RawMessage.Serialize, RawMessage.Deserialize),
        grpc::Marshallers.Create(RawMessage.Serialize, RawMessage.Deserialize));

    }



    class Program
    {

        static void  Main(string[] args)
        {
            // Client constructs a HelloRequest,
            // Then it serializes to bytes,
            // And sends ServiceName: `helloworld.Greeter`, MethodName: `SayHello` to NodeAgent
            var helloReq = new HelloRequest { Name = "hi lei" };
            var array = helloReq.ToByteArray();

            // Agent recieves the ServiceName, MethodName and bytes
            // Then construct a rpc call to client's service.
            var rawReq = new RawMessage { msg = array };
            Channel channel = new Channel("127.0.0.1:50051", ChannelCredentials.Insecure);
            var callInvoker = channel.CreateCallInvoker();
            // send rpc to client's service, put client's response bytes in RawMessage
            RawMessage resp = callInvoker.BlockingUnaryCall(Raw.RawMethd, null, new grpc::CallOptions(), rawReq);

            // Client gets the response bytes and deserializes to HelloReply
            var reply = HelloReply.Parser.ParseFrom(resp.msg);
            Console.WriteLine(reply.Message);
        }

        private static async Task<ServerReflectionResponse> SingleRequestAsync(ServerReflectionClient client, ServerReflectionRequest request)
        {
            var call = client.ServerReflectionInfo();
            await call.RequestStream.WriteAsync(request);
            Debug.Assert(await call.ResponseStream.MoveNext());

            var response = call.ResponseStream.Current;
            await call.RequestStream.CompleteAsync();
            return response;
        }
    }
}
