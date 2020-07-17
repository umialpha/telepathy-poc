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

namespace GreeterClient
{
    class Program
    {
        //public static void Main(string[] args)
        //{
        //    Channel channel = new Channel("127.0.0.1:50051", ChannelCredentials.Insecure);

        //    var client = new Greeter.GreeterClient(channel);
        //    String user = "you";

        //    var reply = client.SayHello(new HelloRequest { Name = user });
        //    Console.WriteLine("Greeting: " + reply.Message);

        //    channel.ShutdownAsync().Wait();
        //    Console.WriteLine("Press any key to exit...");
        //    Console.ReadKey();
        //}

        public static async Task RelectionFromSvc()
        {
            /*
             FileDescriptor from svc.
            reflection using the returned FileDescriptors is not currently supported.            
            */
            Channel channel = new Channel("127.0.0.1:50051", ChannelCredentials.Insecure);
            var client = new ServerReflectionClient(channel);

            var response = await SingleRequestAsync(client, new ServerReflectionRequest
            {
                FileContainingSymbol = "helloworld.Greeter" // Get all services
            });



            var fds = FileDescriptor.BuildFromByteStrings(response.FileDescriptorResponse.FileDescriptorProto);
            var fd = fds[0];
            
            Console.WriteLine(fd.Name);
            var sd = fd.FindTypeByName<ServiceDescriptor>("Greeter");
            Console.WriteLine(sd.Name);
            var md = sd.FindMethodByName("SayHello");

            Console.WriteLine(md.Name);
            var ind = md.InputType;
            Console.WriteLine(ind.Name);
            Console.WriteLine(ind.Parser);
        }


        public static async Task RelectionFromProtoFile()
        {
            /*
             RelectionFromProtoFile
             Limit:
             We must first generate FileDescriptorSet bytes[] using `protoc`:
            `protoc "helloworld.proto"  --descriptor_set_out="helloworld"`
            */

            using (var stream = File.OpenRead(@"D:\gits\telepathy-project\poc\dotnet_grpc_reflection\GreeterClient\helloworld"))
            {
                var descriptorSet = FileDescriptorSet.Parser.ParseFrom(stream);
                var byteStrings = descriptorSet.File.Select(f => f.ToByteString()).ToList();
                var fds = FileDescriptor.BuildFromByteStrings(byteStrings);
                var fd = fds[0];

                Console.WriteLine(fd.Name);
                var sd = fd.FindTypeByName<ServiceDescriptor>("Greeter");
                Console.WriteLine(sd.Name);
                var md = sd.FindMethodByName("SayHello");

                Console.WriteLine(md.Name);
                var ind = md.InputType;
                Console.WriteLine(ind.Name);
                Console.WriteLine(ind.Parser);
                Console.WriteLine(ind.ClrType);

            }
                

        }


        static async Task Main(string[] args)
        {
            await RelectionFromProtoFile();
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
