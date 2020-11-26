using System;
using Helloworld;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Utils;


namespace server
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {    
                Server server = new Server
                {
                    Services = { Greeter.BindService(new GreeterService()) },
                    Ports = { new ServerPort("localhost", 50051, ServerCredentials.Insecure) }
                };
                server.Start();
                Console.WriteLine("Accounts server listening on port " + 50051);
                Console.WriteLine("Press any key to stop the server...");
                Console.ReadKey();
                server.ShutdownAsync().Wait();
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Exception encountered: {ex}");
            }
        }
    }
}
