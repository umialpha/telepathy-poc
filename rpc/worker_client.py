import grpc

import worker_pb2_grpc
import worker_pb2 



class WorkerClient:

    def __init__(self, server_endpoint):
        self.stub = grpc.insecure_channel(server_endpoint)


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.stub.close()


    # def __getattr__(self, attr):
    #     if attr in self.__dict__:
    #         return self.__dict__[attr]
    #     else:
    #         return self.stub.__getattr__(attr)
    





# if __name__ == "__main__":
#     client = WorkerClient("localhost:5001")
#     client.stub.send_task()