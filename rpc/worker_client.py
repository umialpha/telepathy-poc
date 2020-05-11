import grpc

import rpc.worker_pb2_grpc as worker_pb2_grpc
import rpc.worker_pb2 as worker_pb2 
import queue


class WorkerClient:

    def __init__(self, server_endpoint):
        
        self.channel = grpc.insecure_channel(server_endpoint)
        self.stub = worker_pb2_grpc.WorkerSvcStub(self.channel)
        self._queue = queue.Queue()
        self._stream =  self.stub.send_task(iter(self._queue.get, None))


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.channel.close()

    def send_task(self, taskid):
        self._queue.put(worker_pb2.TaskRequest(taskid=int(taskid)), False)

    def close(self):
        self.channel.close()

    # def __getattr__(self, attr):
    #     if attr in self.__dict__:
    #         return self.__dict__[attr]
    #     else:
    #         return self.stub.__getattr__(attr)
    





if __name__ == "__main__":
    client = WorkerClient("localhost:5001")
    client.stub.send_task(worker_pb2.TaskRequest(taskid=1))