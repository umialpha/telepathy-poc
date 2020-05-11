import time
import threading
from concurrent import futures

import grpc

from rpc.worker_svc import WorkerSvc
import rpc.worker_pb2_grpc as worker_pb2_grpc
import rpc.worker_pb2
import config

class Worker(WorkerSvc):
    pass


def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    worker_pb2_grpc.add_WorkerSvcServicer_to_server(WorkerSvc(), server)
    server.add_insecure_port('[::]:{0}'.format(port))
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    import sys
    serve(sys.argv[1])



    
