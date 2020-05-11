

if __name__ == "__main__":
    from rpc.worker_client import WorkerClient
    import rpc.worker_pb2 as worker_pb2 
    client = WorkerClient("localhost:5001")
    client.send_task(1)
    client.send_task(2)
    client.send_task(3)