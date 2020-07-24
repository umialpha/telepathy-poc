# Dispatcher POC
Dispatcher works as the special proxy of NSQ.

##
Basically, workers(node agent) GetTask from dispatcher, declaring the `topic`, `channel` and `worker_id`.
Dispatcher will (pre)fecth the message from NSQ and forward it to the worker.

When workers finish the task, it will FinTask to dispatcher, declaring the `topic`, `channel`, `worker_id`, `message_id` and `payload`.
Dispatcher will confirm the fin of the message to NSQ.
