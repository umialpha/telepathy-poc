# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: worker.proto

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='worker.proto',
  package='',
  syntax='proto3',
  serialized_options=None,
  serialized_pb=b'\n\x0cworker.proto\"\x1d\n\x0bTaskRequest\x12\x0e\n\x06taskid\x18\x01 \x01(\x05\"\x1e\n\x0cTaskResponse\x12\x0e\n\x06taskid\x18\x01 \x01(\x05\x32\x39\n\tWorkerSvc\x12,\n\tsend_task\x12\x0c.TaskRequest\x1a\r.TaskResponse(\x01\x30\x01\x62\x06proto3'
)




_TASKREQUEST = _descriptor.Descriptor(
  name='TaskRequest',
  full_name='TaskRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='taskid', full_name='TaskRequest.taskid', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=16,
  serialized_end=45,
)


_TASKRESPONSE = _descriptor.Descriptor(
  name='TaskResponse',
  full_name='TaskResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='taskid', full_name='TaskResponse.taskid', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=47,
  serialized_end=77,
)

DESCRIPTOR.message_types_by_name['TaskRequest'] = _TASKREQUEST
DESCRIPTOR.message_types_by_name['TaskResponse'] = _TASKRESPONSE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

TaskRequest = _reflection.GeneratedProtocolMessageType('TaskRequest', (_message.Message,), {
  'DESCRIPTOR' : _TASKREQUEST,
  '__module__' : 'worker_pb2'
  # @@protoc_insertion_point(class_scope:TaskRequest)
  })
_sym_db.RegisterMessage(TaskRequest)

TaskResponse = _reflection.GeneratedProtocolMessageType('TaskResponse', (_message.Message,), {
  'DESCRIPTOR' : _TASKRESPONSE,
  '__module__' : 'worker_pb2'
  # @@protoc_insertion_point(class_scope:TaskResponse)
  })
_sym_db.RegisterMessage(TaskResponse)



_WORKERSVC = _descriptor.ServiceDescriptor(
  name='WorkerSvc',
  full_name='WorkerSvc',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  serialized_start=79,
  serialized_end=136,
  methods=[
  _descriptor.MethodDescriptor(
    name='send_task',
    full_name='WorkerSvc.send_task',
    index=0,
    containing_service=None,
    input_type=_TASKREQUEST,
    output_type=_TASKRESPONSE,
    serialized_options=None,
  ),
])
_sym_db.RegisterServiceDescriptor(_WORKERSVC)

DESCRIPTOR.services_by_name['WorkerSvc'] = _WORKERSVC

# @@protoc_insertion_point(module_scope)
