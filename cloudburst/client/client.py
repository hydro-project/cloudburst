#  Copyright 2019 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging

import zmq
from anna.client import AnnaTcpClient

from cloudburst.shared.function import CloudburstFunction
from cloudburst.shared.future import CloudburstFuture
from cloudburst.shared.proto.cloudburst_pb2 import (
    Dag,
    DagCall,
    Function,
    FunctionCall,
    GenericResponse,
    NORMAL,  # Cloudburst consistency modes
    MULTIEXEC # Cloudburst's execution types
)
from cloudburst.shared.proto.shared_pb2 import StringSet
from cloudburst.shared.serializer import Serializer
from cloudburst.shared.utils import (
    CONNECT_PORT,
    DAG_CALL_PORT,
    DAG_CREATE_PORT,
    DAG_DELETE_PORT,
    FUNC_CALL_PORT,
    FUNC_CREATE_PORT,
    LIST_PORT
)

serializer = Serializer()


class CloudburstConnection():
    def __init__(self, func_addr, ip, tid=0, local=False):
        '''
        func_addr: The address of the Cloudburst interface, either localhost or
        the address of an AWS ELB in cluster mode.
        ip: The IP address of the client machine -- used to send and receive
        responses.
        tid: If multiple clients are running on the same machine, they will
        need to use unique IDs.
        local: A boolean representin whether the client is interacting with the
        cluster in local or cluster mode.
        '''

        self.service_addr = 'tcp://' + func_addr + ':%d'
        self.context = zmq.Context(1)

        kvs_addr = self._connect()
        while not kvs_addr:
            logging.info('Connection timed out, retrying')
            print('Connection timed out, retrying')
            kvs_addr = self._connect()

        # Picks a random offset of 10, mostly to alleviate port conflicts when
        # running in local mode.
        self.kvs_client = AnnaTcpClient(kvs_addr, ip, local=local,
                                        offset=tid + 10)

        self.func_create_sock = self.context.socket(zmq.REQ)
        self.func_create_sock.connect(self.service_addr % FUNC_CREATE_PORT)

        self.func_call_sock = self.context.socket(zmq.REQ)
        self.func_call_sock.connect(self.service_addr % FUNC_CALL_PORT)

        self.list_sock = self.context.socket(zmq.REQ)
        self.list_sock.connect(self.service_addr % LIST_PORT)

        self.dag_create_sock = self.context.socket(zmq.REQ)
        self.dag_create_sock.connect(self.service_addr % DAG_CREATE_PORT)

        self.dag_call_sock = self.context.socket(zmq.REQ)
        self.dag_call_sock.connect(self.service_addr % DAG_CALL_PORT)

        self.dag_delete_sock = self.context.socket(zmq.REQ)
        self.dag_delete_sock.connect(self.service_addr % DAG_DELETE_PORT)

        self.response_sock = self.context.socket(zmq.PULL)
        response_port = 9000 + tid
        self.response_sock.setsockopt(zmq.RCVTIMEO, 1000)
        self.response_sock.bind('tcp://*:' + str(response_port))

        self.response_address = 'tcp://' + ip + ':' + str(response_port)

        self.rid = 0

    def list(self, prefix=None):
        '''
        Returns a list of all the functions registered in the system.

        prefix: An optional argument which, if specified, prunes the list of
        returned functions to match the provided prefix.
        '''

        for fname in self._get_func_list(prefix):
            print(fname)

    def get_function(self, name):
        '''
        Retrieves a handle for an individual function. Returns None if the
        function cannot be found in the system. The returned object can be
        called like a regular Python function, which returns a CloudburstFuture.

        name: The name of the function to retrieve.
        '''
        if name not in self._get_func_list():
            print(f'''No function found with name {name}. To view all
                  functions, use the `list` method.''')
            return None

        return CloudburstFunction(name, self, self.kvs_client)

    def register(self, function, name):
        '''
        Registers a new function or class with the system. The returned object
        can be called like a regular Python function, which returns a Cloudburst
        Future. If the input is a class, the class is expected to have a run
        method, which is what is invoked at runtime.

        function: The function object that we are registering.
        name: A unique name for the function to be stored with in the system.
        '''

        func = Function()
        func.name = name
        func.body = serializer.dump(function)

        self.func_create_sock.send(func.SerializeToString())

        resp = GenericResponse()
        resp.ParseFromString(self.func_create_sock.recv())

        if resp.success:
            return CloudburstFunction(name, self, self.kvs_client)
        else:
            raise RuntimeError(f'Unexpected error while registering function: {resp}.')

    def register_dag(self, name, functions, connections, gpu_functions=[],
                     batching_functions=[], colocated=[]):
        '''
        Registers a new DAG with the system. This operation will fail if any of
        the functions provided cannot be identified in the system.

        name: A unique name for this DAG.
        functions: A list of names of functions to be included in this DAG.
        connections: A list of ordered pairs of function names that represent
        the edges in this DAG.
        colocated: A list of function names that (if possible) should be
        colocated.
        '''

        flist = self._get_func_list()
        for fname in functions:
            if isinstance(fname, tuple):
                fname = fname[0]

            if fname not in flist:
                raise RuntimeError(
                    f'Function {fname} not registered. Please register before ' +
                    'including it in a DAG.')

        dag = Dag()
        dag.name = name
        dag.colocated.extend(colocated)

        for function in functions:
            ref = dag.functions.add()

            if type(function) == tuple:
                fname = function[0]
                invalids = function[1]
                ref.type = MULTIEXEC
            else:
                fname = function
                invalids = []

            if function in gpu_functions:
                ref.gpu = True

            if function in batching_functions:
                ref.batching = True

            ref.name = fname
            for invalid in invalids:
                ref.invalid_results.append(serializer.dump(invalid))

        for pair in connections:
            conn = dag.connections.add()
            conn.source = pair[0]
            conn.sink = pair[1]

        self.dag_create_sock.send(dag.SerializeToString())

        r = GenericResponse()
        r.ParseFromString(self.dag_create_sock.recv())

        return r.success, r.error

    def call_dag(self, dname, arg_map, direct_response=False,
                 consistency=NORMAL, output_key=None, client_id=None,
                 dry_run=False, continuation=None):
        '''
        Issues a new request to execute the DAG. Returns a CloudburstFuture that

        dname: The name of the DAG to cexecute.
        arg_map: A map from function names to lists of arguments for each of
        the functions in the DAG.
        direct_response: If True, the response will be synchronously received
        by the client; otherwise, the result will be stored in the KVS.
        consistency: The consistency mode to use with this function: either
        NORMAL or MULTI.
        output_key: The KVS key in which to store the result of thie DAG.
        client_id: An optional ID associated with an individual client across
        requests; this is used for causal metadata.
        '''
        dc = DagCall()
        dc.name = dname
        dc.consistency = consistency

        if output_key:
            dc.output_key = output_key

        if client_id:
            dc.client_id = client_id

        for fname in arg_map:
            fname_args = arg_map[fname]
            if type(fname_args) != list:
                fname_args = [fname_args]
            args = [serializer.dump(arg, serialize=False) for arg in
                    fname_args]
            al = dc.function_args[fname]
            al.values.extend(args)

        if direct_response:
            dc.response_address = self.response_address

        if continuation:
            if bool(continuation.response_address) != direct_response:
                raise RuntimeError('Continuation does not have same direct'
                                   + ' response setting as current call.')

            dc.continuation.name = continuation.name
            dc.continuation.call.CopyFrom(continuation)

        if dry_run:
            return dc

        self.dag_call_sock.send(dc.SerializeToString())

        r = GenericResponse()
        r.ParseFromString(self.dag_call_sock.recv())

        if r.success:
            if direct_response:
                try:
                    result = self.response_sock.recv()
                    return serializer.load(result)
                except zmq.ZMQError as e:
                    if e.errno == zmq.EAGAIN:
                        logging.error('Request timed out')
                        return None
                    else:
                        raise e
            else:
                return CloudburstFuture(r.response_id, self.kvs_client,
                                     serializer)
        else:
            logging.error('Scheduler returned unexpected error: \n' + str(r))
            raise RuntimeError(str(r.error))

    def delete_dag(self, dname):
        '''
        Removes the specified DAG from the system.

        dname: The name of the DAG to delete.
        '''
        self.dag_delete_sock.send_string(dname)

        r = GenericResponse()
        r.ParseFromString(self.dag_delete_sock.recv())

        return r.success, r.error

    def get_object(self, key):
        '''
        Retrieves an arbitrary key from the KVS, automatically deserializes it,
        and returns the value to the user.
        '''
        lattice = self.kvs_client.get(key)[key]
        return serializer.load_lattice(lattice)

    def put_object(self, key, value):
        '''
        Automatically wraps an object in a lattice and puts it into the
        key-value store at the desired key.
        '''
        lattice = serializer.dump_lattice(value)
        return self.kvs_client.put(key, lattice)

    def exec_func(self, name, args):
        call = FunctionCall()
        call.name = name
        call.request_id = self.rid

        for arg in args:
            argobj = call.arguments.values.add()
            serializer.dump(arg, argobj)

        self.func_call_sock.send(call.SerializeToString())

        r = GenericResponse()
        r.ParseFromString(self.func_call_sock.recv())

        self.rid += 1
        return r.response_id

    def _connect(self):
        sckt = self.context.socket(zmq.REQ)
        sckt.setsockopt(zmq.RCVTIMEO, 1000)
        sckt.connect(self.service_addr % CONNECT_PORT)
        sckt.send_string('')

        try:
            result = sckt.recv_string()
            return result
        except zmq.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                return None
            else:
                raise e

    def _get_func_list(self, prefix=None):
        msg = prefix if prefix else ''
        self.list_sock.send_string(msg)

        flist = StringSet()
        flist.ParseFromString(self.list_sock.recv())
        return flist.keys
