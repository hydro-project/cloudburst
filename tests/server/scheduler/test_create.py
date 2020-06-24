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

import random
import unittest

from anna.lattices import LWWPairLattice, SingleKeyCausalLattice

from cloudburst.server.scheduler.create import (
    create_dag,
    create_function,
    delete_dag
)
from cloudburst.server.scheduler.policy.default_policy import (
    DefaultCloudburstSchedulerPolicy
)
from cloudburst.server.scheduler.utils import get_pin_address, get_unpin_address
import cloudburst.server.utils as sutils
from cloudburst.shared.proto.cloudburst_pb2 import (
    Dag,
    Function,
    GenericResponse,
    NORMAL, MULTI,  # Cloudburst's consistency modes
    DAG_ALREADY_EXISTS, NO_RESOURCES, NO_SUCH_DAG  # Cloudburst's error modes
)
from cloudburst.shared.proto.internal_pb2 import PinFunction
from cloudburst.shared.serializer import Serializer
from tests.mock import kvs_client, zmq_utils
from tests.server.utils import create_linear_dag

serializer = Serializer()


class TestSchedulerCreate(unittest.TestCase):
    '''
    This test suite ensures that the scheduler correctly creates individual
    functions by storing them in the KVS and also correctly creates DAGs by
    sending the correct pin messages to executors and updating the server-side
    metadata when appropriate.
    '''

    def setUp(self):

        self.pusher_cache = zmq_utils.MockPusherCache()
        self.socket = zmq_utils.MockZmqSocket()
        self.pin_socket = zmq_utils.MockZmqSocket()

        self.kvs_client = kvs_client.MockAnnaClient()
        self.ip = '127.0.0.1'

        self.policy = DefaultCloudburstSchedulerPolicy(self.pin_socket,
                                                       self.pusher_cache,
                                                       self.kvs_client, self.ip,
                                                       policy='random',
                                                       random_threshold=0)

    '''
    INDIVIDUAL FUNCTION CREATION TESTS
    '''

    def test_create_function(self):
        '''
        This test creates a new function and checks that it is persisted in
        the KVS in the expected format.
        '''
        # Create a new function message and add it to our socket.
        def func(_, x): x + 1
        function = Function()
        function.name = 'function'
        function.body = serializer.dump(func)

        self.socket.inbox.append(function.SerializeToString())

        # Call the function creation script.
        create_function(self.socket, self.kvs_client, consistency=NORMAL)

        # Check that the function was created correctly.
        kvs_name = sutils.get_func_kvs_name(function.name)
        result = self.kvs_client.get(kvs_name)
        created = result[kvs_name]
        self.assertTrue(created is not None)
        self.assertEqual(type(created), LWWPairLattice)

        created = serializer.load_lattice(created)
        self.assertEqual(func('', 1), created('', 1))

    def test_create_causal_function(self):
        '''
        This test creates a new function and checks that it is persisted in
        the KVS in the expected format in causal mode.
        '''
        # Create a new function message and add it to our socket.
        def func(_, x): x + 1
        function = Function()
        function.name = 'function'
        function.body = serializer.dump(func)

        self.socket.inbox.append(function.SerializeToString())

        # Call the function creation script.
        create_function(self.socket, self.kvs_client, consistency=MULTI)

        # Check that the function was created correctly.
        kvs_name = sutils.get_func_kvs_name(function.name)
        result = self.kvs_client.get(kvs_name)
        created = result[kvs_name]
        self.assertTrue(created is not None)
        self.assertEqual(type(created), SingleKeyCausalLattice)
        self.assertEqual(created.vector_clock, sutils.DEFAULT_VC)
        self.assertEqual(len(created.reveal()), 1)

        created = serializer.load_lattice(created)[0]
        self.assertEqual(func('', 1), created('', 1))

    '''
    DAG CREATION TESTS
    '''

    def test_create_dag(self):
        '''
        This test creates a new DAG, checking that the correct pin messages are
        sent to executors and that it is persisted in the KVS correctly. It
        also checks that the server metadata was updated as expected.
        '''
        # Create a simple two-function DAG and add it to the inbound socket.
        source = 'source'
        sink = 'sink'
        dag_name = 'dag'

        dag = create_linear_dag([None, None], [source, sink], self.kvs_client,
                                dag_name)
        self.socket.inbox.append(dag.SerializeToString())

        # Add relevant metadata to the policy engine.
        address_set = {(self.ip, 1), (self.ip, 2)}
        self.policy.unpinned_cpu_executors.update(address_set)

        # Prepopulate the pin_accept socket with sufficient success messages.
        self.pin_socket.inbox.append(sutils.ok_resp)
        self.pin_socket.inbox.append(sutils.ok_resp)

        # Call the DAG creation method.
        dags = {}
        call_frequency = {}
        create_dag(self.socket, self.pusher_cache, self.kvs_client, dags,
                   self.policy, call_frequency)

        # Test that the correct metadata was created.
        self.assertTrue(dag_name in dags)
        created, dag_source = dags[dag_name]
        self.assertEqual(created, dag)
        self.assertEqual(len(dag_source), 1)
        self.assertEqual(list(dag_source)[0], source)
        self.assertTrue(source in call_frequency)
        self.assertTrue(sink in call_frequency)
        self.assertEqual(call_frequency[source], 0)
        self.assertEqual(call_frequency[sink], 0)

        # Test that the DAG is stored in the KVS correctly.
        result = self.kvs_client.get(dag_name)[dag_name]
        created = Dag()
        created.ParseFromString(result.reveal())
        self.assertEqual(created, dag)

        # Test that the correct response was returned to the user.
        self.assertTrue(len(self.socket.outbox), 1)
        response = GenericResponse()
        response.ParseFromString(self.socket.outbox.pop())
        self.assertTrue(response.success)

        # Test that the correct pin messages were sent.
        self.assertEqual(len(self.pusher_cache.socket.outbox), 2)
        messages = self.pusher_cache.socket.outbox
        function_set = {source, sink}
        for message in messages:
            pin_msg = PinFunction()
            pin_msg.ParseFromString(message)
            self.assertEqual(pin_msg.response_address, self.ip)
            self.assertTrue(pin_msg.name in function_set)
            function_set.discard(pin_msg.name)

        self.assertEqual(len(function_set), 0)

        for address in address_set:
            self.assertTrue(get_pin_address(*address) in
                            self.pusher_cache.addresses)

        # Test that the policy engine has the correct metadata stored.
        self.assertEqual(len(self.policy.unpinned_cpu_executors), 0)
        self.assertEqual(len(self.policy.pending_dags), 0)
        self.assertTrue(source in self.policy.function_locations)
        self.assertTrue(sink in self.policy.function_locations)

        self.assertEqual(len(self.policy.function_locations[source]), 1)
        self.assertEqual(len(self.policy.function_locations[sink]), 1)

    def test_create_dag_already_exists(self):
        '''
        This test attempts to create a DAG that already exists and makes sure
        that the server correctly rejects the request.
        '''
        # Create a simple two-function DAG and add it to the inbound socket.
        source = 'source'
        sink = 'sink'
        dag_name = 'dag'

        dag = create_linear_dag([None, None], [source, sink], self.kvs_client,
                                dag_name)
        self.socket.inbox.append(dag.SerializeToString())

        # Add this to the existing server metadata.
        dags = {dag.name: (dag, {source})}

        # Add relevant metadata to the policy engine.
        address_set = {(self.ip, 1), (self.ip, 2)}
        self.policy.unpinned_cpu_executors.update(address_set)

        # Attempt to create the DAG.
        call_frequency = {}
        create_dag(self.socket, self.pusher_cache, self.kvs_client, dags,
                   self.policy, call_frequency)

        # Check that an error was returned to the user.
        self.assertEqual(len(self.socket.outbox), 1)
        response = GenericResponse()
        response.ParseFromString(self.socket.outbox[0])
        self.assertFalse(response.success)
        self.assertEqual(response.error, DAG_ALREADY_EXISTS)

        # Check that no additional metadata was created or sent.
        self.assertEqual(len(self.pusher_cache.socket.outbox), 0)
        self.assertEqual(len(self.policy.unpinned_cpu_executors), 2)
        self.assertEqual(len(self.policy.function_locations), 0)
        self.assertEqual(len(self.policy.pending_dags), 0)

    def test_create_dag_insufficient_resources(self):
        '''
        This test attempts to create a DAG even though there are not enough
        free executors in the system. It checks that a pin message is attempted
        to be sent, we run out of resources, and then the request is rejected.
        We check that the metadata is properly restored back to its original
        state.
        '''
        # Create a simple two-function DAG and add it to the inbound socket.
        source = 'source'
        sink = 'sink'
        dag_name = 'dag'

        dag = create_linear_dag([None, None], [source, sink], self.kvs_client,
                                dag_name)
        self.socket.inbox.append(dag.SerializeToString())

        # Add relevant metadata to the policy engine, but set the number of
        # executors to fewer than needed.
        address_set = {(self.ip, 1)}
        self.policy.unpinned_cpu_executors.update(address_set)

        # Prepopulate the pin_accept socket with sufficient success messages.
        self.pin_socket.inbox.append(sutils.ok_resp)

        # Attempt to create the DAG.
        dags = {}
        call_frequency = {}
        create_dag(self.socket, self.pusher_cache, self.kvs_client, dags,
                   self.policy, call_frequency)

        # Check that an error was returned to the user.
        self.assertEqual(len(self.socket.outbox), 1)
        response = GenericResponse()
        response.ParseFromString(self.socket.outbox[0])
        self.assertFalse(response.success)
        self.assertEqual(response.error, NO_RESOURCES)

        # Test that the correct pin messages were sent.
        self.assertEqual(len(self.pusher_cache.socket.outbox), 2)
        messages = self.pusher_cache.socket.outbox

        # Checks for the pin message.
        pin_msg = PinFunction()
        pin_msg.ParseFromString(messages[0])
        self.assertEqual(pin_msg.response_address, self.ip)
        self.assertEqual(pin_msg.name, source)

        # Checks for the unpin message.
        self.assertEqual(messages[1], source)

        address = random.sample(address_set, 1)[0]
        addresses = self.pusher_cache.addresses
        self.assertEqual(get_pin_address(*address), addresses[0])
        self.assertEqual(get_unpin_address(*address), addresses[1])

        # Check that no additional messages were sent.
        self.assertEqual(len(self.policy.unpinned_cpu_executors), 0)
        self.assertEqual(len(self.policy.function_locations), 0)
        self.assertEqual(len(self.policy.pending_dags), 0)

        # Check that no additional metadata was created or sent.
        self.assertEqual(len(call_frequency), 0)
        self.assertEqual(len(dags), 0)

    def test_delete_dag(self):
        '''
        We attempt to delete a DAG that has already been created and check to
        ensure that the correct unpin messages are sent to executors and that
        the metadata is updated appropriately.
        '''
        # Create a simple two fucntion DAG and add it to the system metadata.
        source = 'source'
        sink = 'sink'
        dag_name = 'dag'

        dag = create_linear_dag([None, None], [source, sink], self.kvs_client,
                                dag_name)

        dags = {}
        call_frequency = {}
        dags[dag.name] = (dag, {source})
        call_frequency[source] = 100
        call_frequency[sink] = 100

        # Add the correct metadata to the policy engine.
        source_location = (self.ip, 1)
        sink_location = (self.ip, 2)
        self.policy.function_locations[source] = {source_location}
        self.policy.function_locations[sink] = {sink_location}

        self.socket.inbox.append(dag.name)

        # Attempt to delete the DAG.
        delete_dag(self.socket, dags, self.policy, call_frequency)

        # Check that the correct unpin messages were sent.
        messages = self.pusher_cache.socket.outbox
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0], source)
        self.assertEqual(messages[1], sink)

        addresses = self.pusher_cache.addresses
        self.assertEqual(len(addresses), 2)
        self.assertEqual(addresses[0], get_unpin_address(*source_location))
        self.assertEqual(addresses[1], get_unpin_address(*sink_location))

        # Check that the server metadata was updated correctly.
        self.assertEqual(len(dags), 0)
        self.assertEqual(len(call_frequency), 0)

        # Check that the correct message was sent to the user.
        self.assertEqual(len(self.socket.outbox), 1)
        response = GenericResponse()
        response.ParseFromString(self.socket.outbox.pop())
        self.assertTrue(response.success)

    def test_delete_nonexistent_dag(self):
        '''
        This test attempts to delete a nonexistent DAG and ensures that no
        metadata is affected by the failed operation.
        '''

        # Make a request to delete an unknown DAG.
        self.socket.inbox.append('dag')
        delete_dag(self.socket, {}, self.policy, {})

        # Ensure that an error response is sent to the user.
        self.assertEqual(len(self.socket.outbox), 1)
        response = GenericResponse()
        response.ParseFromString(self.socket.outbox[0])
        self.assertFalse(response.success)
        self.assertEqual(response.error, NO_SUCH_DAG)

        # Check that no additional messages were sent and no metadata changed.
        self.assertEqual(len(self.pusher_cache.socket.outbox), 0)
        self.assertEqual(len(self.policy.function_locations), 0)
        self.assertEqual(len(self.policy.unpinned_cpu_executors), 0)

    def test_create_gpu_dag_no_resources(self):
        # Create a simple two-function DAG and add it to the inbound socket.
        dag_name = 'dag'

        dag = create_linear_dag([None], ['fn'], self.kvs_client,
                                dag_name)
        dag.functions[0].gpu = True
        self.socket.inbox.append(dag.SerializeToString())

        dags = {}
        call_frequency = {}

        create_dag(self.socket, self.pusher_cache, self.kvs_client, dags,
                   self.policy, call_frequency)

        # Check that an error was returned to the user.
        self.assertEqual(len(self.socket.outbox), 1)
        response = GenericResponse()
        response.ParseFromString(self.socket.outbox[0])
        self.assertFalse(response.success)
        self.assertEqual(response.error, NO_RESOURCES)

        # Test that the correct pin messages were sent.
        self.assertEqual(len(self.pusher_cache.socket.outbox), 0)

        # Check that no additional messages were sent.
        self.assertEqual(len(self.policy.unpinned_cpu_executors), 0)
        self.assertEqual(len(self.policy.function_locations), 0)
        self.assertEqual(len(self.policy.pending_dags), 0)

        # Check that no additional metadata was created or sent.
        self.assertEqual(len(call_frequency), 0)
        self.assertEqual(len(dags), 0)

    def test_create_gpu_dag(self):
        # Create a simple two-function DAG and add it to the inbound socket.
        dag_name = 'dag'
        fn = 'fn'

        dag = create_linear_dag([None], [fn], self.kvs_client,
                                dag_name)
        dag.functions[0].gpu = True
        self.socket.inbox.append(dag.SerializeToString())

        dags = {}
        call_frequency = {}

        address_set = {(self.ip, 1)}
        self.policy.unpinned_gpu_executors.update(address_set)

        self.pin_socket.inbox.append(sutils.ok_resp)

        create_dag(self.socket, self.pusher_cache, self.kvs_client, dags,
                   self.policy, call_frequency)

        # Test that the correct metadata was created.
        self.assertTrue(dag_name in dags)
        created, dag_source = dags[dag_name]
        self.assertEqual(created, dag)
        self.assertEqual(len(dag_source), 1)
        self.assertEqual(list(dag_source)[0], fn)
        self.assertTrue(fn in call_frequency)
        self.assertEqual(call_frequency[fn], 0)

        # Test that the DAG is stored in the KVS correctly.
        result = self.kvs_client.get(dag_name)[dag_name]
        created = Dag()
        created.ParseFromString(result.reveal())
        self.assertEqual(created, dag)

        # Test that the correct response was returned to the user.
        self.assertTrue(len(self.socket.outbox), 1)
        response = GenericResponse()
        response.ParseFromString(self.socket.outbox.pop())
        self.assertTrue(response.success)

        # Test that the correct pin messages were sent.
        self.assertEqual(len(self.pusher_cache.socket.outbox), 1)
        messages = self.pusher_cache.socket.outbox
        function_set = {fn}
        for message in messages:
            pin_msg = PinFunction()
            pin_msg.ParseFromString(message)
            self.assertEqual(pin_msg.response_address, self.ip)
            self.assertTrue(pin_msg.name in function_set)
            function_set.discard(pin_msg.name)

        self.assertEqual(len(function_set), 0)

        for address in address_set:
            self.assertTrue(get_pin_address(*address) in
                            self.pusher_cache.addresses)

        # Test that the policy engine has the correct metadata stored.
        self.assertEqual(len(self.policy.unpinned_cpu_executors), 0)
        self.assertEqual(len(self.policy.pending_dags), 0)
        self.assertTrue(fn in self.policy.function_locations)

        self.assertEqual(len(self.policy.function_locations[fn]), 1)
