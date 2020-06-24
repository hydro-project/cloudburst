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

import unittest

from cloudburst.server.scheduler.call import call_function, call_dag
from cloudburst.server.scheduler.policy.default_policy import (
    DefaultCloudburstSchedulerPolicy
)
from cloudburst.server.scheduler import utils
from cloudburst.server import utils as sutils
from cloudburst.shared.proto.cloudburst_pb2 import (
    Dag,
    DagCall,
    DagSchedule,
    DagTrigger,
    FunctionCall,
    GenericResponse,
    NO_RESOURCES,  # Cloudburst's error types
    NORMAL  # Cloudburst's consistency modes
)
from cloudburst.shared.proto.internal_pb2 import ThreadStatus
from cloudburst.shared.reference import CloudburstReference
from cloudburst.shared.serializer import Serializer
from tests.mock import kvs_client, zmq_utils

serializer = Serializer()


class TestSchedulerCall(unittest.TestCase):
    '''
    Tests the call forwarding functionality of the scheduler. This primarily
    evaluates the sanity of the placement policy, and there are further
    policy-specific tests in tests.server.schedule.policy.test_default_policy.
    This test suite also ensures that the correct metadata is forwarded and
    that the messages are sent to the correct locations.
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

        # Add an executor to the policy engine by default.
        status = ThreadStatus()
        status.ip = self.ip
        status.tid = 0
        self.executor_key = (status.ip, status.tid)
        self.policy.unpinned_cpu_executors.add(self.executor_key)

    '''
    INDIVIDUAL FUNCTION CALL TESTS
    '''

    def test_call_function_no_refs(self):
        '''
        A basic test that makes sure that an executor is successfully chosen
        when there is only one possible executor to choose from.
        '''
        # Create a new function call for a function that doesn't exist.
        call = FunctionCall()
        call.name = 'function'
        call.request_id = 12

        # Add an argument to thhe function.
        val = call.arguments.values.add()
        serializer.dump(2, val)
        self.socket.inbox.append(call.SerializeToString())

        # Execute the scheduling policy.
        call_function(self.socket, self.pusher_cache, self.policy)

        # Check that the correct number of messages were sent.
        self.assertEqual(len(self.socket.outbox), 1)
        self.assertEqual(len(self.pusher_cache.socket.outbox), 1)

        # Extract and deserialize the messages.
        response = GenericResponse()
        forwarded = FunctionCall()
        response.ParseFromString(self.socket.outbox[0])
        forwarded.ParseFromString(self.pusher_cache.socket.outbox[0])

        self.assertTrue(response.success)
        self.assertEqual(response.response_id, forwarded.response_key)
        self.assertEqual(forwarded.name, call.name)
        self.assertEqual(forwarded.request_id, call.request_id)

        # Makes sure that the correct executor was chosen.
        self.assertEqual(len(self.pusher_cache.addresses), 1)
        self.assertEqual(self.pusher_cache.addresses[0],
                         utils.get_exec_address(*self.executor_key))

    def test_call_function_with_refs(self):
        '''
        Creates a scenario where the policy should deterministically pick the
        same executor to run a request on: There is one reference, and it's
        cached only on the node we create in this test.
        '''
        # Add a new executor for which we will construct cached references.
        ip_address = '192.168.0.1'
        new_key = (ip_address, 2)
        self.policy.unpinned_cpu_executors.add(new_key)

        # Create a new reference and add its metadata.
        ref_name = 'reference'
        self.policy.key_locations[ref_name] = [ip_address]

        # Create a function call that asks for this reference.
        call = FunctionCall()
        call.name = 'function'
        call.request_id = 12
        val = call.arguments.values.add()
        serializer.dump(CloudburstReference(ref_name, True), val)
        self.socket.inbox.append(call.SerializeToString(0))

        # Execute the scheduling policy.
        call_function(self.socket, self.pusher_cache, self.policy)

        # Check that the correct number of messages were sent.
        self.assertEqual(len(self.socket.outbox), 1)
        self.assertEqual(len(self.pusher_cache.socket.outbox), 1)

        # Extract and deserialize the messages.
        response = GenericResponse()
        forwarded = FunctionCall()
        response.ParseFromString(self.socket.outbox[0])
        forwarded.ParseFromString(self.pusher_cache.socket.outbox[0])

        self.assertTrue(response.success)
        self.assertEqual(response.response_id, forwarded.response_key)
        self.assertEqual(forwarded.name, call.name)
        self.assertEqual(forwarded.request_id, call.request_id)

        # Makes sure that the correct executor was chosen.
        self.assertEqual(len(self.pusher_cache.addresses), 1)
        self.assertEqual(self.pusher_cache.addresses[0],
                         utils.get_exec_address(*new_key))

    def test_function_call_no_resources(self):
        '''
        Constructs a scenario where there are no available resources in the
        system, and ensures that the scheduler correctly returns an error to
        the user.
        '''
        # Clear all executors from the system.
        self.policy.thread_statuses.clear()
        self.policy.unpinned_cpu_executors.clear()

        # Create a function call.
        call = FunctionCall()
        call.name = 'function'
        call.request_id = 12
        val = call.arguments.values.add()
        serializer.dump(2, val)
        self.socket.inbox.append(call.SerializeToString())

        # Execute the scheduling policy.
        call_function(self.socket, self.pusher_cache, self.policy)

        # Check that the correct number of messages were sent.
        self.assertEqual(len(self.socket.outbox), 1)
        self.assertEqual(len(self.pusher_cache.socket.outbox), 0)

        # Extract and deserialize the messages.
        response = GenericResponse()
        response.ParseFromString(self.socket.outbox[0])

        self.assertFalse(response.success)
        self.assertEqual(response.error, NO_RESOURCES)

    '''
    DAG FUNCTION CALL TESTS
    '''

    def test_dag_call_no_refs(self):
        '''
        Tests a DAG call without any references. We do not currently have a
        test for selecting a DAG call with references because the reference
        logic in the default policy is the same for individual functions
        (tested above) and for DAGs.
        '''

        # Create a simple DAG.
        source = 'source'
        sink = 'sink'
        dag, source_address, sink_address = self._construct_dag_with_locations(
            source, sink)

        # Create a DAG call that corresponds to this new DAG.
        call = DagCall()
        call.name = dag.name
        call.consistency = NORMAL
        call.output_key = 'output_key'
        call.client_id = '0'

        # Execute the scheduling policy.
        call_dag(call, self.pusher_cache, {dag.name: (dag, {source})},
                 self.policy)

        # Check that the correct number of messages were sent.
        self.assertEqual(len(self.pusher_cache.socket.outbox), 3)

        # Extract each of the two schedules and ensure that they are correct.
        source_schedule = DagSchedule()
        source_schedule.ParseFromString(self.pusher_cache.socket.outbox[0])
        self._verify_dag_schedule(source, 'BEGIN', source_schedule, dag, call)

        sink_schedule = DagSchedule()
        sink_schedule.ParseFromString(self.pusher_cache.socket.outbox[1])
        self._verify_dag_schedule(sink, source, sink_schedule, dag, call)

        # Make sure that only trigger was sent, and it was for the DAG source.
        trigger = DagTrigger()
        trigger.ParseFromString(self.pusher_cache.socket.outbox[2])
        self.assertEqual(trigger.id, source_schedule.id)
        self.assertEqual(trigger.target_function, source)
        self.assertEqual(trigger.source, 'BEGIN')
        self.assertEqual(len(trigger.version_locations), 0)
        self.assertEqual(len(trigger.dependencies), 0)

        # Ensure that all the the destination addresses match the addresses we
        # expect.
        self.assertEqual(len(self.pusher_cache.addresses), 3)
        self.assertEqual(self.pusher_cache.addresses[0],
                         utils.get_queue_address(*source_address))
        self.assertEqual(self.pusher_cache.addresses[1],
                         utils.get_queue_address(*sink_address))
        self.assertEqual(
            self.pusher_cache.addresses[2], sutils.get_dag_trigger_address(
                ':'.join(map(lambda s: str(s), source_address))))

    '''
    HELPER FUNCTIONS
    '''

    def _verify_dag_schedule(self, function, trigger, schedule, dag, call):
        self.assertEqual(schedule.dag, dag)
        self.assertEqual(schedule.target_function, function)
        self.assertEqual(len(schedule.triggers), 1)
        self.assertEqual(schedule.triggers[0], trigger)
        self.assertEqual(len(schedule.locations), len(dag.functions))
        self.assertEqual(schedule.client_id, call.client_id)
        self.assertEqual(schedule.output_key, call.output_key)

    def _construct_dag_with_locations(self, source, sink):
        # Construct a simple, two-function DAG.
        dag = Dag()
        dag.name = 'dag'
        dag.functions.extend([Dag.FunctionReference(name=source),
                              Dag.FunctionReference(name=sink)])
        link = dag.connections.add()
        link.source = source
        link.sink = sink

        # Add the relevant metadata to the policy engine.
        source_address = (self.ip, 1)
        sink_address = (self.ip, 2)
        self.policy.function_locations[source] = [source_address]
        self.policy.function_locations[sink] = [sink_address]

        return dag, source_address, sink_address
