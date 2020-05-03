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
import time
import unittest

from anna.lattices import LWWPairLattice

from cloudburst.server.scheduler.policy.default_policy import (
    DefaultCloudburstSchedulerPolicy
)
from cloudburst.server.scheduler.utils import get_cache_ip_key
import cloudburst.server.utils as sutils
from cloudburst.shared.proto.cloudburst_pb2 import Dag
from cloudburst.shared.proto.internal_pb2 import ThreadStatus, SchedulerStatus
from cloudburst.shared.proto.shared_pb2 import StringSet
from cloudburst.shared.serializer import Serializer
from tests.mock import kvs_client, zmq_utils

serializer = Serializer()
logging.disable(logging.CRITICAL)


class TestDefaultSchedulerPolicy(unittest.TestCase):
    '''
    This test suite tests the parts of the default scheduler policy that aren't
    covered by the scheduler creation and call test cases. In particular, most
    of these test cases have to do with the metadata management that is invoked
    periodically from the server.
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
                                                       random_threshold=0)

    def tearDown(self):
        # Clear all policy metadata.
        self.policy.running_counts.clear()
        self.policy.backoff.clear()
        self.policy.key_locations.clear()
        self.policy.unpinned_executors.clear()
        self.policy.function_locations.clear()
        self.policy.pending_dags.clear()
        self.policy.thread_statuses.clear()

    def test_policy_ignore_overloaded(self):
        '''
        This test ensures that the policy engine correctly ignores nodes that
        have either explicitly reported a high load recently or have received
        many calls in the recent past.
        '''
        # Create two executors, one of which has received too many requests,
        # and the other of which has reported high load.
        address_set = {(self.ip, 1), (self.ip, 2)}
        self.policy.unpinned_executors.update(address_set)

        self.policy.backoff[(self.ip, 1)] = time.time()
        self.policy.running_counts[self.ip, 2] = set()
        for _ in range(1100):
            time.sleep(.0001)
            self.policy.running_counts[(self.ip, 2)].add(time.time())

        # Ensure that we have returned None because both our valid executors
        # were overloaded.
        result = self.policy.pick_executor([])
        self.assertEqual(result, None)

    def test_pin_reject(self):
        '''
        This test explicitly rejects a pin request from the policy and ensures
        that it tries again to pin on another node.
        '''
        # Create two unpinned executors.
        address_set = {(self.ip, 1), (self.ip, 2)}
        self.policy.unpinned_executors.update(address_set)

        # Create one failing and one successful response.
        self.pin_socket.inbox.append(sutils.ok_resp)
        self.pin_socket.inbox.append(sutils.error.SerializeToString())

        success = self.policy.pin_function(
            'dag', Dag.FunctionReference(name='function'), [])
        self.assertTrue(success)

        # Ensure that both remaining executors have been removed from unpinned
        # and that the DAG commit is pending.
        self.assertEqual(len(self.policy.unpinned_executors), 0)
        self.assertEqual(len(self.policy.pending_dags), 1)

    def test_process_status(self):
        '''
        This test ensures that when a new status update is received from an
        executor, the local server metadata is correctly updated in the normal
        case.
        '''
        # Construct a new thread status to pass into the policy engine.
        function_name = 'square'
        status = ThreadStatus()
        status.running = True
        status.ip = self.ip
        status.tid = 1
        status.functions.append(function_name)
        status.utilization = 0.10

        # Process the newly created status.
        self.policy.process_status(status)

        status.tid = 2
        status.utilization = 0.90
        self.policy.process_status(status)

        key = (status.ip, status.tid)

        self.assertTrue(key not in self.policy.unpinned_executors)
        self.assertTrue(key in self.policy.function_locations[function_name])
        self.assertTrue(key in self.policy.backoff)

    def test_process_status_restart(self):
        '''
        This tests checks that when we receive a status update from a restarted
        executor, we correctly update its metadata.
        '''
        # Construct a new thread status to pass into the policy engine.
        function_name = 'square'
        status = ThreadStatus()
        status.running = True
        status.ip = self.ip
        status.tid = 1
        status.functions.append(function_name)
        status.utilization = 0

        key = (status.ip, status.tid)

        # Add metadata to the policy engine to make it think that this node
        # used to have a pinned function.
        self.policy.function_locations[function_name] = {key}
        self.policy.thread_statuses[key] = status

        # Clear the status' pinned functions (i.e., restart).
        status = ThreadStatus()
        status.ip = self.ip
        status.tid = 1
        status.running = True
        status.utilization = 0

        # Process the status and check the metadata.
        self.policy.process_status(status)

        self.assertEqual(len(self.policy.function_locations[function_name]), 0)
        self.assertTrue(key in self.policy.unpinned_executors)

    def test_process_status_not_running(self):
        '''
        This test passes in a status for a server that is leaving the system
        and ensures that all the metadata reflects this fact after the
        processing.
        '''
        # Construct a new thread status to prime the policy engine with.
        function_name = 'square'
        status = ThreadStatus()
        status.running = True
        status.ip = self.ip
        status.tid = 1
        status.functions.append(function_name)
        status.utilization = 0

        key = (status.ip, status.tid)

        # Add metadata to the policy engine to make it think that this node
        # used to have a pinned function.
        self.policy.function_locations[function_name] = {key}
        self.policy.thread_statuses[key] = status

        # Clear the status' fields to report that it is turning off.
        status = ThreadStatus()
        status.running = False
        status.ip = self.ip
        status.tid = 1

        # Process the status and check the metadata.
        self.policy.process_status(status)

        self.assertTrue(key not in self.policy.thread_statuses)
        self.assertTrue(key not in self.policy.unpinned_executors)
        self.assertEqual(len(self.policy.function_locations[function_name]), 0)

    def test_metadata_update(self):
        '''
        This test calls the periodic metadata update protocol and ensures that
        the correct metadata is removed from the system and that the correct
        metadata is retrieved/updated from the KVS.
        '''
        # Create two executor threads on separate machines.
        old_ip = '127.0.0.1'
        new_ip = '192.168.0.1'
        old_executor = (old_ip, 1)
        new_executor = (new_ip, 2)

        old_status = ThreadStatus()
        old_status.ip = old_ip
        old_status.tid = 1
        old_status.running = True

        new_status = ThreadStatus()
        new_status.ip = new_ip
        new_status.tid = 2
        new_status.running = True

        self.policy.thread_statuses[old_executor] = old_status
        self.policy.thread_statuses[new_executor] = new_status

        # Add two executors, one with old an old backoff and one with a new
        # time.
        self.policy.backoff[old_executor] = time.time() - 10
        self.policy.backoff[new_executor] = time.time()

        # For the new executor, add 10 old running times and 10 new ones.
        self.policy.running_counts[new_executor] = set()
        for _ in range(10):
            time.sleep(.0001)
            self.policy.running_counts[new_executor].add(time.time() - 10)

        for _ in range(10):
            time.sleep(.0001)
            self.policy.running_counts[new_executor].add(time.time())

        # Publish some caching metadata into the KVS for each executor.
        old_set = StringSet()
        old_set.keys.extend(['key1', 'key2', 'key3'])
        new_set = StringSet()
        new_set.keys.extend(['key3', 'key4', 'key5'])
        self.kvs_client.put(get_cache_ip_key(old_ip),
                            LWWPairLattice(0, old_set.SerializeToString()))
        self.kvs_client.put(get_cache_ip_key(new_ip),
                            LWWPairLattice(0, new_set.SerializeToString()))

        self.policy.update()

        # Check that the metadata has been correctly pruned.
        self.assertEqual(len(self.policy.backoff), 1)
        self.assertTrue(new_executor in self.policy.backoff)
        self.assertEqual(len(self.policy.running_counts[new_executor]), 10)

        # Check that the caching information is correct.
        self.assertTrue(len(self.policy.key_locations['key1']), 1)
        self.assertTrue(len(self.policy.key_locations['key2']), 1)
        self.assertTrue(len(self.policy.key_locations['key3']), 2)
        self.assertTrue(len(self.policy.key_locations['key4']), 1)
        self.assertTrue(len(self.policy.key_locations['key5']), 1)

        self.assertTrue(old_ip in self.policy.key_locations['key1'])
        self.assertTrue(old_ip in self.policy.key_locations['key2'])
        self.assertTrue(old_ip in self.policy.key_locations['key3'])
        self.assertTrue(new_ip in self.policy.key_locations['key3'])
        self.assertTrue(new_ip in self.policy.key_locations['key4'])
        self.assertTrue(new_ip in self.policy.key_locations['key5'])

    def test_update_function_locations(self):
        '''
        This test ensures that the update_function_locations method correctly
        updates local metadata about which functions are pinned on which nodes.
        '''
        # Construct function location metadata received from another node.
        locations = {}
        function1 = 'square'
        function2 = 'incr'
        key1 = ('127.0.0.1', 0)
        key2 = ('127.0.0.2', 0)
        key3 = ('192.168.0.1', 0)
        locations[function1] = [key1, key3]
        locations[function2] = [key2, key3]

        # Serialize the location map in the expected protobuf.
        status = SchedulerStatus()
        for function_name in locations:
            for ip, tid in locations[function_name]:
                location = status.function_locations.add()
                location.name = function_name
                location.ip = ip
                location.tid = tid

        self.policy.update_function_locations(status.function_locations)

        self.assertEqual(len(self.policy.function_locations[function1]), 2)
        self.assertEqual(len(self.policy.function_locations[function2]), 2)

        self.assertTrue(key1 in self.policy.function_locations[function1])
        self.assertTrue(key3 in self.policy.function_locations[function1])
        self.assertTrue(key2 in self.policy.function_locations[function2])
        self.assertTrue(key3 in self.policy.function_locations[function2])
