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

from cloudburst.server.executor.pin import pin, unpin
from cloudburst.server.executor.user_library import CloudburstUserLibrary
from cloudburst.shared.proto.cloudburst_pb2 import GenericResponse
from cloudburst.shared.proto.internal_pb2 import PinFunction, ThreadStatus
from tests.mock import kvs_client, zmq_utils
from tests.server.utils import create_function


class TestExecutorPin(unittest.TestCase):
    '''
    Tests for the function pinning component of the executor, in order to
    ensure that functions are correctly retrieve, metadata is correctly
    generated, functions are not pinned when they are not supposed to be, and
    that unpinning works correctly.
    '''

    def setUp(self):
        self.kvs_client = kvs_client.MockAnnaClient()
        self.socket = zmq_utils.MockZmqSocket()
        self.pusher_cache = zmq_utils.MockPusherCache()

        self.ip = '127.0.0.1'
        self.status = ThreadStatus()
        self.status.ip = self.ip
        self.status.tid = 0
        self.status.running = True

        self.pinned_functions = {}
        self.runtimes = {}
        self.exec_counts = {}

        self.user_library = CloudburstUserLibrary(zmq_utils.MockZmqContext(),
                                                  self.pusher_cache, self.ip, 0,
                                                  self.kvs_client)

    def test_succesful_pin(self):
        '''
        This test executes a pin operation that is supposed to be successful,
        and it checks to make sure that that the correct metadata for execution
        and reporting is generated.
        '''
        # Create a new function in the KVS.
        fname = 'incr'

        def func(_, x): return x + 1
        create_function(func, self.kvs_client, fname)

        # Create a pin message and put it into the socket.
        msg = PinFunction(name=fname, response_address=self.ip)
        self.socket.inbox.append(msg.SerializeToString())

        # Execute the pin operation.
        pin(self.socket, self.pusher_cache, self.kvs_client, self.status,
            self.pinned_functions, self.runtimes, self.exec_counts,
            self.user_library, False, False)

        # Check that the correct messages were sent and the correct metadata
        # created.
        self.assertEqual(len(self.pusher_cache.socket.outbox), 1)
        response = GenericResponse()
        response.ParseFromString(self.pusher_cache.socket.outbox[0])
        self.assertTrue(response.success)

        self.assertEqual(func('', 1), self.pinned_functions[fname]('', 1))
        self.assertTrue(fname in self.pinned_functions)
        self.assertTrue(fname in self.runtimes)
        self.assertTrue(fname in self.exec_counts)
        self.assertTrue(fname in self.status.functions)

    def test_occupied_pin(self):
        '''
        This test attempts to pin a function onto a node where another function
        is already pinned. We currently only allow one pinned node per machine,
        so this operation should fail.
        '''
        # Create a new function in the KVS.
        fname = 'incr'

        def func(_, x): return x + 1
        create_function(func, self.kvs_client, fname)

        # Create a pin message and put it into the socket.
        msg = PinFunction(name=fname, response_address=self.ip)
        self.socket.inbox.append(msg.SerializeToString())

        # Add an already pinned_function, so that we reject the request.
        self.pinned_functions['square'] = lambda _, x: x * x
        self.runtimes['square'] = []
        self.exec_counts['square'] = []
        self.status.functions.append('square')

        # Execute the pin operation.
        pin(self.socket, self.pusher_cache, self.kvs_client, self.status,
            self.pinned_functions, self.runtimes, self.exec_counts,
            self.user_library, False, False)

        # Check that the correct messages were sent and the correct metadata
        # created.
        self.assertEqual(len(self.pusher_cache.socket.outbox), 1)
        response = GenericResponse()
        response.ParseFromString(self.pusher_cache.socket.outbox[0])
        self.assertFalse(response.success)

        # Make sure that none of the metadata was corrupted with this failed
        # pin attempt
        self.assertTrue(fname not in self.pinned_functions)
        self.assertTrue(fname not in self.runtimes)
        self.assertTrue(fname not in self.exec_counts)
        self.assertTrue(fname not in self.status.functions)

    def test_unpin(self):
        '''
        This test sends an unpin operation to an executor that has the function
        pinned. Executors restart after unpin operations, so this test
        explicitly waits for the test to raise a `SystemExit`.
        '''
        # Add an already pinned_function.
        fname = 'square'

        def square(_, x): x * x
        self.pinned_functions[fname] = square
        self.runtimes[fname] = []
        self.exec_counts[fname] = []
        self.status.functions.append(fname)

        self.socket.inbox.append(fname)
        with self.assertRaises(SystemExit) as restart:
            unpin(self.socket, self.status, self.pinned_functions,
                  self.runtimes, self.exec_counts)

        self.assertEqual(restart.exception.code, 0)

    def test_bad_unpin(self):
        '''
        This test attempts to unpin a function that does not currently exist at
        this executor. The result should be no change.
        '''
        # Set the message to an unknown function
        fname = 'bad_func'
        self.socket.inbox.append(fname)

        try:
            unpin(self.socket, self.status, self.pinned_functions,
                  self.runtimes, self.exec_counts)
        except SystemExit:
            self.fail('Unpin attempted to exit when the function was unknown.')
