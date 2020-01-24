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

from cloudburst.server.executor.user_library import CloudburstUserLibrary
from cloudburst.shared.serializer import Serializer
from tests.mock.kvs_client import MockAnnaClient
from tests.mock.zmq_utils import MockPusherCache, MockZmqContext

serializer = Serializer()


class TestUserLibrary(unittest.TestCase):
    '''
    Test cases for the user library that is passed into each function upon
    execution. Most user library functionality is currently a wrapper around
    the KVS, so we have dummy tests for that, and we focus on testing send and
    receive.
    '''

    def setUp(self):
        self.context = MockZmqContext()
        self.pusher_cache = MockPusherCache()
        self.ip = '127.0.0.1'
        self.kvs_client = MockAnnaClient()

        self.user_library = CloudburstUserLibrary(self.context, self.pusher_cache,
                                               self.ip, 0, self.kvs_client)

    def test_kvs_io(self):
        '''
        A dummy test that ensures that retrieving a key from the mock KVS
        returns the correct value.
        '''
        key = 'key'
        self.user_library.put(key, 2)

        result = self.user_library.get(key)

        self.assertEqual(result, 2)

    def test_send(self):
        '''
        Tests that send correctly populates the expected metadata and puts the
        message on the wire.
        '''
        msg = 'hello!'
        dest = (self.ip, 0)
        self.user_library.send(dest, msg)

        send_socket = self.pusher_cache.socket
        self.assertEqual(len(send_socket.outbox), 1)

        sender, received = send_socket.outbox[0]
        self.assertEqual(msg, received)
        self.assertEqual(dest, sender)

    def test_receive(self):
        '''
        Tests that receive correctly retrieves one or more messages off the
        wire and returns them to the user.
        '''
        sender = (self.ip, 0)
        message1 = 'hello'
        message2 = 'goodbye'

        self.context.sckt.inbox.append((sender, message1))
        self.context.sckt.inbox.append((sender, message2))

        msgs = self.user_library.recv()

        self.assertEqual(len(msgs), 2)
        self.assertEqual(msgs[0][0], sender)
        self.assertEqual(msgs[1][0], sender)
        self.assertEqual(msgs[0][1], message2)
        self.assertEqual(msgs[1][1], message1)
