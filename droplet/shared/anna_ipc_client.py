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

from anna.base_client import BaseAnnaClient
import zmq

from droplet.shared.proto.anna_pb2 import (
    NONE,  # The undefined lattice type
    NO_ERROR, KEY_DNE,  # Anna's error modes
    KeyResponse
)
from droplet.shared.proto.causal_pb2 import (
    CausalRequest,
    CausalResponse
)
from droplet.shared.proto.droplet_pb2 import (
    SINGLE, MULTI  # Droplet's consistency modes
)

GET_REQUEST_ADDR = "ipc:///requests/get"
PUT_REQUEST_ADDR = "ipc:///requests/put"

GET_RESPONSE_ADDR_TEMPLATE = "ipc:///requests/get_%d"
PUT_RESPONSE_ADDR_TEMPLATE = "ipc:///requests/put_%d"


class AnnaIpcClient(BaseAnnaClient):
    def __init__(self, thread_id=0, context=None):
        if not context:
            self.context = zmq.Context(1)
        else:
            self.context = context

        self.get_response_address = GET_RESPONSE_ADDR_TEMPLATE % thread_id
        self.put_response_address = PUT_RESPONSE_ADDR_TEMPLATE % thread_id

        self.get_request_socket = self.context.socket(zmq.PUSH)
        self.get_request_socket.connect(GET_REQUEST_ADDR)

        self.put_request_socket = self.context.socket(zmq.PUSH)
        self.put_request_socket.connect(PUT_REQUEST_ADDR)

        self.get_response_socket = self.context.socket(zmq.PULL)
        self.get_response_socket.setsockopt(zmq.RCVTIMEO, 5000)
        self.get_response_socket.bind(self.get_response_address)

        self.put_response_socket = self.context.socket(zmq.PULL)
        self.put_response_socket.setsockopt(zmq.RCVTIMEO, 5000)
        self.put_response_socket.bind(self.put_response_address)

        self.rid = 0

        # Set this to None because we do not use the address cache, but the
        # super class checks to see if there is one.
        self.address_cache = None

    def get(self, keys):
        if type(keys) != list:
            keys = [keys]

        request, _ = self._prepare_data_request(keys)
        request.response_address = self.get_response_address
        self.get_request_socket.send(request.SerializeToString())

        kv_pairs = {}
        for key in keys:
            kv_pairs[key] = None

        try:
            msg = self.get_response_socket.recv()
        except zmq.ZMQError as e:
            logging.error("Unexpected error while requesting keys %s: %s." %
                          (str(keys), str(e)))

            return kv_pairs
        else:
            resp = KeyResponse()
            resp.ParseFromString(msg)

            for tp in resp.tuples:
                if tp.error == KEY_DNE or tp.lattice_type == NONE:
                    continue

                kv_pairs[tp.key] = self._deserialize(tp)

            return kv_pairs

    def causal_get(self, keys, future_read_set=set(), key_version_locations={},
                   consistency=SINGLE, client_id=0):
        if type(keys) != list:
            keys = list(keys)

        request, _ = self._prepare_causal_data_request(client_id, keys,
                                                       consistency)

        for addr in key_version_locations:
            request.key_version_locations[addr].key_versions.extend(
                                key_version_locations[addr].key_versions)

        request.response_address = self.get_response_address
        request.future_read_set.extend(future_read_set)

        self.get_request_socket.send(request.SerializeToString())

        # Initialize all responses to None, and only change them if we have a
        # valid response for that key.
        kv_pairs = {}
        for key in keys:
            kv_pairs[key] = None

        try:
            msg = self.get_response_socket.recv()
        except zmq.ZMQError as e:
            logging.error("Unexpected error while requesting keys %s: %s." %
                          (str(keys), str(e)))

            return ((None, None), kv_pairs)
        else:
            kv_pairs = {}
            resp = CausalResponse()
            resp.ParseFromString(msg)

            for tp in resp.tuples:
                if tp.error == KEY_DNE:
                    return (None, kv_pairs)

                val = self._deserialize(tp)

                # We resolve multiple concurrent versions by randomly picking
                # the first listed value.
                kv_pairs[tp.key] = (val.vector_clock.reveal(),
                                    val.values.reveal()[0])

            if len(resp.key_versions) != 0:
                return ((resp.key_version_query_address,
                        resp.key_versions), kv_pairs)
            else:
                return ((None, None), kv_pairs)

    def put(self, key, value):
        request, tuples = self._prepare_data_request([key])

        # We can assume this is tuples[0] because we only support one put
        # operation at a time.
        tup = tuples[0]
        tup.payload, tup.lattice_type = self._serialize(value)

        request.response_address = self.put_response_address
        self.put_request_socket.send(request.SerializeToString())

        try:
            msg = self.put_response_socket.recv()
        except zmq.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                logging.error("Request for %s timed out!" % (str(key)))
            else:
                logging.error("Unexpected ZMQ error: %s." % (str(e)))

            return False
        else:
            resp = KeyResponse()
            resp.ParseFromString(msg)

            return resp.tuples[0].error == NO_ERROR

    def causal_put(self, key, mk_causal_value, client_id):
        request, tuples = self._prepare_causal_data_request(client_id, key,
                                                            MULTI)

        # We can assume this is tuples[0] because we only support one put
        # operation at a time.
        tuples[0].payload, _ = self._serialize(mk_causal_value)

        request.response_address = self.put_response_address
        self.put_request_socket.send(request.SerializeToString())

        # If we get a response from the causal cache in this case, it is
        # guaranteed to succeed, so we don't need to inspect the response
        # message. The only failure case is if the request times out.
        try:
            self.put_response_socket.recv()
        except zmq.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                logging.error("Request for %s timed out!" % (str(key)))
            else:
                logging.error("Unexpected ZMQ error: %s." % (str(e)))

            return False
        else:
            return True

    def _prepare_causal_data_request(self, client_id, keys, consistency):
        request = CausalRequest()
        request.consistency = consistency
        request.client_id = str(client_id)

        tuples = []
        for key in keys:
            ct = request.add_tuples()
            ct.key = key
            tuples.append(ct)

        return request, tuples

    @property
    def response_address(self):
        # We define this property because the default interface expects it to
        # be set. However, we manually override it in this client based on what
        # the request type is, so we return an empty string here.
        return ''

    def _get_request_id(self):
        # Override the _get_request_id method to avoid the default
        # implementation.
        self.rid += 1
        return str(self.rid)
