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
import sys

import cloudburst.server.utils as sutils
from cloudburst.server.executor import utils
from cloudburst.shared.proto.internal_pb2 import PinFunction


def pin(pin_socket, pusher_cache, kvs, status, function_cache, runtimes,
        exec_counts, user_library, local, batching):
    serialized = pin_socket.recv()
    pin_msg = PinFunction()
    pin_msg.ParseFromString(serialized)

    sckt = pusher_cache.get(sutils.get_pin_accept_port(pin_msg.response_address))
    name = pin_msg.name

    # We currently only allow one pinned function per container in non-local
    # mode.
    if not local:
        if (len(function_cache) > 0 and name not in function_cache):
            sutils.error.SerializeToString()
            sckt.send(sutils.error.SerializeToString())
            return batching

    func = utils.retrieve_function(pin_msg.name, kvs, user_library)

    # The function must exist -- because otherwise the DAG couldn't be
    # registered -- so we keep trying to retrieve it.
    while not func:
        func = utils.retrieve_function(name, kvs, user_library)

    if name not in function_cache:
        function_cache[name] = func

    if name not in status.functions:
        status.functions.append(name)

    # Add metadata tracking for the newly pinned functions.
    runtimes[name] = []
    exec_counts[name] = 0
    logging.info('Adding function %s to my local pinned functions.' % (name))

    if pin_msg.batching and len(status.functions) > 1:
        raise RuntimeError('There is more than one pinned function (we are'
                           + ' operating in local mode), and the function'
                           + ' attempting to be pinned has batching enabled. This'
                           + ' is not allowed -- you can only use batching in'
                           + ' cluster mode or in local mode with one function.')

    sckt.send(sutils.ok_resp)

    return pin_msg.batching


def unpin(unpin_socket, status, function_cache, runtimes, exec_counts):
    name = unpin_socket.recv_string()
    if name not in function_cache:
        logging.info('Received an unpin request for an unknown function: %s.' %
                     (name))
        return

    logging.info('Removing function %s from my local pinned functions.' %
                 (name))

    # We restart the container after unpinning the function in order to clear
    # the context of the previous function. Exiting with code 0 means that we
    # will get restarted by the wrapper script.
    sys.exit(0)
