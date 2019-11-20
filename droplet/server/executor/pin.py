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

import droplet.server.utils as sutils
from droplet.server.executor import utils


def pin(pin_socket, pusher_cache, kvs, status, pinned_functions, runtimes,
        exec_counts, user_library):
    msg = pin_socket.recv_string()
    splits = msg.split(':')

    resp_ip, name = splits[0], splits[1]
    sckt = pusher_cache.get(sutils.get_pin_accept_port(resp_ip))

    # We currently only allow one pinned function per container.
    if len(pinned_functions) > 0 or not status.running:
        sutils.error.SerializeToString()
        sckt.send(sutils.error.SerializeToString())
        return

    sckt.send(sutils.ok_resp)

    func = utils.retrieve_function(name, kvs, user_library)

    # The function must exist -- because otherwise the DAG couldn't be
    # registered -- so we keep trying to retrieve it.
    while not func:
        func = utils.retrieve_function(name, kvs, user_library)

    if name not in pinned_functions:
        pinned_functions[name] = func
        status.functions.append(name)

    # Add metadata tracking for the newly pinned functions.
    runtimes[name] = []
    exec_counts[name] = 0
    logging.info('Adding function %s to my local pinned functions.' % (name))


def unpin(unpin_socket, status, pinned_functions, runtimes, exec_counts):
    name = unpin_socket.recv_string()
    if name not in pinned_functions:
        logging.info('Received an unpin request for an unknown function: %s.' %
                     (name))
        return

    logging.info('Removing function %s from my local pinned functions.' %
                 (name))

    # We restart the container after unpinning the function in order to clear
    # the context of the previous function. Exiting with code 0 means that we
    # will get restarted by the wrapper script.
    sys.exit(0)
