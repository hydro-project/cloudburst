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

import cloudburst.server.utils as sutils
from cloudburst.shared.proto.cloudburst_pb2 import (
    NORMAL,
    EXECUTION_ERROR
)
from cloudburst.shared.serializer import Serializer

from anna.lattices import (
    MultiKeyCausalLattice,
)

serializer = Serializer()

UTILIZATION_REPORT_PORT = 7003
EXECUTOR_DEPART_PORT = 7005
CACHE_VERISON_GC_PORT = 7200


def generate_error_response(schedule, client, fname):
    sutils.error.error = EXECUTION_ERROR
    result = ('ERROR: ' + fname + ' not in function cache', sutils.error.SerializeToString())
    if schedule.consistency == NORMAL:
        result = serializer.dump_lattice(result)
        client.put(schedule.output_key, result)
    else:
        result = serializer.dump_lattice(result, MultiKeyCausalLattice)
        client.causal_put(schedule.output_key, result)

def retrieve_function(name, kvs, user_library, consistency=NORMAL):
    kvs_name = sutils.get_func_kvs_name(name)

    if consistency == NORMAL:
        # This means that the function is stored in an LWWPairLattice.
        lattice = kvs.get(kvs_name)[kvs_name]
        if lattice:
            result = serializer.load_lattice(lattice)
        else:
            return None
    else:
        # This means that the function is stored in an SingleKeyCausalLattice.
        _, result = kvs.causal_get([kvs_name])
        lattice = result[kvs_name]

        if lattice:
            # If there are multiple concurrent values, we arbitrarily pick the
            # first one listed.
            result = serializer.load_lattice(lattice)[0]
        else:
            return None

    # Check to see if the result is a tuple. This means that the first object
    # in the tuple is a class that we can initialize, and the second value is a
    # set of initialization args. Otherwise, we just return the retrieved
    # function.
    if type(result) == tuple:
        cls = result[0]
        if type(result[1]) != tuple:
            result[1] = (result[1],)

        args = (user_library,) + result[1]
        obj = cls(*args)
        result = obj.run

    return result


def push_status(schedulers, pusher_cache, status):
    msg = status.SerializeToString()

    # tell all the schedulers your new status
    for sched in schedulers:
        sckt = pusher_cache.get(get_status_address(sched))
        sckt.send(msg)


def get_status_address(ip):
    return 'tcp://' + ip + ':' + str(sutils.STATUS_PORT)


def get_util_report_address(mgmt_ip):
    return 'tcp://' + mgmt_ip + ':' + str(UTILIZATION_REPORT_PORT)


def get_depart_done_addr(mgmt_ip):
    return 'tcp://' + mgmt_ip + ':' + str(EXECUTOR_DEPART_PORT)


def get_cache_gc_address(ip):
    return 'tcp://' + ip + ':' + str(CACHE_VERISON_GC_PORT)

def get_continuation_address(schedulers):
    # If this variable is not set, that means we are running in local mode, so
    # we just use 127.0.0.1 as the scheduler address.
    addr = random.choice(schedulers)
    return  'tcp://' + addr + ':' +  str(sutils.CONTINUATION_PORT)
