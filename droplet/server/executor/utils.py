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

import droplet.server.utils as sutils
from droplet.shared.proto.droplet_pb2 import NORMAL
from droplet.shared.serializer import Serializer

serializer = Serializer()

UTILIZATION_REPORT_PORT = 7003
EXECUTOR_DEPART_PORT = 7005
CACHE_VERISON_GC_PORT = 7200


def retrieve_function(name, kvs, consistency=NORMAL):
    kvs_name = sutils.get_func_kvs_name(name)

    if consistency == NORMAL:
        # This means that the function is stored in an LWWPairLattice.
        lattice = kvs.get(kvs_name)[kvs_name]
        if lattice:
            return serializer.load_lattice(lattice)
        else:
            return None
    else:
        # This means that the function is stored in an SingleKeyCausalLattice.
        _, result = kvs.causal_get([kvs_name])
        lattice = result[kvs_name]

        if lattice:
            # If there are multiple concurrent values, we arbitrarily pick the
            # first one listed.
            return serializer.load_lattice(lattice)[0]
        else:
            return None


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
