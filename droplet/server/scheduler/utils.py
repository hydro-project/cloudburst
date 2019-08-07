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

from anna.lattices import SetLattice

import droplet.server.utils as sutils
from droplet.shared.proto.shared_pb2 import StringSet

FUNCOBJ = 'funcs/index-allfuncs'

NUM_EXEC_THREADS = 3

EXECUTORS_PORT = 7002
SCHEDULERS_PORT = 7004


def get_func_list(client, prefix, fullname=False):
    funcs = client.get(FUNCOBJ)[FUNCOBJ]
    if not funcs:
        return []

    prefix = sutils.FUNC_PREFIX + prefix
    decoded = map(lambda v: str(v, 'utf-8'), funcs.reveal())
    result = list(filter(lambda fn: fn.startswith(prefix), decoded))

    if not fullname:
        result = list(map(lambda fn: fn.split(sutils.FUNC_PREFIX)[-1], result))

    return result


def put_func_list(client, funclist):
    # Convert to a set in order to remove duplicates.
    funclist = set(funclist)

    lattice = SetLattice(funclist)
    client.put(FUNCOBJ, lattice)


def get_cache_ip_key(ip):
    return 'ANNA_METADATA|cache_ip|' + ip


def get_pin_address(ip, tid):
    return 'tcp://' + ip + ':' + str(sutils.PIN_PORT + tid)


def get_unpin_address(ip, tid):
    return 'tcp://' + ip + ':' + str(sutils.UNPIN_PORT + tid)


def get_exec_address(ip, tid):
    return 'tcp://' + ip + ':' + str(sutils.FUNC_EXEC_PORT + tid)


def get_queue_address(ip, tid):
    return 'tcp://' + ip + ':' + str(sutils.DAG_QUEUE_PORT + int(tid))


def get_scheduler_list_address(mgmt_ip):
    return 'tcp://' + mgmt_ip + ':' + str(SCHEDULERS_PORT)


def get_scheduler_update_address(ip):
    return 'tcp://' + ip + ':' + str(sutils.SCHED_UPDATE_PORT)


def get_ip_set(request_ip, socket_cache, exec_threads=True):
    sckt = socket_cache.get(request_ip)

    # we can send an empty request because the response is always thes same
    sckt.send(b'')

    ips = StringSet()
    ips.ParseFromString(sckt.recv())
    result = set()

    if exec_threads:
        for ip in ips.keys:
            for i in range(NUM_EXEC_THREADS):
                result.add((ip, i))

        return result
    else:
        return set(ips.keys)


def find_dag_source(dag):
    sinks = set()
    for conn in dag.connections:
        sinks.add(conn.sink)

    funcs = set(dag.functions)
    for sink in sinks:
        funcs.remove(sink)

    return funcs
