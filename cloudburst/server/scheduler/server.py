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

import json
import logging
import sys
import time
import uuid
import zmq

from anna.client import AnnaTcpClient
from anna.zmq_util import SocketCache
import requests

from cloudburst.server.scheduler.call import call_dag, call_function
from cloudburst.server.scheduler.create import (
    create_dag,
    create_function,
    delete_dag
)
from cloudburst.server.scheduler.policy.heft_policy import (
    HeftCloudburstSchedulerPolicy
)
import cloudburst.server.scheduler.utils as sched_utils
import cloudburst.server.utils as sutils
from cloudburst.shared.proto.cloudburst_pb2 import (
    Dag,
    DagCall,
    GenericResponse,
    NO_SUCH_DAG  # Cloudburst's error types
)
from cloudburst.shared.proto.internal_pb2 import (
    ExecutorStatistics,
    SchedulerStatus,
    ThreadStatus
)
from cloudburst.shared.proto.shared_pb2 import StringSet
from cloudburst.shared.utils import (
    CONNECT_PORT,
    DAG_CALL_PORT,
    DAG_CREATE_PORT,
    DAG_DELETE_PORT,
    FUNC_CALL_PORT,
    FUNC_CREATE_PORT,
    LIST_PORT
)

METADATA_THRESHOLD = 5
REPORT_THRESHOLD = 5

logging.basicConfig(filename='log_scheduler.txt', level=logging.INFO,
                    format='%(asctime)s %(message)s')


def scheduler(ip, mgmt_ip, route_addr):

    # If the management IP is not set, we are running in local mode.
    local = (mgmt_ip is None)
    kvs = AnnaTcpClient(route_addr, ip, local=local)

    scheduler_id = str(uuid.uuid4())

    context = zmq.Context(1)

    # A mapping from a DAG's name to its protobuf representation.
    dags = {}

    # Tracks how often a request for each function is received.
    call_frequency = {}

    # Tracks the time interval between successive requests for a particular
    # DAG.
    interarrivals = {}

    # Tracks the most recent arrival for each DAG -- used to calculate
    # interarrival times.
    last_arrivals = {}

    # Maintains a list of all other schedulers in the system, so we can
    # propagate metadata to them.
    schedulers = []

    connect_socket = context.socket(zmq.REP)
    connect_socket.bind(sutils.BIND_ADDR_TEMPLATE % (CONNECT_PORT))

    func_create_socket = context.socket(zmq.REP)
    func_create_socket.bind(sutils.BIND_ADDR_TEMPLATE % (FUNC_CREATE_PORT))

    func_call_socket = context.socket(zmq.REP)
    func_call_socket.bind(sutils.BIND_ADDR_TEMPLATE % (FUNC_CALL_PORT))

    dag_create_socket = context.socket(zmq.REP)
    dag_create_socket.bind(sutils.BIND_ADDR_TEMPLATE % (DAG_CREATE_PORT))

    dag_call_socket = context.socket(zmq.REP)
    dag_call_socket.bind(sutils.BIND_ADDR_TEMPLATE % (DAG_CALL_PORT))

    dag_delete_socket = context.socket(zmq.REP)
    dag_delete_socket.bind(sutils.BIND_ADDR_TEMPLATE % (DAG_DELETE_PORT))

    list_socket = context.socket(zmq.REP)
    list_socket.bind(sutils.BIND_ADDR_TEMPLATE % (LIST_PORT))

    exec_status_socket = context.socket(zmq.PULL)
    exec_status_socket.bind(sutils.BIND_ADDR_TEMPLATE % (sutils.STATUS_PORT))

    sched_update_socket = context.socket(zmq.PULL)
    sched_update_socket.bind(sutils.BIND_ADDR_TEMPLATE %
                             (sutils.SCHED_UPDATE_PORT))

    pin_accept_socket = context.socket(zmq.PULL)
    pin_accept_socket.setsockopt(zmq.RCVTIMEO, 500)
    pin_accept_socket.bind(sutils.BIND_ADDR_TEMPLATE %
                           (sutils.PIN_ACCEPT_PORT))

    requestor_cache = SocketCache(context, zmq.REQ)
    pusher_cache = SocketCache(context, zmq.PUSH)

    poller = zmq.Poller()
    poller.register(connect_socket, zmq.POLLIN)
    poller.register(func_create_socket, zmq.POLLIN)
    poller.register(func_call_socket, zmq.POLLIN)
    poller.register(dag_create_socket, zmq.POLLIN)
    poller.register(dag_call_socket, zmq.POLLIN)
    poller.register(dag_delete_socket, zmq.POLLIN)
    poller.register(list_socket, zmq.POLLIN)
    poller.register(exec_status_socket, zmq.POLLIN)
    poller.register(sched_update_socket, zmq.POLLIN)

    # Start the policy engine.
    policy = HeftCloudburstSchedulerPolicy(pin_accept_socket, pusher_cache,
                                           kvs, ip, local=local)
    policy.update()

    start = time.time()

    while True:
        socks = dict(poller.poll(timeout=1000))

        if connect_socket in socks and socks[connect_socket] == zmq.POLLIN:
            msg = connect_socket.recv_string()
            connect_socket.send_string(route_addr)

        if (func_create_socket in socks and
                socks[func_create_socket] == zmq.POLLIN):
            create_function(func_create_socket, kvs)

        if func_call_socket in socks and socks[func_call_socket] == zmq.POLLIN:
            call_function(func_call_socket, pusher_cache, policy)

        if (dag_create_socket in socks and socks[dag_create_socket]
                == zmq.POLLIN):
            create_dag(dag_create_socket, pusher_cache, kvs, dags, policy,
                       call_frequency)

        if dag_call_socket in socks and socks[dag_call_socket] == zmq.POLLIN:
            call = DagCall()
            call.ParseFromString(dag_call_socket.recv())

            name = call.name

            t = time.time()
            if name in last_arrivals:
                if name not in interarrivals:
                    interarrivals[name] = []

                interarrivals[name].append(t - last_arrivals[name])

            last_arrivals[name] = t

            if name not in dags:
                resp = GenericResponse()
                resp.success = False
                resp.error = NO_SUCH_DAG

                dag_call_socket.send(resp.SerializeToString())
                continue

            dag = dags[name]
            for fname in dag[0].functions:
                call_frequency[fname] += 1

            response = call_dag(call, pusher_cache, dags, policy)
            dag_call_socket.send(response.SerializeToString())

        if (dag_delete_socket in socks and socks[dag_delete_socket] ==
                zmq.POLLIN):
            delete_dag(dag_delete_socket, dags, policy, call_frequency)

        if list_socket in socks and socks[list_socket] == zmq.POLLIN:
            msg = list_socket.recv_string()
            prefix = msg if msg else ''

            resp = StringSet()
            resp.keys.extend(sched_utils.get_func_list(kvs, prefix))

            list_socket.send(resp.SerializeToString())

        if exec_status_socket in socks and socks[exec_status_socket] == \
                zmq.POLLIN:
            status = ThreadStatus()
            status.ParseFromString(exec_status_socket.recv())

            policy.process_status(status)

        if sched_update_socket in socks and socks[sched_update_socket] == \
                zmq.POLLIN:
            status = SchedulerStatus()
            status.ParseFromString(sched_update_socket.recv())

            # Retrieve any DAGs that some other scheduler knows about that we
            # do not yet know about.
            for dname in status.dags:
                if dname not in dags:
                    payload = kvs.get(dname)
                    while None in payload:
                        payload = kvs.get(dname)

                    dag = Dag()
                    dag.ParseFromString(payload[dname].reveal())
                    dags[dag.name] = (dag, sched_utils.find_dag_source(dag))

                    for fname in dag.functions:
                        if fname not in call_frequency:
                            call_frequency[fname] = 0

            policy.update_function_locations(status.function_locations)

        end = time.time()

        if end - start > METADATA_THRESHOLD:
            # Update the scheduler policy-related metadata.
            policy.update()

            # If the management IP is None, that means we arre running in
            # local mode, so there is no need to deal with caches and other
            # schedulers.
            if mgmt_ip:
                schedulers = sched_utils.get_ip_set(
                    sched_utils.get_scheduler_list_address(mgmt_ip),
                    requestor_cache, False)

        if end - start > REPORT_THRESHOLD:
            num_unique_executors = policy.get_unique_executors()
            key = scheduler_id + ':' + str(time.time())
            data = {'key': key, 'count': num_unique_executors}

            status = SchedulerStatus()
            for name in dags.keys():
                status.dags.append(name)

            for fname in policy.function_locations:
                for loc in policy.function_locations[fname]:
                    floc = status.function_locations.add()
                    floc.name = fname
                    floc.ip = loc[0]
                    floc.tid = loc[1]

            msg = status.SerializeToString()

            for sched_ip in schedulers:
                if sched_ip != ip:
                    sckt = pusher_cache.get(
                        sched_utils.get_scheduler_update_address
                        (sched_ip))
                    sckt.send(msg)

            stats = ExecutorStatistics()
            for fname in call_frequency:
                fstats = stats.functions.add()
                fstats.name = fname
                fstats.call_count = call_frequency[fname]
                logging.info('Reporting %d calls for function %s.' %
                             (call_frequency[fname], fname))

                call_frequency[fname] = 0

            for dname in interarrivals:
                dstats = stats.dags.add()
                dstats.name = dname
                dstats.call_count = len(interarrivals[dname]) + 1
                dstats.interarrival.extend(interarrivals[dname])

                interarrivals[dname].clear()

            # We only attempt to send the statistics if we are running in
            # cluster mode. If we are running in local mode, we write them to
            # the local log file.
            if mgmt_ip:
                sckt = pusher_cache.get(sutils.get_statistics_report_address
                                        (mgmt_ip))
                sckt.send(stats.SerializeToString())

            start = time.time()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        conf_file = sys.argv[1]
    else:
        conf_file = 'conf/cloudburst-config.yml'

    conf = sutils.load_conf(conf_file)
    sched_conf = conf['scheduler']

    scheduler(conf['ip'], conf['mgmt_ip'], sched_conf['routing_address'])
