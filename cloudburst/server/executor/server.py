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
import os
import sys
import time

from anna.client import AnnaTcpClient
from anna.zmq_util import SocketCache
import zmq

from cloudburst.server import utils as sutils
from cloudburst.server.executor import utils
from cloudburst.server.executor.call import exec_function, exec_dag_function
from cloudburst.server.executor.pin import pin, unpin
from cloudburst.server.executor.user_library import CloudburstUserLibrary
from cloudburst.shared.anna_ipc_client import AnnaIpcClient
from cloudburst.shared.proto.cloudburst_pb2 import (
    DagSchedule,
    DagTrigger,
    MULTIEXEC # Cloudburst's execution types
)
from cloudburst.shared.proto.internal_pb2 import (
    CPU, GPU, # Cloudburst's executor types
    ExecutorStatistics,
    ThreadStatus,
)

REPORT_THRESH = 5
BATCH_SIZE_MAX = 20


def executor(ip, mgmt_ip, schedulers, thread_id):
    logging.basicConfig(filename='log_executor.txt', level=logging.INFO,
                        format='%(asctime)s %(message)s')

    # Check what resources we have access to, set as an environment variable.
    if os.getenv('EXECUTOR_TYPE', 'CPU') == 'GPU':
        exec_type = GPU
    else:
        exec_type = CPU

    context = zmq.Context(1)
    poller = zmq.Poller()

    pin_socket = context.socket(zmq.PULL)
    pin_socket.bind(sutils.BIND_ADDR_TEMPLATE % (sutils.PIN_PORT + thread_id))

    unpin_socket = context.socket(zmq.PULL)
    unpin_socket.bind(sutils.BIND_ADDR_TEMPLATE % (sutils.UNPIN_PORT +
                                                   thread_id))

    exec_socket = context.socket(zmq.PULL)
    exec_socket.bind(sutils.BIND_ADDR_TEMPLATE % (sutils.FUNC_EXEC_PORT +
                                                  thread_id))

    dag_queue_socket = context.socket(zmq.PULL)
    dag_queue_socket.bind(sutils.BIND_ADDR_TEMPLATE % (sutils.DAG_QUEUE_PORT
                                                       + thread_id))

    dag_exec_socket = context.socket(zmq.PULL)
    dag_exec_socket.bind(sutils.BIND_ADDR_TEMPLATE % (sutils.DAG_EXEC_PORT
                                                      + thread_id))

    self_depart_socket = context.socket(zmq.PULL)
    self_depart_socket.bind(sutils.BIND_ADDR_TEMPLATE %
                            (sutils.SELF_DEPART_PORT + thread_id))

    pusher_cache = SocketCache(context, zmq.PUSH)

    poller = zmq.Poller()
    poller.register(pin_socket, zmq.POLLIN)
    poller.register(unpin_socket, zmq.POLLIN)
    poller.register(exec_socket, zmq.POLLIN)
    poller.register(dag_queue_socket, zmq.POLLIN)
    poller.register(dag_exec_socket, zmq.POLLIN)
    poller.register(self_depart_socket, zmq.POLLIN)

    # If the management IP is set to None, that means that we are running in
    # local mode, so we use a regular AnnaTcpClient rather than an IPC client.
    if mgmt_ip:
        client = AnnaIpcClient(thread_id, context)
        local = False
    else:
        client = AnnaTcpClient('127.0.0.1', '127.0.0.1', local=True, offset=1)
        local = True

    user_library = CloudburstUserLibrary(context, pusher_cache, ip, thread_id,
                                      client)

    status = ThreadStatus()
    status.ip = ip
    status.tid = thread_id
    status.running = True
    status.type = exec_type
    utils.push_status(schedulers, pusher_cache, status)

    departing = False

    # Maintains a request queue for each function pinned on this executor. Each
    # function will have a set of request IDs mapped to it, and this map stores
    # a schedule for each request ID.
    queue = {}

    # Tracks the actual function objects that are pinned to this executor.
    function_cache = {}

    # Tracks runtime cost of excuting a DAG function.
    runtimes = {}

    # If multiple triggers are necessary for a function, track the triggers as
    # we receive them. This is also used if a trigger arrives before its
    # corresponding schedule.
    received_triggers = {}

    # Tracks when we received a function request, so we can report end-to-end
    # latency for the whole executio.
    receive_times = {}

    # Tracks the number of requests we are finishing for each function pinned
    # here.
    exec_counts = {}

    # Tracks the end-to-end runtime of each DAG request for which we are the
    # sink function.
    dag_runtimes = {}

    # A map with KVS keys and their corresponding deserialized payloads.
    cache = {}

    # A map which tracks the most recent DAGs for which we have finished our
    # work.
    finished_executions = {}

    # The set of pinned functions and whether they support batching. NOTE: This
    # is only a set for local mode -- in cluster mode, there will only be one
    # pinned function per executor.
    batching = False

    # Internal metadata to track thread utilization.
    report_start = time.time()
    event_occupancy = {'pin': 0.0,
                       'unpin': 0.0,
                       'func_exec': 0.0,
                       'dag_queue': 0.0,
                       'dag_exec': 0.0}
    total_occupancy = 0.0

    while True:
        socks = dict(poller.poll(timeout=1000))

        if pin_socket in socks and socks[pin_socket] == zmq.POLLIN:
            work_start = time.time()
            batching = pin(pin_socket, pusher_cache, client, status,
                           function_cache, runtimes, exec_counts, user_library,
                           local, batching)
            utils.push_status(schedulers, pusher_cache, status)

            elapsed = time.time() - work_start
            event_occupancy['pin'] += elapsed
            total_occupancy += elapsed

        if unpin_socket in socks and socks[unpin_socket] == zmq.POLLIN:
            work_start = time.time()
            unpin(unpin_socket, status, function_cache, runtimes,
                  exec_counts)
            utils.push_status(schedulers, pusher_cache, status)

            elapsed = time.time() - work_start
            event_occupancy['unpin'] += elapsed
            total_occupancy += elapsed

        if exec_socket in socks and socks[exec_socket] == zmq.POLLIN:
            work_start = time.time()
            exec_function(exec_socket, client, user_library, cache,
                          function_cache)
            user_library.close()

            utils.push_status(schedulers, pusher_cache, status)

            elapsed = time.time() - work_start
            event_occupancy['func_exec'] += elapsed
            total_occupancy += elapsed

        if dag_queue_socket in socks and socks[dag_queue_socket] == zmq.POLLIN:
            work_start = time.time()

            # In order to effectively support batching, we have to make sure we
            # dequeue lots of schedules in addition to lots of triggers. Right
            # now, we're not going to worry about supporting batching here,
            # just on the trigger dequeue side, but we still have to dequeue
            # all schedules we've received. We just process them one at a time.
            while True:
                schedule = DagSchedule()
                try:
                    msg = dag_queue_socket.recv(zmq.DONTWAIT)
                except zmq.ZMQError as e:
                    if e.errno == zmq.EAGAIN:
                        break # There are no more messages.
                    else:
                        raise e # Unexpected error.

                schedule.ParseFromString(msg)
                fname = schedule.target_function

                logging.info('Received a schedule for DAG %s (%s), function %s.' %
                             (schedule.dag.name, schedule.id, fname))

                if fname not in queue:
                    queue[fname] = {}

                queue[fname][schedule.id] = schedule

                if (schedule.id, fname) not in receive_times:
                    receive_times[(schedule.id, fname)] = time.time()

                # In case we receive the trigger before we receive the schedule, we
                # can trigger from this operation as well.
                trkey = (schedule.id, fname)
                fref = None

                # Check to see what type of execution this function is.
                for ref in schedule.dag.functions:
                    if ref.name == fname:
                        fref = ref

                if (trkey in received_triggers and
                        ((len(received_triggers[trkey]) == len(schedule.triggers))
                         or (fref.type == MULTIEXEC))):

                    triggers = list(received_triggers[trkey].values())

                    if fname not in function_cache:
                        logging.error('%s not in function cache', fname)
                        utils.generate_error_response(schedule, client, fname)
                        continue

                    # We don't support actual batching for when we receive a
                    # schedule before a trigger, so everything is just a batch of
                    # size 1 if anything.
                    success = exec_dag_function(pusher_cache, client,
                                                [triggers], function_cache[fname],
                                                [schedule], user_library,
                                                dag_runtimes, cache, schedulers,
                                                batching)[0]
                    user_library.close()

                    del received_triggers[trkey]
                    if success:
                        del queue[fname][schedule.id]

                        fend = time.time()
                        fstart = receive_times[(schedule.id, fname)]
                        runtimes[fname].append(fend - fstart)
                        exec_counts[fname] += 1

                        finished_executions[(schedule.id, fname)] = time.time()

            elapsed = time.time() - work_start
            event_occupancy['dag_queue'] += elapsed
            total_occupancy += elapsed

        if dag_exec_socket in socks and socks[dag_exec_socket] == zmq.POLLIN:
            work_start = time.time()

            # How many messages to dequeue -- BATCH_SIZE_MAX or 1 depending on
            # the function configuration.
            if batching:
                count = BATCH_SIZE_MAX
            else:
                count = 1

            trigger_keys = set()

            for _ in range(count): # Dequeue count number of messages.
                trigger = DagTrigger()

                try:
                    msg = dag_exec_socket.recv(zmq.DONTWAIT)
                except zmq.ZMQError as e:
                    if e.errno == zmq.EAGAIN: # There are no more messages.
                        break
                    else:
                        raise e # Unexpected error.

                trigger.ParseFromString(msg)

                # We have received a repeated trigger for a function that has
                # already finished executing.
                if trigger.id in finished_executions:
                    continue

                fname = trigger.target_function
                logging.info('Received a trigger for schedule %s, function %s.' %
                             (trigger.id, fname))

                key = (trigger.id, fname)
                trigger_keys.add(key)
                if key not in received_triggers:
                    received_triggers[key] = {}

                if (trigger.id, fname) not in receive_times:
                    receive_times[(trigger.id, fname)] = time.time()

                received_triggers[key][trigger.source] = trigger

            # Only execute the functions for which we have received a schedule.
            # Everything else will wait.
            for tid, fname in list(trigger_keys):
                if fname not in queue or tid not in queue[fname]:
                    trigger_keys.remove((tid, fname))

            if len(trigger_keys) == 0:
                continue

            fref = None
            schedule = queue[fname][list(trigger_keys)[0][0]] # Pick a random schedule to check.
            # Check to see what type of execution this function is.
            for ref in schedule.dag.functions:
                if ref.name == fname:
                    fref = ref
                    break

            # Compile a list of all the trigger sets for which we have
            # enough triggers.
            trigger_sets = []
            schedules = []
            for key in trigger_keys:
                if (len(received_triggers[key]) == len(schedule.triggers)) or \
                        fref.type == MULTIEXEC:

                    if fref.type == MULTIEXEC:
                        triggers = [trigger]
                    else:
                        triggers = list(received_triggers[key].values())

                    if fname not in function_cache:
                        logging.error('%s not in function cache', fname)
                        utils.generate_error_response(schedule, client, fname)
                        continue

                    trigger_sets.append(triggers)
                    schedule = queue[fname][key[0]]
                    schedules.append(schedule)


            # Pass all of the trigger_sets into exec_dag_function at once.
            # We also include the batching variaible to make sure we know
            # whether to pass lists into the fn or not.
            successes = exec_dag_function(pusher_cache, client,
                                          trigger_sets,
                                          function_cache[fname],
                                          schedules, user_library,
                                          dag_runtimes, cache,
                                          schedulers, batching)
            user_library.close()
            del received_triggers[key]

            for key, success in zip(trigger_keys, successes):
                if success:
                    del queue[fname][key[0]] # key[0] is trigger.id.

                    fend = time.time()
                    fstart = receive_times[key]
                    runtimes[fname].append(fend - fstart)
                    exec_counts[fname] += 1

                    finished_executions[(schedule.id, fname)] = time.time()

            elapsed = time.time() - work_start
            event_occupancy['dag_exec'] += elapsed
            total_occupancy += elapsed

        if self_depart_socket in socks and socks[self_depart_socket] == \
                zmq.POLLIN:
            # This message does not matter.
            self_depart_socket.recv()

            logging.info('Preparing to depart. No longer accepting requests ' +
                         'and clearing all queues.')

            status.ClearField('functions')
            status.running = False
            utils.push_status(schedulers, pusher_cache, status)

            departing = True

        # periodically report function occupancy
        report_end = time.time()
        if report_end - report_start > REPORT_THRESH:
            cache.clear()

            utilization = total_occupancy / (report_end - report_start)
            status.utilization = utilization

            # Periodically report my status to schedulers with the utilization
            # set.
            utils.push_status(schedulers, pusher_cache, status)

            logging.info('Total thread occupancy: %.6f' % (utilization))

            for event in event_occupancy:
                occ = event_occupancy[event] / (report_end - report_start)
                logging.info('\tEvent %s occupancy: %.6f' % (event, occ))
                event_occupancy[event] = 0.0

            stats = ExecutorStatistics()
            for fname in runtimes:
                if exec_counts[fname] > 0:
                    fstats = stats.functions.add()
                    fstats.name = fname
                    fstats.call_count = exec_counts[fname]
                    fstats.runtime.extend(runtimes[fname])

                runtimes[fname].clear()
                exec_counts[fname] = 0

            for dname in dag_runtimes:
                dstats = stats.dags.add()
                dstats.name = dname

                dstats.runtimes.extend(dag_runtimes[dname])

                dag_runtimes[dname].clear()

            # If we are running in cluster mode, mgmt_ip will be set, and we
            # will report our status and statistics to it. Otherwise, we will
            # write to the local conf file
            if mgmt_ip:
                sckt = pusher_cache.get(sutils.get_statistics_report_address
                                        (mgmt_ip))
                sckt.send(stats.SerializeToString())

                sckt = pusher_cache.get(utils.get_util_report_address(mgmt_ip))
                sckt.send(status.SerializeToString())
            else:
                logging.info(stats)

            status.ClearField('utilization')
            report_start = time.time()
            total_occupancy = 0.0

            # Periodically clear any old functions we have cached that we are
            # no longer accepting requests for.
            del_list = []
            for fname in queue:
                if len(queue[fname]) == 0 and fname not in status.functions:
                    del_list.append(fname)
                    del function_cache[fname]
                    del runtimes[fname]
                    del exec_counts[fname]

            for fname in del_list:
                del queue[fname]

            del_list = []
            for tid in finished_executions:
                if (time.time() - finished_executions[tid]) > 10:
                    del_list.append(tid)

            for tid in del_list:
                del finished_executions[tid]

            # If we are departing and have cleared our queues, let the
            # management server know, and exit the process.
            if departing and len(queue) == 0:
                sckt = pusher_cache.get(utils.get_depart_done_addr(mgmt_ip))
                sckt.send_string(ip)

                # We specifically pass 1 as the exit code when ending our
                # process so that the wrapper script does not restart us.
                sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        conf_file = sys.argv[1]
    else:
        conf_file = 'conf/cloudburst-config.yml'

    conf = sutils.load_conf(conf_file)
    exec_conf = conf['executor']

    executor(conf['ip'], conf['mgmt_ip'], exec_conf['scheduler_ips'],
             int(exec_conf['thread_id']))
