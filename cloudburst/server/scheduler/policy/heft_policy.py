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
import random
import time

import zmq

from cloudburst.shared.proto.cloudburst_pb2 import GenericResponse
from cloudburst.shared.proto.shared_pb2 import StringSet
from cloudburst.server.scheduler.policy.base_policy import (
    BaseCloudburstSchedulerPolicy
)
from cloudburst.server.scheduler.utils import (
    get_cache_ip_key,
    get_pin_address,
    get_unpin_address
)

sys_random = random.SystemRandom()

import cloudburst.server.utils as sutils
BASE_FUNCTION_COST = 1
KVS_ACCESS_COST = 100


class HeftCloudburstSchedulerPolicy(BaseCloudburstSchedulerPolicy):

    def __init__(self, pin_accept_socket, pusher_cache, kvs_client, ip,
                 random_threshold=0.20, local=False):
        # This scheduler's IP address.
        self.ip = ip

        # A socket to listen for confirmations of pin operations' successes.
        self.pin_accept_socket = pin_accept_socket

        # A cache for zmq.PUSH sockets.
        self.pusher_cache = pusher_cache

        # This thread's Anna KVS client.
        self.kvs_client = kvs_client

        # A map to track how many requests have been routed to each executor in
        # the most recent timeslice.
        self.running_counts = {}

        # A map to track nodes which have recently reported high load. These
        # nodes will not be sent requests until after a cooling period.
        self.backoff = {}

        # A map to track which caches are currently caching which keys.
        self.key_locations = {}

        # Executors which currently have no functions pinned on them.
        self.unpinned_executors = set()

        # A map from function names to the executor(s) on which they are
        # pinned.
        self.function_locations = {}

        # A map to sequester function location information until all functions
        # in a DAG have accepted their pin operations.
        self.pending_dags = {}

        # The most recently reported statuses of each executor thread.
        self.thread_statuses = {}

        # This quantifies how many requests should be routed stochastically
        # rather than by policy.
        self.random_threshold = random_threshold

        self.unique_executors = set()

        # Indicates if we are running in local mode
        self.local = local

    def get_unique_executors(self):
        count = len(self.unique_executors)
        self.unique_executors = set()
        return count

    # Returns a mapping from function to executor IP for a DAG
    def pick_executors(self, functions, dag):
        executor_map = {}
        available = {}
        actual_finish_time = {}
        for function_name in list(functions.keys()):
            executors = set(self.function_locations[function_name])
            discard = []
            for e in executors:
                if e in self.backoff or \
                        ((e in self.running_counts and (len(self.running_counts[e]) > 1000))
                         and sys_random.random() > self.random_threshold):
                    discard.append(e)
                elif e not in available:
                    available[e] = 0
            for executor in discard:
                executors.discard(executor)
            if len(executors) == 0:
                return {None: None}
            executor_map[function_name] = executors

        costs, cost_matrix = self.get_average_costs(functions, executor_map)
        upward_ranks = self.compute_upward_ranks(list(functions.keys()), costs, dag)
        ranks_desc = [k for k, v in sorted(upward_ranks.items(), key=lambda x: x[1], reverse=True)]

        schedule = {}

        while len(ranks_desc) > 0:
            function = ranks_desc.pop(0)
            # Find the executor that gives this function the earliest finish time
            earliest_finish_time = float('inf')
            best_executor = None
            for executor in executor_map[function]:
                # Find the maximum actual finish time of this function's predecessors
                max_aft = float('-inf')
                for predecessor in sutils.get_dag_predecessors(dag, function):
                    max_aft = max(max_aft, actual_finish_time[predecessor])

                # Compare with actual availability of executor to get earliest start time
                earliest_start_time = max(available[executor], max_aft)

                # Add cost of this function on this executor to earliest_start_time to get
                # earliest_finish_time
                eft = cost_matrix[function][executor] + earliest_start_time
                if eft < earliest_finish_time:
                    earliest_finish_time = eft
                    best_executor = executor

            actual_finish_time[function] = earliest_finish_time
            available[best_executor] = earliest_finish_time
            schedule[function] = best_executor

        for _, ip in schedule.items():
            if ip not in self.running_counts:
                self.running_counts[ip] = set()
            self.running_counts[ip].add(time.time())
            self.unique_executors.add(ip)

        return schedule

    # Returns the average cost of each function on its available executors,
    # based on the location of its arguments.
    def get_average_costs(self, functions, executors):
        costs = {}
        cost_matrix = {}
        for function_name, refs in functions.items():
            key_loc = {}
            for ref in refs:
                if ref.key in self.key_locations:
                    key_loc[ref.key] = self.key_locations[ref.key]
                else:
                    key_loc[ref.key] = []

            total_cost = 0
            cost_matrix[function_name] = {}
            for executor in executors[function_name]:
                kvs_accesses = 0
                for ref in refs:
                    if executor[0] not in key_loc[ref.key]:
                        kvs_accesses += 1
                cost = BASE_FUNCTION_COST + KVS_ACCESS_COST * kvs_accesses
                total_cost += cost
                cost_matrix[function_name][executor] = cost

            costs[function_name] = total_cost / len(executors[function_name])
        return costs, cost_matrix

    def compute_upward_ranks(self, functions, costs, dag):
        # Get reverse topological sort of functions
        func_list = self.reverse_topological_sort(functions, dag)
        rank = {}
        for function in func_list:
            max_successor = 0
            for f in sutils.get_dag_successors(dag, function):
                max_successor = max(max_successor, rank[f])
            rank[function] = costs[function] + max_successor
        return rank

    def reverse_topological_sort(self, functions, dag):
        visited = [False] * len(functions)
        stack = []
        for i in range(len(functions)):
            if not visited[i]:
                self.reverse_top_sort_util(i, visited, stack, functions, dag)
        return stack

    def reverse_top_sort_util(self, curr, visited, stack, functions, dag):
        visited[curr] = True
        for func in sutils.get_dag_predecessors(dag, functions[curr]):
            index = functions.index(func)
            if not visited[index]:
                self.reverse_top_sort_util(index, visited, stack, functions, dag)
        stack.insert(0, functions[curr])

    def pick_executor(self, references, function_name=None):
        # Construct a map which maps from IP addresses to the number of
        # relevant arguments they have cached. For the time begin, we will
        # just pick the machine that has the most number of keys cached.
        arg_map = {}

        if function_name:
            executors = set(self.function_locations[function_name])
        else:
            executors = set(self.unpinned_executors)

        for executor in self.backoff:
            executors.discard(executor)

        # Generate a list of all the keys in the system; if any of these nodes
        # have received many requests, we remove them from the executor set
        # with high probability.
        for key in self.running_counts:
            if (len(self.running_counts[key]) > 1000 and sys_random.random() >
                    self.random_threshold):
                executors.discard(key)

        if len(executors) == 0:
            return None

        executor_ips = set([e[0] for e in executors])

        # For each reference, we look at all the places where they are cached,
        # and we calculate which IP address has the most references cached.
        for reference in references:
            if reference.key in self.key_locations:
                ips = self.key_locations[reference.key]

                for ip in ips:
                    # Only choose this cached node if its a valid executor for
                    # our purposes.
                    if ip in executor_ips:
                        if ip not in arg_map:
                            arg_map[ip] = 0

                        arg_map[ip] += 1

        # Get the IP address that has the maximum value in the arg_map, if
        # there are any values.
        max_ip = None
        if arg_map:
            max_ip = max(arg_map, key=arg_map.get)

        # Pick a random thead from our potential executors that is on that IP
        # address with the most keys cached.
        if max_ip:
            candidates = list(filter(lambda e: e[0] == max_ip, executors))
            max_ip = sys_random.choice(candidates)

        # If max_ip was never set (i.e. there were no references cached
        # anywhere), or with some random chance, we assign this node to a
        # random executor.
        if not max_ip or sys_random.random() < self.random_threshold:
            max_ip = sys_random.sample(executors, 1)[0]

        if max_ip not in self.running_counts:
            self.running_counts[max_ip] = set()

        self.running_counts[max_ip].add(time.time())

        # Remove this IP/tid pair from the system's metadata until it notifies
        # us that it is available again, but only do this for non-DAG requests.
        if not function_name:
            self.unpinned_executors.discard(max_ip)

        self.unique_executors.add(max_ip)
        return max_ip

    def pin_function(self, dag_name, function_name):
        # If there are no functions left to choose from, then we return None,
        # indicating that we ran out of resources to use.
        if len(self.unpinned_executors) == 0:
            return False

        if dag_name not in self.pending_dags:
            self.pending_dags[dag_name] = []

        # Make a copy of the set of executors, so that we don't modify the
        # system's metadata.
        candidates = set(self.unpinned_executors)

        while True:
            # Pick a random executor from the set of candidates and attempt to
            # pin this function there.
            node, tid = sys_random.sample(candidates, 1)[0]

            sckt = self.pusher_cache.get(get_pin_address(node, tid))
            msg = self.ip + ':' + function_name
            sckt.send_string(msg)

            response = GenericResponse()
            try:
                response.ParseFromString(self.pin_accept_socket.recv())
            except zmq.ZMQError:
                logging.error('Pin operation to %s:%d timed out. Retrying.' %
                              (node, tid))
                continue

            # Do not use this executor either way: If it rejected, it has
            # something else pinned, and if it accepted, it has pinned what we
            # just asked it to pin.
            # In local model allow executors to have multiple functions pinned
            if not self.local:
                self.unpinned_executors.discard((node, tid))
                candidates.discard((node, tid))

            if response.success:
                # The pin operation succeeded, so we return the node and thread
                # ID to the caller.
                self.pending_dags[dag_name].append((function_name, (node,
                                                                    tid)))
                return True
            else:
                # The pin operation was rejected, remove node and try again.
                logging.error('Node %s:%d rejected pin for %s. Retrying.'
                              % (node, tid, function_name))

                continue

    def commit_dag(self, dag_name):
        for function_name, location in self.pending_dags[dag_name]:
            if function_name not in self.function_locations:
                self.function_locations[function_name] = set()

            self.function_locations[function_name].add(location)

        del self.pending_dags[dag_name]

    def discard_dag(self, dag, pending=False):
        pinned_locations = []
        if pending:
            if dag.name in self.pending_dags:
                # If the DAG was pending, we can simply look at the sequestered
                # pending metadata.
                pinned_locations = list(self.pending_dags[dag.name])
                del self.pending_dags[dag.name]
        else:
            # If the DAG was not pinned, we construct a set of all the
            # locations where functions were pinned for this DAG.
            for function_name in dag.functions:
                for location in self.function_locations[function_name]:
                    pinned_locations.append((function_name, location))

        # For each location, we fire-and-forget an unpin message.
        for function_name, location in pinned_locations:
            ip, tid = location

            sckt = self.pusher_cache.get(get_unpin_address(ip, tid))
            sckt.send_string(function_name)

    def process_status(self, status):
        key = (status.ip, status.tid)
        logging.info('Received status update from executor %s:%d.' %
                     (key[0], int(key[1])))

        # This means that this node is currently departing, so we remove it
        # from all of our metadata tracking.
        if not status.running:
            if key in self.thread_statuses:
                for fname in self.thread_statuses[key].functions:
                    self.function_locations[fname].discard(key)

                del self.thread_statuses[key]

            self.unpinned_executors.discard(key)
            return

        if len(status.functions) == 0:
            self.unpinned_executors.add(key)

        # Remove all the old function locations, and all the new ones -- there
        # will probably be a large overlap, but this shouldn't be much
        # different than calculating two different set differences anyway.
        if key in self.thread_statuses and self.thread_statuses[key] != status:
            for function_name in self.thread_statuses[key].functions:
                if function_name in self.function_locations:
                    self.function_locations[function_name].discard(key)

        self.thread_statuses[key] = status
        for function_name in status.functions:
            if function_name not in self.function_locations:
                self.function_locations[function_name] = set()

            self.function_locations[function_name].add(key)

        # If the executor thread is overutilized, we add it to the backoff set
        # and ignore it for a period of time.
        if status.utilization > 0.70:
            self.backoff[key] = time.time()

    def update(self):
        # Periodically clean up the running counts map to drop any times older
        # than 5 seconds.
        for executor in self.running_counts:
            new_set = set()
            for ts in self.running_counts[executor]:
                if time.time() - ts < 5:
                    new_set.add(ts)

            self.running_counts[executor] = new_set

        # Clean up any backoff messages that were added more than 5 seconds ago
        # -- this should be enough to drain a queue.
        remove_set = set()
        for executor in self.backoff:
            if time.time() - self.backoff[executor] > 5:
                remove_set.add(executor)

        for executor in remove_set:
            del self.backoff[executor]

        executors = set(map(lambda status: status.ip,
                            self.thread_statuses.values()))

        # Update the sets of keys that are being cached at each IP address.
        self.key_locations.clear()
        for ip in executors:
            key = get_cache_ip_key(ip)

            # This is of type LWWPairLattice, which has a StringSet protobuf
            # packed into it; we want the keys in that StringSet protobuf.
            lattice = self.kvs_client.get(key)[key]
            if lattice is None:
                # We will only get None if this executor is still joining; if
                # so, we just ignore this for now and move on.
                continue

            st = StringSet()
            st.ParseFromString(lattice.reveal())

            for key in st.keys:
                if key not in self.key_locations:
                    self.key_locations[key] = []

                self.key_locations[key].append(ip)

    def update_function_locations(self, new_locations):
        for location in new_locations:
            function_name = location.name
            if function_name not in self.function_locations:
                self.function_locations[function_name] = set()

            key = (location.ip, location.tid)
            self.function_locations[function_name].add(key)
