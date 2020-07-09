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
from cloudburst.shared.proto.internal_pb2 import (
    CPU, GPU, # Cloudburst's executor types
    PinFunction
)
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

NUM_EXECUTOR_THREADS = 3


class DefaultCloudburstSchedulerPolicy(BaseCloudburstSchedulerPolicy):

    def __init__(self, pin_accept_socket, pusher_cache, kvs_client, ip,
                 policy, random_threshold=0.20, local=False):
        # This scheduler's IP address.
        self.ip = ip

        # The policy to use with the scheduler -- random, round-robin, or
        # locality.
        self.policy = policy

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
        self.unpinned_cpu_executors = set()

        # The subset of all executors that have access to GPUs and are
        # currently unallocated.
        # NOTE: We currently only support GPU executors # as a part of DAG
        # requests.
        self.unpinned_gpu_executors = set()

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

        # Indicates if we are running in local mode
        self.local = local


    def pick_executor(self, references, function_name=None, colocated=[],
                      schedule=None):
        # Construct a map which maps from IP addresses to the number of
        # relevant arguments they have cached. For the time begin, we will
        # just pick the machine that has the most number of keys cached.
        arg_map = {}

        if function_name:
            executors = set(self.function_locations[function_name])
        else:
            executors = set(self.unpinned_cpu_executors)

        # First priority is scheduling things on the same node if possible.
        # Otherwise, continue on with the regular policy.
        if len(colocated) > 0:
            candidate_nodes = set()
            for fn in colocated:
                if fn in schedule.locations:
                    ip = schedule.locations[fn].split(':')[0]
                    candidate_nodes.add(ip)

            for ip, tid in executors:
                if ip in candidate_nodes:
                    return ip, tid

        for executor in self.backoff:
            if len(executors) > 1:
                executors.discard(executor)

        # Shortcut policies -- if neither of these are activated, we go to the
        # default backoff and locality policy.
        if function_name:
            if self.policy == 'random':
                return random.choice(self.function_locations[function_name])
            if self.policy == 'round-robin':
                executor = self.function_locations[function_name].pop(0)
                self.function_locations[function_name].append(executor)
                return executor


        # Generate a list of all the keys in the system; if any of these nodes
        # have received many requests, we remove them from the executor set
        # with high probability.
        for key in self.running_counts:
           if (len(self.running_counts[key]) > 1000 and sys_random.random() >
                   self.random_threshold):
                if len(executors) > 1:
                    executors.discard(key)

        if len(executors) == 0:
            logging.error('No available executors.')
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
        if not self.local and not function_name:
            self.unpinned_cpu_executors.discard(max_ip)

        if not max_ip:
            logging.error('No available executors.')

        return max_ip

    def pin_function(self, dag_name, function_ref, colocated):
        # If there are no functions left to choose from, then we return None,
        # indicating that we ran out of resources to use.
        if function_ref.gpu and len(self.unpinned_gpu_executors) == 0:
            return False
        elif not function_ref.gpu and len(self.unpinned_cpu_executors) == 0:
            return False

        if dag_name not in self.pending_dags:
            self.pending_dags[dag_name] = []

        # Make a copy of the set of executors, so that we don't modify the
        # system's metadata.
        if function_ref.gpu:
            candidates = set(self.unpinned_gpu_executors)
        elif len(colocated) == 0:
            # If this is not a GPU function, just look at all of the unpinned
            # executors.
            candidates = set(self.unpinned_cpu_executors)
        else:
            candidates = set()

            already_pinned = set()
            for fn, thread in self.pending_dags[dag_name]:
                if fn in colocated:
                    already_pinned.add((fn, thread))
            candidate_nodes = set()

            if len(already_pinned) > 0:
                for fn, thread in already_pinned:
                    candidate_nodes.add(thread[0]) # The node's IP

                for node, tid in self.unpinned_cpu_executors:
                    if node in candidate_nodes:
                        candidates.add((node, tid))
            else:
                # If this is the first colocate to be pinned, try to assign to
                # an empty node.
                nodes = {}
                for node, tid in self.unpinned_cpu_executors:
                    if node not in nodes:
                        nodes[node] = 0
                    nodes[node] += 1

                for node in nodes:
                    if nodes[node] == NUM_EXECUTOR_THREADS:
                        for i in range(NUM_EXECUTOR_THREADS):
                            candidates.add((node, i))

        if len(candidates) == 0: # There no valid executors to colocate on.
            return self.pin_function(dag_name, function_ref, [])

        # Construct a PinFunction message to be sent to executors.
        pin_msg = PinFunction()
        pin_msg.name = function_ref.name
        pin_msg.batching = function_ref.batching
        pin_msg.response_address = self.ip

        serialized = pin_msg.SerializeToString()

        while True:
            # Pick a random executor from the set of candidates and attempt to
            # pin this function there.
            node, tid = sys_random.sample(candidates, 1)[0]

            for other_node, _ in self.pending_dags[dag_name]:
                if len(candidates) > 1 and node == other_node:
                    continue

            sckt = self.pusher_cache.get(get_pin_address(node, tid))
            sckt.send(serialized)

            response = GenericResponse()
            try:
                response.ParseFromString(self.pin_accept_socket.recv())
            except zmq.ZMQError:
                logging.error('Pin operation to %s:%d timed out. Retrying.' %
                              (node, tid))
                continue

            # Do not use this executor either way: If it rejected, it has
            # something else pinned, and if it accepted, it has pinned what we
            # just asked it to pin. In local mode, however we allow executors
            # to have multiple functions pinned.
            if not self.local:
                if function_ref.gpu:
                    self.unpinned_gpu_executors.discard((node, tid))
                    candidates.discard((node, tid))
                else:
                    self.unpinned_cpu_executors.discard((node, tid))
                    candidates.discard((node, tid))

            if response.success:
                # The pin operation succeeded, so we return the node and thread
                # ID to the caller.
                self.pending_dags[dag_name].append((function_ref.name, (node,
                                                                        tid)))
                return True
            else:
                # The pin operation was rejected, remove node and try again.
                logging.error('Node %s:%d rejected pin for %s. Retrying.'
                              % (node, tid, function_ref.name))

                continue

            if len(candidates) == 0 and len(colocated) > 0:
                # Try again without colocation.
                return self.pin_function(self, dag_name, function_ref, [])


    def commit_dag(self, dag_name):
        for function_name, location in self.pending_dags[dag_name]:
            if function_name not in self.function_locations:
                self.function_locations[function_name] = []

            self.function_locations[function_name].append(location)

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
            for function_ref in dag.functions:
                for location in self.function_locations[function_ref.name]:
                    pinned_locations.append((function_ref.name, location))

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
                    self.function_locations[fname].remove(key)

                del self.thread_statuses[key]

            if status.type == CPU:
                self.unpinned_cpu_executors.discard(key)
            else:
                self.unpinned_gpu_executors.discard(key)

            return

        if len(status.functions) == 0:
            if status.type == CPU:
                self.unpinned_cpu_executors.add(key)
            else:
                self.unpinned_gpu_executors.add(key)

        # Remove all the old function locations, and all the new ones -- there
        # will probably be a large overlap, but this shouldn't be much
        # different than calculating two different set differences anyway.
        if key in self.thread_statuses and self.thread_statuses[key] != status:
            for function_name in self.thread_statuses[key].functions:
                if function_name in self.function_locations:
                    self.function_locations[function_name].remove(key)

        self.thread_statuses[key] = status
        for function_name in status.functions:
            if function_name not in self.function_locations:
                self.function_locations[function_name] = list()

            if key not in self.function_locations[function_name]:
                self.function_locations[function_name].insert(0, key)

        # If the executor thread is overutilized, we add it to the backoff set
        # and ignore it for a period of time.
        if status.utilization > 0.70 and not self.local:
            not_lone_executor = []
            for function_name in status.functions:
                if len(self.function_locations[function_name]) > 1:
                    not_lone_executor.append(True)
                else:
                    not_lone_executor.append(False)

            if all(not_lone_executor):
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
                self.function_locations[function_name] = []

            key = (location.ip, location.tid)
            if key not in self.function_locations[function_name]:
                self.function_locations[function_name].append(key)
