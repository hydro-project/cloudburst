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
import time

from anna.lattices import (
    Lattice,
    MapLattice,
    MultiKeyCausalLattice,
    SetLattice,
    SingleKeyCausalLattice,
    VectorClock
)

from cloudburst.server.executor import utils
import cloudburst.server.utils as sutils
from cloudburst.shared.proto.cloudburst_pb2 import (
    Continuation,
    DagTrigger,
    FunctionCall,
    NORMAL, MULTI,  # Cloudburst's consistency modes,
    EXECUTION_ERROR, FUNC_NOT_FOUND,  # Cloudburst's error types
    MULTIEXEC # Cloudburst's execution types
)
from cloudburst.shared.reference import CloudburstReference
from cloudburst.shared.serializer import Serializer

serializer = Serializer()


def exec_function(exec_socket, kvs, user_library, cache, function_cache):
    call = FunctionCall()
    call.ParseFromString(exec_socket.recv())

    fargs = [serializer.load(arg) for arg in call.arguments.values]

    if call.name in function_cache:
        f = function_cache[call.name]
    else:
        f = utils.retrieve_function(call.name, kvs, user_library, call.consistency)

    if not f:
        logging.info('Function %s not found! Returning an error.' %
                     (call.name))
        sutils.error.error = FUNC_NOT_FOUND
        result = ('ERROR', sutils.error.SerializeToString())
    else:
        function_cache[call.name] = f
        try:
            if call.consistency == NORMAL:
                result = _exec_func_normal(kvs, f, fargs, user_library, cache)
                logging.info('Finished executing %s: %s!' % (call.name,
                                                             str(result)))
            else:
                dependencies = {}
                result = _exec_func_causal(kvs, f, fargs, user_library,
                                           dependencies=dependencies)
        except Exception as e:
            logging.exception('Unexpected error %s while executing function.' %
                              (str(e)))
            sutils.error.error = EXECUTION_ERROR
            result = ('ERROR: ' + str(e), sutils.error.SerializeToString())

    if call.consistency == NORMAL:
        result = serializer.dump_lattice(result)
        succeed = kvs.put(call.response_key, result)
    else:
        result = serializer.dump_lattice(result, MultiKeyCausalLattice,
                                         causal_dependencies=dependencies)
        succeed = kvs.causal_put(call.response_key, result)

    if not succeed:
        logging.info(f'Unsuccessful attempt to put key {call.response_key} '
                     + 'into the KVS.')


def _exec_func_normal(kvs, func, args, user_lib, cache):
    # NOTE: We may not want to keep this permanently but need it for
    # continuations if the upstream function returns multiple things.
    processed = tuple()
    for arg in args:
        if type(arg) == tuple:
            processed += arg
        else:
            processed += (arg,)
    args = processed

    refs = list(filter(lambda a: isinstance(a, CloudburstReference), args))

    if refs:
        refs = _resolve_ref_normal(refs, kvs, cache)

    return _run_function(func, refs, args, user_lib)


def _exec_func_causal(kvs, func, args, user_lib, schedule=None,
                      key_version_locations={}, dependencies={}):
    refs = list(filter(lambda a: isinstance(a, CloudburstReference), args))

    if refs:
        refs = _resolve_ref_causal(refs, kvs, schedule, key_version_locations,
                                   dependencies)

    return _run_function(func, refs, args, user_lib)


def _run_function(func, refs, args, user_lib):
    # Set the first argument to the user library.
    func_args = (user_lib,)

    # If any of the arguments are references, we insert the resolved reference
    # instead of the raw value.
    for arg in args:
        if isinstance(arg, CloudburstReference):
            func_args += (refs[arg.key],)
        else:
            func_args += (arg,)

    return func(*func_args)


def _resolve_ref_normal(refs, kvs, cache):
    deserialize_map = {}
    kv_pairs = {}
    keys = set()

    for ref in refs:
        deserialize_map[ref.key] = ref.deserialize
        if ref.key in cache:
            kv_pairs[ref.key] = cache[ref.key]
        else:
            keys.add(ref.key)

    keys = list(keys)

    if len(keys) != 0:
        returned_kv_pairs = kvs.get(keys)

        # When chaining function executions, we must wait, so we check to see
        # if certain values have not been resolved yet.
        while None in returned_kv_pairs.values():
            returned_kv_pairs = kvs.get(keys)

        for key in keys:
            # Because references might be repeated, we check to make sure that
            # we haven't already deserialized this ref.
            if deserialize_map[key] and isinstance(returned_kv_pairs[key],
                                                   Lattice):
                kv_pairs[key] = serializer.load_lattice(returned_kv_pairs[key])
            else:
                kv_pairs[key] = returned_kv_pairs[key].reveal()

            # Cache the deserialized payload for future use
            cache[key] = kv_pairs[key]

    return kv_pairs


def _resolve_ref_causal(refs, kvs, schedule, key_version_locations,
                        dependencies):
    if schedule:
        future_read_set = _compute_children_read_set(schedule)
        client_id = schedule.client_id
        consistency = schedule.consistency
    else:
        future_read_set = set()
        client_id = 0
        consistency = MULTI

    keys = [ref.key for ref in refs]
    (address, versions), kv_pairs = kvs.causal_get(keys, future_read_set,
                                                   key_version_locations,
                                                   consistency, client_id)

    while None in kv_pairs.values():
        (address, versions), kv_pairs = kvs.causal_get(keys, future_read_set,
                                                       key_version_locations,
                                                       consistency, client_id)
    if address is not None:
        if address not in key_version_locations:
            key_version_locations[address] = versions
        else:
            key_version_locations[address].extend(versions)

    for key in kv_pairs:
        if key in dependencies:
            dependencies[key].merge(kv_pairs[key].vector_clock)
        else:
            dependencies[key] = kv_pairs[key].vector_clock

    for ref in refs:
        key = ref.key
        if ref.deserialize:
            # In causal mode, you can only use these two lattice types.
            if (isinstance(kv_pairs[key], SingleKeyCausalLattice) or
                    isinstance(kv_pairs[key], MultiKeyCausalLattice)):
                # If there are multiple values, we choose the first one listed
                # at random.
                kv_pairs[key] = serializer.load_lattice(kv_pairs[key])[0]
            else:
                raise ValueError(('Invalid lattice type %s encountered when' +
                                 ' executing in causal mode.') %
                                 str(type(kv_pairs[key])))
        else:
            kv_pairs[key] = kv_pairs[key].reveal()

    return kv_pairs


def exec_dag_function(pusher_cache, kvs, triggers, function, schedule,
                      user_library, dag_runtimes, cache, schedulers):
    if schedule.consistency == NORMAL:
        finished, success = _exec_dag_function_normal(pusher_cache, kvs,
                                                      triggers, function,
                                                      schedule, user_library,
                                                      cache, schedulers)
    else:
        finished, success = _exec_dag_function_causal(pusher_cache, kvs,
                                                      triggers, function,
                                                      schedule, user_library)

    # If finished is true, that means that this executor finished the DAG
    # request. We will report the end-to-end latency for this DAG if so.
    if finished:
        dname = schedule.dag.name
        if dname not in dag_runtimes:
            dag_runtimes[dname] = []

        runtime = time.time() - schedule.start_time
        dag_runtimes[schedule.dag.name].append(runtime)

    return success


def _construct_trigger(sid, fname, result):
    trigger = DagTrigger()
    trigger.id = sid
    trigger.source = fname

    if type(result) != tuple:
        result = (result,)

    trigger.arguments.values.extend(list(
        map(lambda v: serializer.dump(v, None, False), result)))
    return trigger


def _exec_dag_function_normal(pusher_cache, kvs, triggers, function, schedule,
                              user_lib, cache, schedulers):
    fname = schedule.target_function
    fargs = list(schedule.arguments[fname].values)

    for trigger in triggers:
        fargs += list(trigger.arguments.values)

    fargs = [serializer.load(arg) for arg in fargs]
    result = _exec_func_normal(kvs, function, fargs, user_lib, cache)

    this_ref = None
    for ref in schedule.dag.functions:
        if ref.name == fname:
            this_ref = ref # There must be a match.

    success = True
    if this_ref.type == MULTIEXEC:
        if serializer.dump(result) in this_ref.invalid_results:
            return False, False

    is_sink = True
    new_trigger = _construct_trigger(schedule.id, fname, result)
    for conn in schedule.dag.connections:
        if conn.source == fname:
            is_sink = False
            new_trigger.target_function = conn.sink

            dest_ip = schedule.locations[conn.sink]
            sckt = pusher_cache.get(sutils.get_dag_trigger_address(dest_ip))
            sckt.send(new_trigger.SerializeToString())

    if is_sink:
        if schedule.continuation.name:
            cont = schedule.continuation
            cont.id = schedule.id
            cont.result = serializer.dump(result)

            logging.info('Sending continuation to scheduler for DAG %s.' %
                         (schedule.id))
            sckt = pusher_cache.get(utils.get_continuation_address(schedulers))
            sckt.send(cont.SerializeToString())
        elif schedule.response_address:
            sckt = pusher_cache.get(schedule.response_address)
            logging.info('DAG %s (ID %s) result returned to requester.' %
                         (schedule.dag.name, trigger.id))
            sckt.send(serializer.dump(result))

        else:
            lattice = serializer.dump_lattice(result)
            output_key = schedule.output_key if schedule.output_key \
                else schedule.id
            logging.info('DAG %s (ID %s) result in KVS at %s.' %
                         (schedule.dag.name, trigger.id, output_key))
            kvs.put(output_key, lattice)

    return is_sink, success


def _exec_dag_function_causal(pusher_cache, kvs, triggers, function, schedule,
                              user_lib):
    fname = schedule.target_function
    fargs = list(schedule.arguments[fname].values)

    key_version_locations = {}
    dependencies = {}

    for trigger in triggers:
        fargs += list(trigger.arguments.values)

        # Combine the locations of upstream cached key versions from all
        # triggers.
        for addr in trigger.version_locations:
            if addr in key_version_locations:
                key_version_locations[addr].extend(
                    trigger.version_locations[addr].key_versions)
            else:
                key_version_locations[addr] = list(
                    trigger.version_locations[addr])

        # Combine the dependency sets from all triggers.
        for dependency in trigger.dependencies:
            vc = VectorClock(dict(dependency.vector_clock), True)
            key = dependency.key

            if key in dependencies:
                dependencies[key].merge(vc)
            else:
                dependencies[key] = vc

    fargs = [serializer.load(arg) for arg in fargs]

    result = _exec_func_causal(kvs, function, fargs, user_lib, schedule,
                               key_version_locations, dependencies)

    this_ref = None
    for ref in schedule.dag.functions:
        if ref.name == fname:
            this_ref = ref # There must be a match.

    success = True
    if this_ref.type == MULTIEXEC:
        if serializer.dump(result) in this_ref.invalid_results:
            return False, False

    # Create a new trigger with the schedule ID and results of this execution.
    new_trigger = _construct_trigger(schedule.id, fname, result)

    # Serialize the key version location information into this new trigger.
    for addr in key_version_locations:
        new_trigger.version_locations[addr].keys.extend(
            key_version_locations[addr])

    # Serialize the set of dependency versions for causal metadata.
    for key in dependencies:
        dep = new_trigger.dependencies.add()
        dep.key = key
        dependencies[key].serialize(dep.vector_clock)

    is_sink = True
    for conn in schedule.dag.connections:
        if conn.source == fname:
            is_sink = False
            new_trigger.target_function = conn.sink

            dest_ip = schedule.locations[conn.sink]
            sckt = pusher_cache.get(sutils.get_dag_trigger_address(dest_ip))
            sckt.send(new_trigger.SerializeToString())

    if is_sink:
        logging.info('DAG %s (ID %s) completed in causal mode; result at %s.' %
                     (schedule.dag.name, schedule.id, schedule.output_key))

        vector_clock = {}
        okey = schedule.output_key
        if okey in dependencies:
            prev_count = 0
            if schedule.client_id in dependencies[okey]:
                prev_count = dependencies[okey][schedule.client_id]

            dependencies[okey].update(schedule.client_id, prev_count + 1)
            dependencies[okey].serialize(vector_clock)
            del dependencies[okey]
        else:
            vector_clock = {schedule.client_id: 1}

        # Serialize result into a MultiKeyCausalLattice.
        vector_clock = VectorClock(vector_clock, True)
        result = serializer.dump(result)
        dependencies = MapLattice(dependencies)
        lattice = MultiKeyCausalLattice(vector_clock, dependencies,
                                        SetLattice({result}))

        succeed = kvs.causal_put(schedule.output_key,
                                 lattice, schedule.client_id)
        while not succeed:
            succeed = kvs.causal_put(schedule.output_key,
                                     lattice, schedule.client_id)

        # Issues requests to all upstream caches for this particular request
        # and asks them to garbage collect pinned versions stored for the
        # context of this request.
        for cache_addr in key_version_locations:
            gc_address = utils.get_cache_gc_address(cache_addr)
            sckt = pusher_cache.get(gc_address)
            sckt.send_string(schedule.client_id)

    return is_sink, success


def _compute_children_read_set(schedule):
    future_read_set = set()
    fname = schedule.target_function
    children = set()
    delta = {fname}

    while len(delta) > 0:
        new_delta = set()
        for conn in schedule.dag.connections:
            if conn.source in delta:
                children.add(conn.sink)
                new_delta.add(conn.sink)
        delta = new_delta

    for child in children:
        refs = list(filter(lambda arg: type(arg) == CloudburstReference,
                           [serializer.load(arg) for arg in
                            schedule.arguments[child].values]))
        for ref in refs:
            future_read_set.add(ref.key)

    return future_read_set
