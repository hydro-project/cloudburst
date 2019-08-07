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

import uuid
import time

from droplet.server.scheduler import utils
import droplet.server.utils as sutils
from droplet.shared.proto.droplet_pb2 import (
    DagSchedule,
    DagTrigger,
    FunctionCall,
    GenericResponse,
    NO_RESOURCES  # Droplet's error types
)
from droplet.shared.reference import DropletReference
from droplet.shared.serializer import Serializer

serializer = Serializer()


def call_function(func_call_socket, pusher_cache, policy):
    # Parse the received protobuf for this function call.
    call = FunctionCall()
    call.ParseFromString(func_call_socket.recv())

    # If there is no response key set for this request, we generate a random
    # UUID.
    if not call.response_key:
        call.response_key = str(uuid.uuid4())

    # Filter the arguments for DropletReferences, and use the policy engine to
    # pick a node for this request.
    refs = list(filter(lambda arg: type(arg) == DropletReference,
                       map(lambda arg: serializer.load(arg),
                           call.arguments.values)))
    result = policy.pick_executor(refs)

    response = GenericResponse()
    if result is None:
        response.success = False
        response.error = NO_RESOURCES
        func_call_socket.send(response.SerializeToString())
        return

    # Forward the request on to the chosen executor node.
    ip, tid = result
    sckt = pusher_cache.get(utils.get_exec_address(ip, tid))
    sckt.send(call.SerializeToString())

    # Send a success response to the user with the response key.
    response.success = True
    response.response_id = call.response_key
    func_call_socket.send(response.SerializeToString())


def call_dag(call, pusher_cache, dags, policy):
    dag, sources = dags[call.name]

    schedule = DagSchedule()
    schedule.id = str(uuid.uuid4())
    schedule.dag.CopyFrom(dag)
    schedule.start_time = time.time()
    schedule.consistency = call.consistency

    if call.response_address:
        schedule.response_address = call.response_address

    if call.output_key:
        schedule.output_key = call.output_key

    if call.client_id:
        schedule.client_id = call.client_id

    for fname in dag.functions:
        args = call.function_args[fname].values

        refs = list(filter(lambda arg: type(arg) == DropletReference,
                           map(lambda arg: serializer.load(arg), args)))
        ip, tid = policy.pick_executor(refs, fname)
        schedule.locations[fname] = ip + ':' + str(tid)

        # copy over arguments into the dag schedule
        arg_list = schedule.arguments[fname]
        arg_list.values.extend(args)

    for fname in dag.functions:
        loc = schedule.locations[fname].split(':')
        ip = utils.get_queue_address(loc[0], loc[1])
        schedule.target_function = fname

        triggers = sutils.get_dag_predecessors(dag, fname)
        if len(triggers) == 0:
            triggers.append('BEGIN')

        schedule.ClearField('triggers')
        schedule.triggers.extend(triggers)

        sckt = pusher_cache.get(ip)
        sckt.send(schedule.SerializeToString())

    for source in sources:
        trigger = DagTrigger()
        trigger.id = schedule.id
        trigger.source = 'BEGIN'
        trigger.target_function = source

        ip = sutils.get_dag_trigger_address(schedule.locations[source])
        sckt = pusher_cache.get(ip)
        sckt.send(trigger.SerializeToString())

    if schedule.output_key:
        return schedule.output_key
    else:
        return schedule.id
