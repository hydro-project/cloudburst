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

from anna.lattices import (
    LWWPairLattice,
    SetLattice,
    SingleKeyCausalLattice
)

from cloudburst.shared.proto.cloudburst_pb2 import (
    Dag,
    Function,
    NORMAL,  # Cloudburst's consistency modes
    DAG_ALREADY_EXISTS, NO_RESOURCES, NO_SUCH_DAG  # Cloudburst's error modes
)
import cloudburst.server.utils as sutils
from cloudburst.server.scheduler import utils

sys_random = random.SystemRandom()


def create_function(func_create_socket, kvs, consistency=NORMAL):
    func = Function()
    func.ParseFromString(func_create_socket.recv())

    name = sutils.get_func_kvs_name(func.name)
    logging.info('Creating function %s.' % (name))

    if consistency == NORMAL:
        body = LWWPairLattice(sutils.generate_timestamp(0), func.body)
        res = kvs.put(name, body)
    else:
        skcl = SingleKeyCausalLattice(sutils.DEFAULT_VC,
                                      SetLattice({func.body}))
        kvs.put(name, skcl)

    funcs = utils.get_func_list(kvs, '', fullname=True)
    funcs.append(name)
    utils.put_func_list(kvs, funcs)

    func_create_socket.send(sutils.ok_resp)


def create_dag(dag_create_socket, pusher_cache, kvs, dags, policy,
               call_frequency, num_replicas=1):
    serialized = dag_create_socket.recv()

    dag = Dag()
    dag.ParseFromString(serialized)

    # We do not allow duplicate DAGs, so we return an error to the user if we
    # already know about this DAG.
    if dag.name in dags:
        sutils.error.error = DAG_ALREADY_EXISTS
        dag_create_socket.send(sutils.error.SerializeToString())
        return

    logging.info('Creating DAG %s.' % (dag.name))

    # We persist the DAG in the KVS, so other schedulers can read the DAG when
    # they hear about it.
    payload = LWWPairLattice(sutils.generate_timestamp(0), serialized)
    kvs.put(dag.name, payload)

    for fref in dag.functions:
        for _ in range(num_replicas):
            colocated = []

            if fref.name in dag.colocated:
                colocated = list(dag.colocated)

            success = policy.pin_function(dag.name, fref, colocated)

            # The policy engine will only return False if it ran out of
            # resources on which to attempt to pin this function.
            if not success:
                logging.info(f'Creating DAG {dag.name} failed due to ' +
                             'insufficient resources.')
                sutils.error.error = NO_RESOURCES
                dag_create_socket.send(sutils.error.SerializeToString())

                # Unpin any previously pinned functions because the operation
                # failed.
                policy.discard_dag(dag, True)
                return

    # Only create this metadata after all functions have been successfully
    # created.
    for fref in dag.functions:
        if fref.name not in call_frequency:
            call_frequency[fref.name] = 0

    policy.commit_dag(dag.name)
    dags[dag.name] = (dag, utils.find_dag_source(dag))
    dag_create_socket.send(sutils.ok_resp)


def delete_dag(dag_delete_socket, dags, policy, call_frequency):
    dag_name = dag_delete_socket.recv_string()

    # If we do not know about this DAG, then we cannot delete it, so we return
    # an error.
    if dag_name not in dags:
        sutils.error.error = NO_SUCH_DAG
        dag_delete_socket.send(sutils.error.SerializeToString())
        return

    # Tell the policy engine to discard metadata about this DAG.
    dag = dags[dag_name][0]
    policy.discard_dag(dag)

    # Remove all performance metadata for this DAG and send a success response
    # to the user.
    for fref in dag.functions:
        del call_frequency[fref.name]

    del dags[dag_name]
    dag_delete_socket.send(sutils.ok_resp)
    logging.info('DAG %s deleted.' % (dag_name))
