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


import sys
import time

import cloudpickle as cp

from cloudburst.shared.proto.cloudburst_pb2 import CloudburstError, DAG_ALREADY_EXISTS


def run(cloudburst_client, num_requests, sckt):
    ''' DEFINE AND REGISTER FUNCTIONS '''
    def incr(cloudburst, x):
        return x + 1

    def square(cloudburst, x):
        return x * x

    cloud_incr = cloudburst_client.register(incr, 'incr')
    cloud_square = cloudburst_client.register(square, 'square')

    if cloud_incr and cloud_square:
        print('Successfully registered incr and square functions.')
    else:
        sys.exit(1)

    ''' TEST REGISTERED FUNCTIONS '''
    incr_test = cloud_incr(2).get()
    if incr_test != 3:
        print('Unexpected result from incr(2): %s' % (str(incr_test)))
        sys.exit(1)

    square_test = cloud_square(2).get()
    if square_test != 4:
        print('Unexpected result from square(2): %s' % (str(square_test)))
        sys.exit(1)

    print('Successfully tested functions!')

    ''' CREATE DAG '''
    dag_name = 'composition'

    functions = ['incr', 'square']
    connections = [('incr', 'square')]
    success, error = cloudburst_client.register_dag(dag_name, functions,
                                                 connections)

    if not success and error != DAG_ALREADY_EXISTS:
        print('Failed to register DAG: %s' % (CloudburstError.Name(error)))
        sys.exit(1)

    ''' RUN DAG '''
    arg_map = {'incr': [1]}

    total_time = []
    scheduler_time = []
    kvs_time = []

    retries = 0

    for _ in range(num_requests):
        start = time.time()
        rid = cloudburst_client.call_dag(dag_name, arg_map)
        end = time.time()

        stime = end - start

        start = time.time()
        rid.get()
        end = time.time()

        ktime = end - start

        total_time += [stime + ktime]
        scheduler_time += [stime]
        kvs_time += [ktime]

    if sckt:
        sckt.send(cp.dumps(total_time))
    return total_time, scheduler_time, kvs_time, retries
