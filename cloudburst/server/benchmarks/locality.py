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
import sys
import time
import uuid

import cloudpickle as cp
import numpy as np

from cloudburst.server.benchmarks import utils
from cloudburst.shared.proto.cloudburst_pb2 import CloudburstError, DAG_ALREADY_EXISTS
from cloudburst.shared.reference import CloudburstReference

sys_random = random.SystemRandom()
OSIZE = 1000000


def run(cloudburst_client, num_requests, create, sckt):
    dag_name = 'locality'
    kvs_key = 'LOCALITY_OIDS'

    if create:
        ''' DEFINE AND REGISTER FUNCTIONS '''
        def dot(cloudburst, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10):
            import numpy as np
            s1 = np.add(v1, v2)
            s2 = np.add(v3, v4)
            s3 = np.add(v5, v6)
            s4 = np.add(v7, v8)
            s5 = np.add(v9, v10)

            s1 = np.add(s1, s2)
            s2 = np.add(s3, s4)

            s1 = np.add(s1, s2)
            s1 = np.add(s1, s5)

            return np.average(s1)

        cloud_dot = cloudburst_client.register(dot, 'dot')

        if cloud_dot:
            logging.info('Successfully registered the dot function.')
        else:
            sys.exit(1)

        ''' TEST REGISTERED FUNCTIONS '''
        refs = ()
        for _ in range(10):
            inp = np.zeros(OSIZE)
            k = str(uuid.uuid4())
            cloudburst_client.put_object(k, inp)

            refs += (CloudburstReference(k, True),)

        dot_test = cloud_dot(*refs).get()
        if dot_test != 0.0:
            print('Unexpected result from dot(v1, v2): %s' % (str(dot_test)))
            sys.exit(1)

        logging.info('Successfully tested function!')

        ''' CREATE DAG '''
        functions = ['dot']
        connections = []
        success, error = cloudburst_client.register_dag(dag_name, functions,
                                                     connections)
        if not success and error != DAG_ALREADY_EXISTS:
            print('Failed to register DAG: %s' % (CloudburstError.Name(error)))
            sys.exit(1)

        # for the hot version
        oid = str(uuid.uuid4())
        arr = np.random.randn(OSIZE)
        cloudburst_client.put_object(oid, arr)
        cloudburst_client.put_object(kvs_key, [oid])

        return [], [], [], 0

    else:
        ''' RUN DAG '''

        # num_data_objects = num_requests * 10 # for the cold version
        # oids = []
        # for i in range(num_data_objects):
        #     if i % 100 == 0:
        #         logging.info('On object %d.' % (i))

        #     array = np.random.rand(OSIZE)
        #     oid = str(uuid.uuid4())
        #     cloudburst_client.put_object(oid, array)
        #     oids.append(oid)

        # logging.info('Finished creating data!')

        # for the hot version
        oids = cloudburst_client.get_object(kvs_key)

        total_time = []
        scheduler_time = []
        kvs_time = []

        retries = 0

        log_start = time.time()

        log_epoch = 0
        epoch_total = []

        for i in range(num_requests):
            refs = []
            # for ref in oids[(i * 10):(i * 10) + 10]: # for the cold version
            #     refs.append(CloudburstReference(ref, True))
            for _ in range(10):  # for the hot version
                refs.append(CloudburstReference(oids[0], True))

            start = time.time()
            arg_map = {'dot': refs}

            cloudburst_client.call_dag(dag_name, arg_map, True)
            end = time.time()

            epoch_total.append(end - start)
            total_time.append(end - start)

            log_end = time.time()
            if (log_end - log_start) > 10:
                if sckt:
                    sckt.send(cp.dumps(epoch_total))
                utils.print_latency_stats(epoch_total, 'EPOCH %d E2E' %
                                          (log_epoch), True)

                epoch_total.clear()
                log_epoch += 1
                log_start = time.time()

        return total_time, scheduler_time, kvs_time, retries
