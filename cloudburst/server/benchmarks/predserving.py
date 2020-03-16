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
import uuid

import cloudpickle as cp
import numpy as np

from cloudburst.shared.reference import CloudburstReference


def run(cloudburst_client, num_requests, sckt):
    ''' DEFINE AND REGISTER FUNCTIONS '''
    def preprocess(cloudburst, inp):
        from skimage import filters
        return filters.gaussian(inp).reshape(1, 3, 224, 224)

    def sqnet(cloudburst, inp):
        import torch
        import torchvision

        model = torchvision.models.squeezenet1_1()
        return model(torch.tensor(inp.astype(np.float32))).detach().numpy()

    def average(cloudburst, inp1, inp2, inp3):
        import numpy as np
        inp = [inp1, inp2, inp3]
        return np.mean(inp, axis=0)

    cloud_prep = cloudburst_client.register(preprocess, 'preprocess')
    cloud_sqnet1 = cloudburst_client.register(sqnet, 'sqnet1')
    cloud_sqnet2 = cloudburst_client.register(sqnet, 'sqnet2')
    cloud_sqnet3 = cloudburst_client.register(sqnet, 'sqnet3')
    cloud_average = cloudburst_client.register(average, 'average')

    if cloud_prep and cloud_sqnet1 and cloud_sqnet2 and cloud_sqnet3 and \
            cloud_average:
        print('Successfully registered preprocess, sqnet, and average '
              + 'functions.')
    else:
        sys.exit(1)

    ''' TEST REGISTERED FUNCTIONS '''
    arr = np.random.randn(1, 224, 224, 3)
    prep_test = cloud_prep(arr).get()
    if type(prep_test) != np.ndarray:
        print('Unexpected result from preprocess(arr): %s' %
              (str(prep_test)))
        sys.exit(1)

    sqnet_test1 = cloud_sqnet1(prep_test).get()
    if type(sqnet_test1) != np.ndarray:
        print('Unexpected result from squeezenet1(arr): %s' %
              (str(sqnet_test1)))
        sys.exit(1)

    sqnet_test2 = cloud_sqnet2(prep_test).get()
    if type(sqnet_test2) != np.ndarray:
        print('Unexpected result from squeezenet2(arr): %s' %
              (str(sqnet_test2)))
        sys.exit(1)

    sqnet_test3 = cloud_sqnet3(prep_test).get()
    if type(sqnet_test3) != np.ndarray:
        print('Unexpected result from squeezenet3(arr): %s' %
              (str(sqnet_test3)))
        sys.exit(1)

    average_test = cloud_average(sqnet_test1, sqnet_test2, sqnet_test3).get()
    if type(average_test) != np.ndarray:
        print('Unexpected result from squeezenet(arr): %s' %
              (str(average_test)))
        sys.exit(1)

    print('Successfully tested functions!')

    ''' CREATE DAG '''

    dag_name = 'pred_serving'

    functions = ['preprocess', 'sqnet1', 'sqnet2', 'sqnet3', 'average']
    connections = [('preprocess', 'sqnet1'), ('preprocess', 'sqnet2'),
                   ('preprocess', 'sqnet3'), ('sqnet1', 'average'),
                   ('sqnet2', 'average'), ('sqnet3', 'average')]
    success, error = cloudburst_client.register_dag(dag_name, functions,
                                                 connections)

    if not success:
        print('Failed to register DAG: %s' % (str(error)))
        sys.exit(1)

    ''' RUN DAG '''
    total_time = []

    # Create all the input data
    oids = []
    for _ in range(num_requests):
        arr = np.random.randn(1, 224, 224, 3)
        oid = str(uuid.uuid4())
        oids.append(oid)

        cloudburst_client.put_object(oid, arr)

    for i in range(num_requests):
        oid = oids[i]

        arg_map = {'preprocess': [CloudburstReference(oid, True)]}

        start = time.time()
        cloudburst_client.call_dag(dag_name, arg_map, True)
        end = time.time()

        total_time += [end - start]

    if sckt:
        sckt.send(cp.dumps(total_time))

    return total_time, [], [], 0
