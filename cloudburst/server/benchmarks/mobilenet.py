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

from anna.lattices import LWWPairLattice
import cloudpickle as cp
import numpy as np

from cloudburst.shared.reference import CloudburstReference


def run(cloudburst_client, num_requests, sckt):
    '''
    UPLOAD THE MODEL OBJECT
    '''
    model_key = 'mobilenet-model'
    label_key = 'mobilenet-label-map'

    with open('model/label_map.json', 'rb') as f:
        bts = f.read()
        lattice = LWWPairLattice(0, bts)
        cloudburst_client.kvs_client.put(label_key, lattice)

    with open('model/mobilenet_v2_1.4_224_frozen.pb', 'rb') as f:
        bts = f.read()
        lattice = LWWPairLattice(0, bts)
        cloudburst_client.kvs_client.put(model_key, lattice)

    ''' DEFINE AND REGISTER FUNCTIONS '''
    def preprocess(cloudburst, inp):
        from skimage import filters
        return filters.gaussian(inp).reshape(1, 224, 224, 3)

    class Mobilenet:
        def __init__(self, cloudburst, model_key, label_map_key):
            import tensorflow as tf
            import json

            tf.enable_eager_execution()

            self.model = cloudburst.get(model_key, deserialize=False)
            self.label_map = json.loads(cloudburst.get(label_map_key,
                                                    deserialize=False))

            self.gd = tf.GraphDef.FromString(self.model)
            self.inp, self.predictions = tf.import_graph_def(self.gd,
                                                             return_elements=['input:0', 'MobilenetV2/Predictions/Reshape_1:0'])

        def run(self, cloudburst, img):
            # load libs
            import tensorflow as tf
            from PIL import Image
            from io import BytesIO
            import base64
            import numpy as np
            import json

            tf.enable_eager_execution()

            # load image and model
            # img = np.array(Image.open(BytesIO(base64.b64decode(img))).resize((224, 224))).astype(np.float) / 128 - 1
            with tf.Session(graph=self.inp.graph):
              x = self.predictions.eval(feed_dict={self.inp: img})

            return x

    def average(cloudburst, inp):
        import numpy as np
        inp = [inp,]
        return np.mean(inp, axis=0)

    cloud_prep = cloudburst_client.register(preprocess, 'preprocess')
    cloud_mnet = cloudburst_client.register((Mobilenet, (model_key, label_key)), 'mnet')
    cloud_average = cloudburst_client.register(average, 'average')

    if cloud_prep and cloud_mnet and cloud_average:
        print('Successfully registered preprocess, mnet, and average '
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

    mnet_test = cloud_mnet(prep_test).get()
    if type(mnet_test) != np.ndarray:
        print('Unexpected result from mobilenet(arr): %s' %
              (str(mnet_test)))
        sys.exit(1)

    average_test = cloud_average(mnet_test).get()
    if type(average_test) != np.ndarray:
        print('Unexpected result from average(arr): %s' %
              (str(average_test)))
        sys.exit(1)

    print('Successfully tested functions!')

    ''' CREATE DAG '''

    dag_name = 'mnet'

    functions = ['preprocess', 'mnet', 'average']
    connections = [('preprocess', 'mnet'), ('mnet', 'average')]
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
