import sys
import time
import uuid

import cloudpickle as cp
import numpy as np

from cloudburst.shared.reference import CloudburstReference

def run(cloudburst_client, num_requests, sckt):
    ''' DEFINE AND REGISTER FUNCTIONS '''
    def preprocess(cloudburst):
        x = 1

    def mat_square1(cloudburst, mat):
        import numpy as np
        return np.matmul(mat, mat)

    def mat_square2(cloudburst, mat):
        import numpy as np
        return np.matmul(mat, mat)

    def mat_square3(cloudburst, mat):
        import numpy as np
        return np.matmul(mat, mat)

    def mat_square4(cloudburst, mat):
        import numpy as np
        return np.matmul(mat, mat)

    def mat_square5(cloudburst, mat):
        import numpy as np
        return np.matmul(mat, mat)

    def mat_square6(cloudburst, mat):
        import numpy as np
        return np.matmul(mat, mat)

    def average(cloudburst, inp1, inp2, inp3, inp4, inp5, inp6):
        import numpy as np
        inp = [inp1, inp2, inp3, inp4, inp5, inp6]
        return np.mean(inp, axis=0)

    cloud_prep = cloudburst_client.register(preprocess, 'preprocess')
    cloud_mat_sq1 = cloudburst_client.register(mat_square1, 'mat_square1')
    cloud_mat_sq2 = cloudburst_client.register(mat_square2, 'mat_square2')
    cloud_mat_sq3 = cloudburst_client.register(mat_square3, 'mat_square3')
    cloud_mat_sq4 = cloudburst_client.register(mat_square4, 'mat_square4')
    cloud_mat_sq5 = cloudburst_client.register(mat_square5, 'mat_square5')
    cloud_mat_sq6 = cloudburst_client.register(mat_square6, 'mat_square6')
    cloud_avg = cloudburst_client.register(average, 'average')

    if cloud_prep and cloud_mat_sq1 and cloud_mat_sq2 and cloud_mat_sq3 \
            and cloud_mat_sq4 and cloud_mat_sq5 and cloud_mat_sq6 and cloud_avg:
        print('Successfully registered preprocess, mat_square, and average functions.')
    else:
        sys.exit(1)

    ''' CREATE DAG '''
    dag_name = 'matrix_mult'

    functions = ['preprocess', 'mat_square1', 'mat_square2', 'mat_square3', 'mat_square4',
                 'mat_square5', 'mat_square6', 'average']
    connections = [('preprocess', 'mat_square1'), ('preprocess', 'mat_square2'),
                   ('preprocess', 'mat_square3'), ('preprocess', 'mat_square4'),
                   ('preprocess', 'mat_square5'), ('preprocess', 'mat_square6'),
                   ('mat_square1', 'average'), ('mat_square2', 'average'),
                   ('mat_square3', 'average'), ('mat_square4', 'average'),
                   ('mat_square5', 'average'), ('mat_square6', 'average')]
    success, error = cloudburst_client.register_dag(dag_name, functions,
                                                    connections)

    if not success:
        print('Failed to register DAG: %s' % (str(error)))
        sys.exit(1)

    ''' RUN DAG '''
    total_time = []
    scheduler_time = []
    kvs_time = []

    # Create all the input data
    oids = {}
    for i in range(num_requests):
        oids[i] = {}
        # Generate large matrices
        for j in range(1, 4):
            arr = np.random.randn(1000, 1000)
            oid = str(uuid.uuid4())
            oids[i][j] = oid
            cloudburst_client.put_object(oid, arr)
        # Generate small matrices
        for j in range(4, 7):
            arr = np.random.randn(100, 100)
            oid = str(uuid.uuid4())
            oids[i][j] = oid
            cloudburst_client.put_object(oid, arr)

    for i in range(num_requests):
        oid = oids[i]

        arg_map = {'mat_square1': [CloudburstReference(oid[1], True)],
                   'mat_square2': [CloudburstReference(oid[2], True)],
                   'mat_square3': [CloudburstReference(oid[3], True)],
                   'mat_square4': [CloudburstReference(oid[4], True)],
                   'mat_square5': [CloudburstReference(oid[5], True)],
                   'mat_square6': [CloudburstReference(oid[6], True)]}

        start = time.time()
        rid = cloudburst_client.call_dag(dag_name, arg_map, True)
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

    return total_time, scheduler_time, kvs_time, 0
