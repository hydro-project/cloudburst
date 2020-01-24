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

import numpy as np


def run(cloudburst_client, num_requests, sckt):
    ''' DEFINE AND REGISTER FUNCTIONS '''
    def avg(cloudburst, uid, eid, num_execs, val):
        import random
        import time

        from anna.lattices import LWWPairLattice
        import cloudpickle as cp
        import numpy as np

        myid = cloudburst.getid()
        key = '%s:%d' % (uid, eid)
        cloudburst.put(key, LWWPairLattice(0, cp.dumps(myid)))

        procs = set()
        keyset = []

        for i in range(num_execs):
            if i == eid:
                continue

            key = '%s:%d' % (uid, i)
            keyset.append(key)

        locs = cloudburst.get(keyset)
        while None in locs.values():
            locs = cloudburst.get(keyset)

        for key in locs:
            procs.add(cp.loads(locs[key].reveal()))

        curr_val = val
        curr_weight = 1
        curr_avg = None

        val_msgs = [curr_val]
        weight_msgs = [curr_weight]

        rounds = 0
        NUM_ROUNDS = 5
        while rounds < NUM_ROUNDS:
            curr_val = np.sum(val_msgs)
            curr_weight = np.sum(weight_msgs)

            dst = random.sample(procs, 1)[0]
            cloudburst.send(dst, cp.dumps((curr_val * .5, curr_weight * .5)))

            val_msgs.clear()
            weight_msgs.clear()

            val_msgs.append(curr_val * .5)
            weight_msgs.append(curr_weight * .5)

            start = time.time()
            while time.time() - start < .1:
                msgs = cloudburst.recv()
                for msg in msgs:
                    msg = cp.loads(msg[1])
                    val_msgs.append(msg[0])
                    weight_msgs.append(msg[1])

            new_avg = curr_val / curr_weight

            curr_avg = new_avg
            rounds += 1

        return curr_avg

    cloud_avg = cloudburst_client.register(avg, 'avg')

    if cloud_avg:
        print('Successfully registered avg function.')
    else:
        sys.exit(1)

    ''' TEST REGISTERED FUNCTIONS '''
    n = 10

    latencies = []
    total_error = 0.0
    total_error_perc = 0.0
    for _ in range(num_requests):
        time.sleep(0.75)
        start = time.time()
        uid = str(uuid.uuid4())

        vals = []
        futures = []
        for i in range(n):
            val = np.random.randint(100)
            vals.append(val)

            futures.append(cloud_avg(uid, i, n, val))

        results = []
        for future in futures:
            res = future.get()
            results.append(res)

        end = time.time()
        latencies.append(end - start)

        m = np.mean(vals)
        r = np.mean(results)

        error = np.sqrt((r - m) ** 2)
        total_error += error

        error_perc = np.abs((m - r) / m)
        total_error_perc += error_perc

    print('Average error: %.6f' % (total_error / num_requests))
    print('Average error percent: %.4f' % (total_error_perc / num_requests))
    return latencies, [], [], 0
