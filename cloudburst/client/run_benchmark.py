#!/usr/bin/env python3

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
import sys

from cloudburst.client.client import CloudburstConnection
from cloudburst.server.benchmarks import (
    centr_avg,
    composition,
    dist_avg,
    locality,
    mobilenet,
    predserving,
    scaling,
    summa,
    utils
)


logging.basicConfig(stream=sys.stdout, level=logging.INFO)

if len(sys.argv) < 4:
    print('Usage: ./run_benchmark.py benchmark_name function_elb num_requests '
          + '{ip}')
    sys.exit(1)

f_elb = sys.argv[2]
num_requests = int(sys.argv[3])

if len(sys.argv) == 5:
    ip = sys.argv[4]
    cloudburst_client = CloudburstConnection(f_elb, ip)
else:
    cloudburst_client = CloudburstConnection(f_elb)

bname = sys.argv[1]

if bname == 'composition':
    total, scheduler, kvs, retries = composition.run(cloudburst_client,
                                                     num_requests, None)
elif bname == 'locality':
    locality.run(cloudburst_client, num_requests, True, None)
    total, scheduler, kvs, retries = locality.run(cloudburst_client, num_requests,
                                                  False, None)
elif bname == 'mobilenet':
    total, scheduler, kvs, retries = mobilenet.run(cloudburst_client, num_requests,
                                                   None)
elif bname == 'pred_serving':
    total, scheduler, kvs, retries = predserving.run(cloudburst_client,
                                                     num_requests, None)
elif bname == 'avg':
    total, scheduler, kvs, retries = dist_avg.run(cloudburst_client, num_requests,
                                                   None)
elif bname == 'center_avg':
    total, scheduler, kvs, retries = centr_avg.run(cloudburst_client, num_requests,
                                                  None)
elif bname == 'summa':
    total, scheduler, kvs, retries = summa.run(cloudburst_client, num_requests,
                                               None)
elif bname == 'scaling':
    total, scheduler, kvs, retries = scaling.run(cloudburst_client, num_requests,
                                                 None)
else:
    print('Unknown benchmark type: %s!' % (bname))

print('Total computation time: %.4f' % (sum(total)))

if total:
    utils.print_latency_stats(total, 'E2E')
if scheduler:
    utils.print_latency_stats(scheduler, 'SCHEDULER')
if kvs:
    utils.print_latency_stats(kvs, 'KVS')

if retries > 0:
    print('Number of KVS get retries: %d' % (retries))
