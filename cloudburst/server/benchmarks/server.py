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

import zmq

from cloudburst.client.client import CloudburstConnection
from cloudburst.server.benchmarks import (
    composition,
    locality,
    lambda_locality,
    mobilenet,
    predserving,
    scaling,
    utils
)
import cloudburst.server.utils as sutils

BENCHMARK_START_PORT = 3000

logging.basicConfig(filename='log_benchmark.txt', level=logging.INFO,
                    format='%(asctime)s %(message)s')

def benchmark(ip, cloudburst_address, tid):
    cloudburst = CloudburstConnection(cloudburst_address, ip, tid)

    ctx = zmq.Context(1)

    benchmark_start_socket = ctx.socket(zmq.PULL)
    benchmark_start_socket.bind('tcp://*:' + str(BENCHMARK_START_PORT + tid))
    kvs = cloudburst.kvs_client

    while True:
        msg = benchmark_start_socket.recv_string()
        splits = msg.split(':')

        resp_addr = splits[0]
        bname = splits[1]
        num_requests = int(splits[2])
        if len(splits) > 3:
            create = bool(splits[3])
        else:
            create = False

        sckt = ctx.socket(zmq.PUSH)
        sckt.connect('tcp://' + resp_addr + ':3000')
        run_bench(bname, num_requests, cloudburst, kvs, sckt, create)


def run_bench(bname, num_requests, cloudburst, kvs, sckt, create=False):
    logging.info('Running benchmark %s, %d requests.' % (bname, num_requests))

    if bname == 'composition':
        total, scheduler, kvs, retries = composition.run(cloudburst, num_requests,
                                                         sckt)
    elif bname == 'locality':
        total, scheduler, kvs, retries = locality.run(cloudburst, num_requests,
                                                      create, sckt)
    elif bname == 'redis' or bname == 's3':
        total, scheduler, kvs, retries = lambda_locality.run(bname, kvs,
                                                             num_requests,
                                                             sckt)
    elif bname == 'predserving':
        total, scheduler, kvs, retries = predserving.run(cloudburst, num_requests,
                                                         sckt)
    elif bname == 'mobilenet':
        total, scheduler, kvs, retries = mobilenet.run(cloudburst, num_requests,
                                                       sckt)
    elif bname == 'scaling':
        total, scheduler, kvs, retries = scaling.run(cloudburst, num_requests,
                                                     sckt, create)
    else:
        logging.info('Unknown benchmark type: %s!' % (bname))
        sckt.send(b'END')
        return

    # some benchmark modes return no results
    if not total:
        sckt.send(b'END')
        logging.info('*** Benchmark %s finished. It returned no results. ***'
                     % (bname))
        return
    else:
        sckt.send(b'END')
        logging.info('*** Benchmark %s finished. ***' % (bname))

    logging.info('Total computation time: %.4f' % (sum(total)))
    if len(total) > 0:
        utils.print_latency_stats(total, 'E2E', True)
    if len(scheduler) > 0:
        utils.print_latency_stats(scheduler, 'SCHEDULER', True)
    if len(kvs) > 0:
        utils.print_latency_stats(kvs, 'KVS', True)
    logging.info('Number of KVS get retries: %d' % (retries))


if __name__ == '__main__':
    if len(sys.argv) > 1:
        conf_file = sys.argv[1]
    else:
        conf_file = 'conf/cloudburst-config.yml'

    conf = sutils.load_conf(conf_file)
    bench_conf = conf['benchmark']

    benchmark(conf['ip'], bench_conf['cloudburst_address'],
              int(bench_conf['thread_id']))
