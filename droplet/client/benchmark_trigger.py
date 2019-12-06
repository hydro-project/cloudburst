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
import time
import zmq

import cloudpickle as cp

from droplet.server.benchmarks import utils

logging.basicConfig(filename='log_trigger.txt', level=logging.INFO,
                    format='%(asctime)s %(message)s')

NUM_THREADS = 3

ips = []
with open('bench_ips.txt', 'r') as f:
    line = f.readline()
    while line:
        ips.append(line.strip())
        line = f.readline()

msg = sys.argv[1]
ctx = zmq.Context(1)

recv_socket = ctx.socket(zmq.PULL)
recv_socket.bind('tcp://*:3000')

sent_msgs = 0

if 'create' in msg:
    sckt = ctx.socket(zmq.PUSH)
    sckt.connect('tcp://' + ips[0] + ':3000')

    sckt.send_string(msg)
    sent_msgs += 1
else:
    for ip in ips:
        for tid in range(NUM_THREADS):
            sckt = ctx.socket(zmq.PUSH)
            sckt.connect('tcp://' + ip + ':' + str(3000 + tid))

            sckt.send_string(msg)
            sent_msgs += 1

reads = []
writes = []
end_recv = 0

epoch_recv = 0
epoch = 1
epoch_thruput = 0
epoch_start = time.time()

while end_recv < sent_msgs:
    msg = recv_socket.recv()

    if b'END' in msg:
        end_recv += 1
    else:
        new_reads, new_writes = cp.loads(msg)
        reads += new_reads
        writes += new_writes

        epoch_recv += 1

        logging.info('\n\n*** EPOCH %d ***' % (epoch))
        utils.print_latency_stats(reads, 'reads', True)
        utils.print_latency_stats(writes, 'writes', True)

        # if epoch_recv == sent_msgs:
        #     epoch_end = time.time()
        #     elapsed = epoch_end - epoch_start

        #     logging.info('\n\n*** EPOCH %d ***' % (epoch))
        #     utils.print_latency_stats(reads, 'reads', True)
        #     utils.print_latency_stats(writes, 'writes', True)

        #     epoch_recv = 0
        #     epoch_start = time.time()
        #     epoch += 1

logging.info('*** END ***')

utils.print_latency_stats(reads, 'reads', True)
utils.print_latency_stats(writes, 'writes', True)
