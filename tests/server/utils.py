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

from anna.lattices import LWWPairLattice

from cloudburst.server.utils import get_func_kvs_name
from cloudburst.shared.proto.cloudburst_pb2 import Dag
from cloudburst.shared.serializer import Serializer

serializer = Serializer()


def create_function(function, kvs_client, fname='func', ltype=LWWPairLattice):
    serialized = serializer.dump_lattice(function, ltype)
    kvs_client.put(get_func_kvs_name(fname), serialized)


def create_linear_dag(functions, fnames, kvs_client, dname,
                      lattice_type=LWWPairLattice):
    dag = Dag()
    dag.name = dname

    prev = None

    for index, fname in enumerate(fnames):
        function = functions[index]
        create_function(function, kvs_client, fname, lattice_type)

        ref = dag.functions.add()
        ref.name = fname

        if prev:
            link = dag.connections.add()
            link.source = prev
            link.sink = fname

        prev = fname

    return dag
