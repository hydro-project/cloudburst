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

from anna.base_client import BaseAnnaClient

from cloudburst.server.utils import DEFAULT_VC
from cloudburst.shared.proto.shared_pb2 import KeyVersion
from cloudburst.shared.proto.cloudburst_pb2 import NORMAL


class MockAnnaClient(BaseAnnaClient):
    '''
    A mock Anna KVS client to be used for testing purposes only. We extend the
    BaseAnnaClient, and we use it only for serialization and deserialization.
    If the constructor's serialize flag is set to True, then we serialize and
    deserialize data before storing it in memory. Otherwise, we store it as a
    regular object.
    '''

    def __init__(self, serialize=False):
        self.kvs = {}
        self.serialize = serialize

    def get(self, keys):
        if type(keys) is not list:
            keys = [keys]

        result = {}

        for key in keys:
            if key not in self.kvs:
                result[key] = None
            else:
                if self.serialize:
                    result[key] = self._deserialize(self.kvs[key])
                else:
                    result[key] = self.kvs[key]

        return result

    def put(self, keys, lattices):
        if type(keys) != list:
            keys = [keys]
            lattices = [lattices]

        for key, lattice in zip(keys, lattices):
            if self.serialize:
                lattice = self._serialize(lattice)

            self.kvs[key] = lattice
        return True

    def causal_get(self, keys, future_read_set=set(), key_version_locations={},
                   consistency=NORMAL, client_id=0):
        address = '127.0.0.1'
        versions = []
        for key in keys:
            kv = KeyVersion()
            kv.key = key
            DEFAULT_VC.serialize(kv.vector_clock)
            versions.append(kv)

        return (address, versions), self.get(keys)

    def causal_put(self, key, lattice, client_id=0):
        # TODO(vikram): Do we need to populate causal metadata here?
        return self.put(key, lattice)
