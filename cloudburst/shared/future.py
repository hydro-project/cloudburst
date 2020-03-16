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


class CloudburstFuture():
    def __init__(self, obj_id, kvs_client, serializer):
        self.obj_id = obj_id
        self.kvs_client = kvs_client
        self.serializer = serializer

    def get(self):
        obj = self.kvs_client.get(self.obj_id)[self.obj_id]

        while obj is None:
            obj = self.kvs_client.get(self.obj_id)[self.obj_id]

        return self.serializer.load_lattice(obj)
