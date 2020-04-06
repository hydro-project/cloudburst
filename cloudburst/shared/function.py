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

from cloudburst.shared.future import CloudburstFuture
from cloudburst.shared.serializer import Serializer

serializer = Serializer()


class CloudburstFunction():
    def __init__(self, name, conn, kvs_client):
        self.name = name
        self._conn = conn
        self._kvs_client = kvs_client

    def get_name(self):
        return self.name

    def __call__(self, *args):
        obj_id = self._conn.exec_func(self.name, args)
        if obj_id is None or len(obj_id) == 0:
            return None

        return CloudburstFuture(obj_id, self._kvs_client, serializer)
