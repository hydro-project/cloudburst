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

from anna.lattices import (
    ListBasedOrderedSet,
    LWWPairLattice,
    MapLattice,
    OrderedSetLattice,
    MultiKeyCausalLattice,
    SingleKeyCausalLattice,
    SetLattice
)
import cloudpickle as cp
import numpy as np
import pandas as pd
import pyarrow as pa

from cloudburst.server.utils import DEFAULT_VC, generate_timestamp
from cloudburst.shared.proto.aft_pb2 import KeyValuePair
from cloudburst.shared.proto.cloudburst_pb2 import (
    DEFAULT, NUMPY, STRING,  # Cloudburst's supported serializer types
    Value
)
from cloudburst.shared.reference import CloudburstReference
import cloudburst.shared.future as future


class Serializer():
    def __init__(self, string_format='raw_unicode_escape'):
        self.string_format = string_format

    def load(self, data):
        # If the type of the input is bytes, then we need to deserialize the
        # input first.
        if type(data) == bytes:
            # First check if this is an AFT value. If it is, we will
            # deserialize it, and if it isn't the call to ParseFromString will
            # fail, and we ignore it. Note that anything that comes out of an
            # AFT KeyValuePair must have been in an LWWPairLattice by
            # construction (AFT doesn't support anything else).
            try:
                val = KeyValuePair()
                val.ParseFromString(data)
                if len(val.value) > 0:
                    data = val.value
            except:
                pass

            val = Value()
            val.ParseFromString(data)
        elif type(data).__name__ == Value.__name__:
            # If it's already deserialized, we can just proceed.
            val = data
        else:
            raise ValueError(f'''Input to load was of unsupported type
                             {str(type(data))}.''')

        if val.type == DEFAULT:
            try:
                return self._load_default(val.body)
            except: # Unpickling error.
                return val.body
        elif val.type == STRING:
            return self._load_string(val.body)
        elif val.type == NUMPY:
            return self._load_numpy(val.body)

    def dump(self, data, valobj=None, serialize=True):
        if not valobj:
            valobj = Value()

        # If we are attempting to pass a future into another function, we
        # simply turn it into a reference because the runtime knows how to
        # automatically resolve it.
        if type(data) == bytes:
            valobj.body = data
            valobj.type = DEFAULT
        elif isinstance(data, future.CloudburstFuture):
            valobj.body = self._dump_default(CloudburstReference(data.obj_id,
                                                              True))
            valobj.type = DEFAULT
        elif isinstance(data, np.ndarray) or isinstance(data, pd.DataFrame):
            valobj.body = self._dump_numpy(data)
            valobj.type = NUMPY
        elif isinstance(data, str):
            valobj.body =  self._dump_string(data)
            valobj.type = STRING
        else:
            valobj.body = self._dump_default(data)
            valobj.type = DEFAULT

        if not serialize:
            return valobj

        return valobj.SerializeToString()

    def load_lattice(self, lattice):
        if isinstance(lattice, LWWPairLattice):
            result = self.load(lattice.reveal())
        elif type(lattice) in [OrderedSetLattice, MultiKeyCausalLattice,
                               SetLattice, SingleKeyCausalLattice]:
            result = list()
            for v in lattice.reveal():
                result.append(self.load(v))
        elif isinstance(lattice, MapLattice):
            result = {}
            revealed = lattice.reveal()

            for key in revealed:
                result[key] = self.load_lattice(revealed[key])
        else:
            raise ValueError(f'Unsupported lattice type: {str(type(lattice))}')

        return result

    def dump_lattice(self, value, typ=None, causal_dependencies={}):
        if not typ:
            if isinstance(value, set):
                return self.dump_lattice(value, SetLattice)
            else:
                return self.dump_lattice(value, LWWPairLattice)

        if typ == SetLattice:
            result = set()
            for v in value:
                result.add(self.dump(v))

            result = SetLattice(result)
        elif typ == MapLattice:
            result = {}
            for key in value:
                result[key] = self.dump_lattice(value[key])

            result = MapLattice(result)
        elif typ == OrderedSetLattice:
            result = list()
            for v in value:
                result.append(self.dump(v))

            result = OrderedSetLattice(ListBasedOrderedSet(result))
        elif typ == LWWPairLattice:
            result = LWWPairLattice(generate_timestamp(0), self.dump(value))
        elif typ == SingleKeyCausalLattice:
            # We assume that we will use the default vector clock for causal
            # metadata.
            data = SetLattice({self.dump(value)})
            result = SingleKeyCausalLattice(DEFAULT_VC, data)
        elif typ == MultiKeyCausalLattice:
            # We assume that we will use the default vector clock for causal
            # metadata.
            data = SetLattice({self.dump(value)})
            result = MultiKeyCausalLattice(DEFAULT_VC,
                                           MapLattice(causal_dependencies),
                                           data)
        else:
            raise ValueError(f'Unexpected lattice type: {str(typ)}')

        return result

    def _dump_default(self, msg):
        return cp.dumps(msg)

    def _load_default(self, msg):
        if not msg:
            return msg

        return cp.loads(msg)

    def _dump_string(self, msg):
        return bytes(msg, 'utf-8')

    def _load_string(self, msg):
        return str(msg, 'utf-8')

    def _dump_numpy(self, msg):
        return pa.serialize(msg).to_buffer().to_pybytes()

    def _load_numpy(self, msg):
        if not msg:
            return msg

        return pa.deserialize(msg)
