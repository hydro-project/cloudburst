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

import unittest

import numpy as np

from cloudburst.shared.proto.cloudburst_pb2 import (
    Value,
    DEFAULT, NUMPY  # Cloudburst's serializer types
)
from cloudburst.shared.future import CloudburstFuture
from cloudburst.shared.reference import CloudburstReference
from cloudburst.shared.serializer import Serializer
from tests.mock.kvs_client import MockAnnaClient


class TestSerializer(unittest.TestCase):
    '''
    This test suite tests various serializer interface methods to ensure that
    they serialize data correctly and raise errors on unknown types.
    '''

    def setUp(self):
        self.serializer = Serializer()

    def test_serialize_obj(self):
        '''
        Tests that a normal Python object is serialized correctly.
        '''
        obj = {'a set'}

        serialized = self.serializer.dump(obj, serialize=False)

        self.assertEqual(type(serialized), Value)
        self.assertEqual(serialized.type, DEFAULT)

        self.assertEqual(self.serializer.load(serialized), obj)

    def test_serialize_numpy(self):
        '''
        Tests that a numpy array is correctly serialized with PyArrow.
        '''
        obj = np.random.randn(100, 100)

        serialized = self.serializer.dump(obj, serialize=False)

        self.assertEqual(type(serialized), Value)
        self.assertEqual(serialized.type, NUMPY)

        deserialized = self.serializer.load(serialized)
        self.assertTrue(np.array_equal(deserialized, obj))

    def test_serialize_to_bytes(self):
        '''
        Tests that the serializer correctly converts to a serialized protobuf.
        '''
        obj = {'a set'}

        val = Value()
        serialized = self.serializer.dump(obj, val, True)

        self.assertEqual(type(serialized), bytes)
        val.ParseFromString(serialized)
        self.assertEqual(val.type, DEFAULT)

        self.assertEqual(self.serializer.load(serialized), obj)

    def test_serialize_future(self):
        '''
        Tests that the serializer correctly detects and converts a
        CloudburstFuture to a CloudburstReference.
        '''

        kvs_client = MockAnnaClient()
        future = CloudburstFuture('id', kvs_client, self.serializer)

        serialized = self.serializer.dump(future, serialize=False)

        self.assertEqual(type(serialized), Value)
        self.assertEqual(serialized.type, DEFAULT)

        reference = self.serializer.load(serialized)
        self.assertEqual(type(reference), CloudburstReference)
        self.assertEqual(future.obj_id, reference.key)
