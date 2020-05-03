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
import time
import unittest

from anna.lattices import (
    LWWPairLattice,
    MultiKeyCausalLattice,
    SingleKeyCausalLattice,
    VectorClock
)

from cloudburst.server.executor.call import exec_function, exec_dag_function
from cloudburst.server.executor.user_library import CloudburstUserLibrary
from cloudburst.server.utils import DEFAULT_VC
from cloudburst.shared.proto.cloudburst_pb2 import (
    DagSchedule,
    DagTrigger,
    FunctionCall,
    GenericResponse,
    NORMAL, MULTI,  # Cloudburst's supported consistency modes
    EXECUTION_ERROR, FUNC_NOT_FOUND,  # Cloudburst's error types
    MULTIEXEC # Cloudburst's execution types
)
from cloudburst.shared.reference import CloudburstReference
from cloudburst.shared.serializer import Serializer
from tests.mock import kvs_client, zmq_utils
from tests.server.utils import (
    create_function,
    create_linear_dag
)

serializer = Serializer()
logging.disable(logging.CRITICAL)


class TestExecutorCall(unittest.TestCase):
    '''
    Tests for function execution in the context of an individual request and
    as a part of a larger DAG of requests, including resolving data from the
    KVS, correctly performing lattice encapsulation/deencapsulation, and
    correctly generating causal consistency-related metadata.
    '''

    def setUp(self):
        self.ip = '127.0.0.1'
        self.response_key = 'result'

        self.kvs_client = kvs_client.MockAnnaClient()
        self.socket = zmq_utils.MockZmqSocket()
        self.pusher_cache = zmq_utils.MockPusherCache()
        self.user_library = CloudburstUserLibrary(zmq_utils.MockZmqContext(),
                                               self.pusher_cache, self.ip, 0,
                                               self.kvs_client)

    ''' INDIVIDUAL FUNCTION EXECUTION TESTS '''

    def test_exec_function_normal(self):
        '''
        Tests creating and executing a function in normal mode, ensuring that
        no messages are sent outside of the system and that the serialized
        result is as expected.
        '''
        # Create the function and put it into the KVS.
        def func(_, x): return x * x
        fname = 'square'
        arg = 2

        # Put the function into the KVS and create a function call.
        create_function(func, self.kvs_client, fname)
        call = self._create_function_call(fname, [arg], NORMAL)
        self.socket.inbox.append(call.SerializeToString())

        # Execute the function call.
        exec_function(self.socket, self.kvs_client, self.user_library, {}, {})

        # Assert that there have been 0 messages sent.
        self.assertEqual(len(self.socket.outbox), 0)

        # Retrieve the result, ensure it is a LWWPairLattice, then deserialize
        # it.
        result = self.kvs_client.get(self.response_key)[self.response_key]
        self.assertEqual(type(result), LWWPairLattice)
        result = serializer.load_lattice(result)

        # Check that the output is equal to a local function execution.
        self.assertEqual(result, func('', arg))

    def test_exec_function_causal(self):
        '''
        Tests creating and executing a function in causal mode, ensuring that
        no messages are sent outside of the system and that the serialized
        result is as expected.
        '''
        # Create the function and serialize it into a lattice.
        def func(_, x): return x * x
        fname = 'square'
        create_function(func, self.kvs_client, fname, SingleKeyCausalLattice)
        arg = 2

        # Put the function into the KVS and create a function call.
        call = self._create_function_call(fname, [arg], MULTI)
        self.socket.inbox.append(call.SerializeToString())

        # Execute the function call.
        exec_function(self.socket, self.kvs_client, self.user_library, {}, {})

        # Assert that there have been 0 messages sent.
        self.assertEqual(len(self.socket.outbox), 0)

        # Retrieve the result, ensure it is a MultiKeyCausalLattice, then
        # deserialize it. Also check to make sure we have an empty vector clock
        # because this request populated no dependencies.
        result = self.kvs_client.get(self.response_key)[self.response_key]
        self.assertEqual(type(result), MultiKeyCausalLattice)
        self.assertEqual(result.vector_clock, DEFAULT_VC)
        result = serializer.load_lattice(result)[0]

        # Check that the output is equal to a local function execution.
        self.assertEqual(result, func('', arg))

    def test_exec_function_nonexistent(self):
        '''
        Attempts to execute a non-existent function and ensures that an error
        is thrown and returned to the user.
        '''
        # Create a call to a function that does not exist.
        call = self._create_function_call('bad_func', [1], NORMAL)
        self.socket.inbox.append(call.SerializeToString())

        # Attempt to execute the nonexistent function.
        exec_function(self.socket, self.kvs_client, self.user_library, {}, {})

        # Assert that there have been 0 messages sent.
        self.assertEqual(len(self.socket.outbox), 0)

        # Retrieve the result from the KVS and ensure that it is a lattice as
        # we expect. Deserialize it and check for an error.
        result = self.kvs_client.get(self.response_key)[self.response_key]
        self.assertEqual(type(result), LWWPairLattice)
        result = serializer.load_lattice(result)

        # Check the type and values of the error.
        self.assertEqual(type(result), tuple)
        self.assertEqual(result[0], 'ERROR')

        # Unpack the GenericResponse and check its values.
        response = GenericResponse()
        response.ParseFromString(result[1])
        self.assertEqual(response.success, False)
        self.assertEqual(response.error, FUNC_NOT_FOUND)

    def test_exec_function_with_error(self):
        '''
        Attempts to executet a function that raises an error during its
        execution. Ensures that an error is returned to the user.
        '''
        e_msg = 'This is a broken_function!'

        def func(_, x):
            raise ValueError(e_msg)
        fname = 'func'
        create_function(func, self.kvs_client, fname)

        # Put the functin into the KVS and create a function call.
        call = self._create_function_call(fname, [''], NORMAL)
        self.socket.inbox.append(call.SerializeToString())

        # Execute the function call.
        exec_function(self.socket, self.kvs_client, self.user_library, {}, {})

        # Retrieve the result from the KVS and ensure that it is the correct
        # lattice type.
        result = self.kvs_client.get(self.response_key)[self.response_key]
        self.assertEqual(type(result), LWWPairLattice)
        result = serializer.load_lattice(result)

        # Check the type and values of the error.
        self.assertEqual(type(result), tuple)
        self.assertTrue(e_msg in result[0])

        # Unpack the GenericResponse and check its values.
        response = GenericResponse()
        response.ParseFromString(result[1])
        self.assertEqual(response.success, False)
        self.assertEqual(response.error, EXECUTION_ERROR)

    def test_exec_func_with_ref(self):
        '''
        Tests a function execution where the argument is a reference to the
        KVS in normal mode.
        '''
        # Create the function and serialize it into a lattice.
        def func(_, x): return x * x
        fname = 'square'
        create_function(func, self.kvs_client, fname)

        # Put an argument value into the KVS.
        arg_value = 2
        arg_name = 'key'
        self.kvs_client.put(arg_name, serializer.dump_lattice(arg_value))

        # Create and serialize the function call.
        call = self._create_function_call(
            fname, [CloudburstReference(arg_name, True)], NORMAL)
        self.socket.inbox.append(call.SerializeToString())

        # Execute the function call.
        exec_function(self.socket, self.kvs_client, self.user_library, {}, {})

        # Assert that there have been 0 messages sent.
        self.assertEqual(len(self.socket.outbox), 0)

        # Retrieve the result, ensure it is a LWWPairLattice, then deserialize
        # it.
        result = self.kvs_client.get(self.response_key)[self.response_key]
        self.assertEqual(type(result), LWWPairLattice)
        result = serializer.load_lattice(result)

        # Check that the output is equal to a local function execution.
        self.assertEqual(result, func('', arg_value))

    def test_exec_func_with_causal_ref(self):
        '''
        Tests a function execution where the argument is a reference to the
        KVS in causal mode. Ensures that the result has the correct causal
        dependencies and metadata.
        '''
        # Create the function and serialize it into a lattice.
        def func(_, x): return x * x
        fname = 'square'
        create_function(func, self.kvs_client, fname, SingleKeyCausalLattice)

        # Put an argument value into the KVS.
        arg_value = 2
        arg_name = 'key'
        self.kvs_client.put(arg_name, serializer.dump_lattice(
            arg_value, MultiKeyCausalLattice))

        # Create and serialize the function call.
        call = self._create_function_call(
            fname, [CloudburstReference(arg_name, True)], MULTI)
        self.socket.inbox.append(call.SerializeToString())

        # Execute the function call.
        exec_function(self.socket, self.kvs_client, self.user_library, {}, {})

        # Assert that there have been 0 messages sent.
        self.assertEqual(len(self.socket.outbox), 0)

        # Retrieve the result, ensure it is a MultiKeyCausalLattice, then
        # deserialize it.
        result = self.kvs_client.get(self.response_key)[self.response_key]
        self.assertEqual(type(result), MultiKeyCausalLattice)
        self.assertEqual(result.vector_clock, DEFAULT_VC)
        self.assertEqual(len(result.dependencies.reveal()), 1)
        self.assertTrue(arg_name in result.dependencies.reveal())
        self.assertEqual(result.dependencies.reveal()[arg_name], DEFAULT_VC)
        result = serializer.load_lattice(result)[0]

        # Check that the output is equal to a local function execution.
        self.assertEqual(result, func('', arg_value))

    def test_exec_with_set(self):
        '''
        Tests a single function execution with a set input as an argument to
        validate that sets are correctly handled.
        '''
        def func(_, x): return sum(x)
        fname = 'set_sum'
        arg_value = {2, 3, 4}
        arg_name = 'set'

        self.kvs_client.put(arg_name, serializer.dump_lattice(arg_value))

        # Put the function into the KVS and create a function call.
        create_function(func, self.kvs_client, fname)
        call = self._create_function_call(
            fname, [CloudburstReference(arg_name, True)], NORMAL)
        self.socket.inbox.append(call.SerializeToString())

        # Execute the function call.
        exec_function(self.socket, self.kvs_client, self.user_library, {}, {})

        # Assert that there have been 0 messages sent.
        self.assertEqual(len(self.socket.outbox), 0)

        # Retrieve the result, ensure it is a LWWPairLattice, then deserialize
        # it.
        result = self.kvs_client.get(self.response_key)[self.response_key]
        self.assertEqual(type(result), LWWPairLattice)
        result = serializer.load_lattice(result)

        # Check that the output is equal to a local function execution.
        self.assertEqual(result, func('', arg_value))

    def test_exec_with_ordered_set(self):
        '''
        Tests a single function execution with an ordered set input as an
        argument to validate that ordered sets are correctly handled.
        '''
        def func(_, x): return len(x) >= 2 and x[0] < x[1]
        fname = 'set_order'
        arg_value = [2, 3]
        arg_name = 'set'

        self.kvs_client.put(arg_name, serializer.dump_lattice(arg_value))

        # Put the function into the KVS and create a function call.
        create_function(func, self.kvs_client, fname)
        call = self._create_function_call(
            fname, [CloudburstReference(arg_name, True)], NORMAL)
        self.socket.inbox.append(call.SerializeToString())

        # Execute the function call.
        exec_function(self.socket, self.kvs_client, self.user_library, {}, {})

        # Assert that there have been 0 messages sent.
        self.assertEqual(len(self.socket.outbox), 0)

        # Retrieve the result, ensure it is a LWWPairLattice, then deserialize
        # it.
        result = self.kvs_client.get(self.response_key)[self.response_key]
        self.assertEqual(type(result), LWWPairLattice)
        result = serializer.load_lattice(result)

        # Check that the output is equal to a local function execution.
        self.assertEqual(result, func('', arg_value))

    def test_exec_class_function(self):
        '''
        Tests creating and executing a class method in normal mode, ensuring
        that no messages are sent outside of the system and that the serialized
        result is as expected.
        '''
        # Create the function and put it into the KVS.
        class Test:
            def __init__(self, cloudburst, num):
                self.num = num

            def run(self, cloudburst, inp):
                return inp + self.num

        fname = 'class'
        init_arg = 3
        arg = 2

        # Put the function into the KVS and create a function call.
        create_function((Test, (init_arg,)), self.kvs_client, fname)
        call = self._create_function_call(fname, [arg], NORMAL)
        self.socket.inbox.append(call.SerializeToString())

        # Execute the function call.
        exec_function(self.socket, self.kvs_client, self.user_library, {}, {})

        # Assert that there have been 0 messages sent.
        self.assertEqual(len(self.socket.outbox), 0)

        # Retrieve the result, ensure it is a LWWPairLattice, then deserialize
        # it.
        result = self.kvs_client.get(self.response_key)[self.response_key]
        self.assertEqual(type(result), LWWPairLattice)
        result = serializer.load_lattice(result)

        # Check that the output is equal to a local function execution.
        self.assertEqual(result, Test(None, init_arg).run('', arg))

    ''' DAG FUNCTION EXECUTION TESTS '''

    def test_exec_dag_sink(self):
        '''
        Tests that the last function in a DAG executes correctly and stores the
        result in the KVS.
        '''
        def func(_, x): return x * x
        fname = 'square'
        arg = 2
        dag = create_linear_dag([func], [fname], self.kvs_client, 'dag')
        schedule, triggers = self._create_fn_schedule(dag, arg, fname, [fname])

        exec_dag_function(self.pusher_cache, self.kvs_client, triggers, func,
                          schedule, self.user_library, {}, {}, [])

        # Assert that there have been 0 messages sent.
        self.assertEqual(len(self.socket.outbox), 0)

        # Retrieve the result, ensure it is a LWWPairLattice, then deserialize
        # it.
        result = self.kvs_client.get(schedule.id)[schedule.id]
        self.assertEqual(type(result), LWWPairLattice)
        result = serializer.load_lattice(result)

        # Check that the output is equal to a local function execution.
        self.assertEqual(result, func('', arg))

    def test_exec_causal_dag_sink(self):
        '''
        Tests that the last function in a causal DAG executes correctly and
        stores the result in the KVS. Also checks to make sure that causal
        metadata is properly created.
        '''
        def func(_, x): return x * x
        fname = 'square'
        arg = 2
        dag = create_linear_dag([func], [fname], self.kvs_client, 'dag',
                                MultiKeyCausalLattice)
        schedule, triggers = self._create_fn_schedule(dag, arg, fname, [fname],
                                                      MULTI)
        schedule.output_key = 'output_key'
        schedule.client_id = '12'

        # We know that there is only one trigger. We populate dependencies
        # explicitly in this trigger message to make sure that they are
        # reflected in the final result.
        kv = triggers[0].dependencies.add()
        kv.key = 'dependency'
        DEFAULT_VC.serialize(kv.vector_clock)

        exec_dag_function(self.pusher_cache, self.kvs_client, triggers, func,
                          schedule, self.user_library, {}, {}, [])

        # Assert that there have been 0 messages sent.
        self.assertEqual(len(self.socket.outbox), 0)

        # Retrieve the result and check its value and its metadata.
        result = self.kvs_client.get(schedule.output_key)[schedule.output_key]
        self.assertEqual(type(result), MultiKeyCausalLattice)

        # Check that the vector clock of the output corresponds ot the client
        # ID.
        self.assertEqual(result.vector_clock, VectorClock({schedule.client_id:
                                                           1}, True))

        # Check that the dependencies of the output match those specified in
        # the trigger.
        self.assertEqual(len(result.dependencies.reveal()), 1)
        self.assertTrue(kv.key in result.dependencies.reveal())
        self.assertEqual(result.dependencies.reveal()[kv.key], DEFAULT_VC)

        # Check that the output is equal to a local function execution.
        result = serializer.load_lattice(result)[0]
        self.assertEqual(result, func('', arg))

    def test_exec_dag_non_sink(self):
        '''
        Executes a non-sink function in a DAG and ensures that the correct
        downstream trigger was sent with a correct execution of the function.
        '''
        # Create two functions intended to be used in sequence.
        def incr(_, x): x + 1
        iname = 'incr'

        def square(_, x): return x * x
        sname = 'square'
        arg = 1

        # Create a DAG and a trigger for the first function in the DAG.
        dag = create_linear_dag([incr, square], [iname, sname],
                                self.kvs_client, 'dag')
        schedule, triggers = self._create_fn_schedule(dag, arg, iname, [iname,
                                                                        sname])

        exec_dag_function(self.pusher_cache, self.kvs_client, triggers, incr,
                          schedule, self.user_library, {}, {}, [])

        # Assert that there has been a message sent.
        self.assertEqual(len(self.pusher_cache.socket.outbox), 1)

        # Extract that message and check its contents.
        trigger = DagTrigger()
        trigger.ParseFromString(self.pusher_cache.socket.outbox[0])
        self.assertEqual(trigger.id, schedule.id)
        self.assertEqual(trigger.target_function, sname)
        self.assertEqual(trigger.source, iname)
        self.assertEqual(len(trigger.arguments.values), 1)

        val = serializer.load(trigger.arguments.values[0])
        self.assertEqual(val, incr('', arg))

    def test_exec_causal_dag_non_sink(self):
        '''
        Creates and executes a non-sink function in a causal-mode DAG. This
        should be exactly the same as the non-causal version of the test,
        except we ensure that the causal metadata is empty, because we don't
        have any KVS accesses.
        '''
        # Create two functions intended to be used in sequence.
        def incr(_, x): x + 1
        iname = 'incr'

        def square(_, x): return x * x
        sname = 'square'
        arg = 1

        # Create a DAG and a trigger for the first function in the DAG.
        dag = create_linear_dag([incr, square], [iname, sname],
                                self.kvs_client, 'dag', MultiKeyCausalLattice)
        schedule, triggers = self._create_fn_schedule(dag, arg, iname,
                                                      [iname, sname], MULTI)

        exec_dag_function(self.pusher_cache, self.kvs_client, triggers, incr,
                          schedule, self.user_library, {}, {}, [])

        # Assert that there has been a message sent.
        self.assertEqual(len(self.pusher_cache.socket.outbox), 1)

        # Extract that message and check its contents.
        trigger = DagTrigger()
        trigger.ParseFromString(self.pusher_cache.socket.outbox[0])
        self.assertEqual(trigger.id, schedule.id)
        self.assertEqual(trigger.target_function, sname)
        self.assertEqual(trigger.source, iname)
        self.assertEqual(len(trigger.arguments.values), 1)
        self.assertEqual(len(trigger.version_locations), 0)
        self.assertEqual(len(trigger.dependencies), 0)

        val = serializer.load(trigger.arguments.values[0])
        self.assertEqual(val, incr('', arg))

    def test_exec_causal_dag_non_sink_with_ref(self):
        '''
        Creates and executes a non-sink function in a causal-mode DAG. This
        version accesses a KVS key, so we ensure that data is appropriately
        cached and the metadata is passed downstream.
        '''
        # Create two functions intended to be used in sequence.
        def incr(_, x): x + 1
        iname = 'incr'

        def square(_, x): return x * x
        sname = 'square'

        # Put tthe argument into the KVS.
        arg_name = 'arg'
        arg_value = 1
        arg = serializer.dump_lattice(arg_value, MultiKeyCausalLattice)
        self.kvs_client.put(arg_name, arg)

        # Create a DAG and a trigger for the first function in the DAG.
        dag = create_linear_dag([incr, square], [iname, sname],
                                self.kvs_client, 'dag', MultiKeyCausalLattice)
        schedule, triggers = self._create_fn_schedule(
            dag, CloudburstReference(arg_name, True), iname, [iname, sname],
            MULTI)

        exec_dag_function(self.pusher_cache, self.kvs_client, triggers, incr,
                          schedule, self.user_library, {}, {}, [])

        # Assert that there has been a message sent.
        self.assertEqual(len(self.pusher_cache.socket.outbox), 1)

        # Extract that message and check its contents.
        trigger = DagTrigger()
        trigger.ParseFromString(self.pusher_cache.socket.outbox[0])
        self.assertEqual(trigger.id, schedule.id)
        self.assertEqual(trigger.target_function, sname)
        self.assertEqual(trigger.source, iname)
        self.assertEqual(len(trigger.arguments.values), 1)

        # Check the metadata of the key that is cached here after execution.
        locs = trigger.version_locations
        self.assertEqual(len(locs), 1)
        self.assertTrue(self.ip in locs.keys())
        self.assertEqual(len(locs[self.ip].keys), 1)
        kv = locs[self.ip].keys[0]
        self.assertEqual(kv.key, arg_name)
        self.assertEqual(VectorClock(dict(kv.vector_clock), True),
                         arg.vector_clock)

        # Check the metatada of the causal dependency passed downstream.
        self.assertEqual(len(trigger.dependencies), 1)
        kv = trigger.dependencies[0]
        self.assertEqual(kv.key, arg_name)
        self.assertEqual(VectorClock(dict(kv.vector_clock), True),
                         arg.vector_clock)

        val = serializer.load(trigger.arguments.values[0])
        self.assertEqual(val, incr('', arg_value))

    def test_dag_exec_multi_input_false(self):
        def incr(_, x): x + 1
        iname = 'incr'

        def square(_, x): return x * x
        sname = 'square'
        arg = 2

        # Create a DAG and a trigger for the first function in the DAG.
        dag = create_linear_dag([incr, square], [iname, sname],
                                self.kvs_client, 'dag')
        ref = dag.functions[1] if dag.functions[1].name == sname else \
            dag.functions[0]
        ref.type = MULTIEXEC
        ref.invalid_results.append(serializer.dump(4))

        schedule, triggers = self._create_fn_schedule(dag, arg, sname,
                                                      [iname, sname])

        result = exec_dag_function(self.pusher_cache, self.kvs_client,
                                   triggers, square, schedule,
                                   self.user_library, {}, {}, [])

        self.assertFalse(result)
        data = self.kvs_client.get(schedule.id)
        self.assertEqual(data[schedule.id], None)

    def test_dag_exec_multi_input_true(self):
        def incr(_, x): x + 1
        iname = 'incr'

        def square(_, x): return x * x
        sname = 'square'
        arg = 2

        # Create a DAG and a trigger for the first function in the DAG.
        dag = create_linear_dag([incr, square], [iname, sname],
                                self.kvs_client, 'dag')
        ref = dag.functions[1] if dag.functions[1].name == sname else \
            dag.functions[0]
        ref.type = MULTIEXEC
        ref.invalid_results.append(serializer.dump(5))

        schedule, triggers = self._create_fn_schedule(dag, arg, sname,
                                                      [iname, sname])

        result = exec_dag_function(self.pusher_cache, self.kvs_client,
                                   triggers, square, schedule,
                                   self.user_library, {}, {}, [])

        self.assertTrue(result)
        data = self.kvs_client.get(schedule.id)
        self.assertEqual(serializer.load_lattice(data[schedule.id]),
                         square(None, arg))

    ''' HELPER FUNCTIONS '''

    def _create_function_call(self, fname, args, consistency):
        call = FunctionCall()
        call.name = fname
        call.request_id = 1
        call.response_key = self.response_key
        call.consistency = consistency

        for arg in args:
            val = call.arguments.values.add()
            serializer.dump(arg, val, False)

        return call

    def _create_fn_schedule(self, dag, arg, target, fnames,
                            consistency=NORMAL):
        schedule = DagSchedule()
        schedule.id = 'id'
        schedule.dag.CopyFrom(dag)
        schedule.target_function = target
        schedule.consistency = consistency

        # The BEGIN trigger is sent by the scheduler.
        schedule.triggers.append('BEGIN')

        # We set all locations as thread ID 0.
        for fname in fnames:
            schedule.locations[fname] = self.ip + ':0'
        val = schedule.arguments[target].values.add()

        # Set the square function's argument.
        serializer.dump(arg, val, False)
        schedule.start_time = time.time()

        # Create a trigger corresponding to this DAG.
        trigger = DagTrigger()
        trigger.id = schedule.id
        trigger.target_function = schedule.target_function
        trigger.source = 'BEGIN'

        return schedule, [trigger] # {'BEGIN': trigger}
