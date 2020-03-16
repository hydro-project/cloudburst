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

import sys
import unittest

from tests.server.executor import (
    test_call as test_executor_call,
    test_pin,
    test_user_library
)
from tests.server.scheduler import (
    test_call as test_scheduler_call,
    test_create
)
from tests.server.scheduler.policy import test_default_policy
from tests.shared import test_serializer


def cloudburst_test_suite():
    cloudburst_tests = []
    loader = unittest.TestLoader()

    # Load Cloudburst Executor tests
    cloudburst_tests.append(
        loader.loadTestsFromTestCase(test_executor_call.TestExecutorCall))
    cloudburst_tests.append(
        loader.loadTestsFromTestCase(test_pin.TestExecutorPin))
    cloudburst_tests.append(
        loader.loadTestsFromTestCase(test_user_library.TestUserLibrary))

    # Load Cloudburst Scheduler tests
    cloudburst_tests.append(
        loader.loadTestsFromTestCase(test_scheduler_call.TestSchedulerCall))
    cloudburst_tests.append(
        loader.loadTestsFromTestCase(test_create.TestSchedulerCreate))
    cloudburst_tests.append(
        loader.loadTestsFromTestCase(
            test_default_policy.TestDefaultSchedulerPolicy))

    # Load miscellaneous tests
    cloudburst_tests.append(loader.loadTestsFromTestCase(
        test_serializer.TestSerializer))

    return unittest.TestSuite(cloudburst_tests)


if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    result = runner.run(cloudburst_test_suite())

    if not result.wasSuccessful():
        sys.exit(1)
