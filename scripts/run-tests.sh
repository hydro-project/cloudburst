#!/bin/bash

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

# Clean the protobuf definitions and recreate them so that they are up to date.
./scripts/clean.sh && ./scripts/build.sh

# We need this on Ubuntu to make sure that the packages are found when
# attempting to execute the tests.
export PYTHONPATH=$PYTHONPATH:$(pwd)

coverage run tests

# If the tests failed, do not generate a report, but report a failure instead.
EXIT=$?
if [[ $EXIT -ne 0 ]]; then
  exit $EXIT
fi

coverage report -m
