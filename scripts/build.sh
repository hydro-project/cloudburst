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

if [ -z "$(command -v protoc)" ]; then
  echo "The protoc tool is required before you can run Cloudburst locally."
  echo "Please install protoc manually, or use the scripts in" \
    "hydro-project/common to install dependencies before proceeding."
  exit 1
fi

rm -rf cloudburst/shared/proto
mkdir cloudburst/shared/proto
touch cloudburst/shared/proto/__init__.py
protoc -I=common/proto --python_out=cloudburst/shared/proto cloudburst.proto shared.proto
protoc -I=common/proto --python_out=cloudburst/shared/proto anna.proto shared.proto causal.proto
protoc -I=proto --python_out=cloudburst/shared/proto internal.proto

# NOTE: This is a hack. We have to do this because the protobufs are not
# packaged properly (in the protobuf definitions). This isn't an issue for C++
# builds, because all the header files are in one place, but it breaks our
# Python imports. Consider how to fix this in the future.
if [[ "$OSTYPE" = "darwin"* ]]; then
  sed -i '' "s/import shared_pb2/from . import shared_pb2/g" $(find cloudburst/shared/proto | grep pb2 | grep -v pyc | grep -v internal)
  sed -i '' "s/import anna_pb2/from . import anna_pb2/g" $(find cloudburst/shared/proto | grep pb2 | grep -v pyc | grep -v internal)
  sed -i '' "s/import cloudburst_pb2/from . import cloudburst_pb2/g" $(find cloudburst/shared/proto | grep pb2 | grep -v pyc | grep -v internal)
else
  # We assume other linux distributions
  sed -i "s|import shared_pb2|from . import shared_pb2|g" $(find cloudburst/shared/proto | grep pb2 | grep -v pyc | grep -v internal)
  sed -i "s|import anna_pb2|from . import anna_pb2|g" $(find cloudburst/shared/proto | grep pb2 | grep -v pyc | grep -v internal)
  sed -i "s|import cloudburst_pb2|from . import cloudburst_pb2|g" $(find cloudburst/shared/proto | grep pb2 | grep -v pyc | grep -v internal)
fi
