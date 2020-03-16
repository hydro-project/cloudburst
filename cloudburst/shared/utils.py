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

# The port on which clients can connect to the Cloudburst service.
CONNECT_PORT = 5000

# The port on which function creation calls are sent to Cloudburst.
FUNC_CREATE_PORT = 5001

# The port on which function invocation messages are sent.
FUNC_CALL_PORT = 5002

# The port on which clients retrieve the list of registered functions.
LIST_PORT = 5003

# The port on which DAG creation requests are sent.
DAG_CREATE_PORT = 5004

# The port on which DAG invocation requests are made.
DAG_CALL_PORT = 5005

# The port on which DAG deletion requests are made.
DAG_DELETE_PORT = 5006
