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


class BaseCloudburstSchedulerPolicy():
    '''
    An abstract class for the Cloudburst scheduler policy. The policy is intended
    to track its relevant metadata over time, and it can optionally choose to
    ignore metadata not relevant to its decisions.
    '''

    def __init__(self):
        raise NotImplementedError

    def pick_executor(self, references, function_name=None):
        '''
        Pick a executor node to execute a particular request from the
        candidate set that is passed in and the set of references for this
        particular request.

        Returns the IP-thread ID pair of the executor chosen.
        '''
        raise NotImplementedError

    def pin_function(self, dag_name, function_name):
        '''
        Pick an executor thread on which to pin a particular DAG function. None
        of these updates are stored permanently until we receive a commit_dag
        call; if we receive a discard_dag call, we unpin this metadata and
        throw it away.

        Returns True if the function was successfully pinned and False if we
        ran out of resources.
        '''
        raise NotImplementedError

    def commit_dag(self, dag_name):
        '''
        Persist the function location metadata generated via a sequence of
        pin_function calls.
        '''

    def discard_dag(self, dag, pending):
        '''
        Once we are done with a DAG (either because delete was called or
        creation failed), we tell the policy engine that we no longer care
        about this DAG and can discard its metadata. If pending is True, this
        DAG had not been fully successfully created yet.
        '''

    def process_status(self, status):
        '''
        Process a status update that is received from an executor thread for
        any metadata that is relevant to this policy (e.g., if the node reports
        high load, we should stop sending it requests).
        '''
        raise NotImplementedError

    def update(self):
        '''
        Since metadata becomes stale, the server process will periodically ask
        the policy engine to prune its metadata, every second. The policy
        engine can choose to ignore it or wait for longer intervals before
        pruning.
        '''
        raise NotImplementedError

    def update_function_locations(self, function_locations):
        '''
        Update this policy's view of which functions are stored where, either
        because we've successfully created a DAG or because we have received
        updates from another schedule that has new metadata.
        '''
        raise NotImplementedError
