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

from zmq import EAGAIN, ZMQError


class MockZmqSocket():
    def __init__(self):
        self.inbox = []
        self.outbox = []
        self.address = None

    def send(self, message):
        self.outbox.append(message)

    def send_pyobj(self, message):
        self.outbox.append(message)

    def send_string(self, message):
        self.outbox.append(message)

    def recv(self):
        return self.inbox.pop()

    # This function takes an optional argument beacuse the user library passes
    # in the zmq.NOBLOCK argument, which we can safely ignore in our mocked
    # implementation. Instead of erroring on pop(), we raise a ZMQ error.
    def recv_pyobj(self, arg=None):
        if len(self.inbox) == 0:
            err = ZMQError()
            err.errno = EAGAIN

            raise err

        return self.inbox.pop()

    def recv_string(self):
        return str(self.inbox.pop())

    def bind(self, addr):
        self.address = addr


class MockZmqContext():
    def __init__(self):
        self.sckt = MockZmqSocket()

    def socket(self, typ):
        return self.sckt


class MockPusherCache():
    def __init__(self, socket=None):
        if not socket:
            self.socket = MockZmqSocket()
        else:
            self.socket = socket

        self.addresses = []

    def get(self, address):
        self.addresses.append(address)
        return self.socket
