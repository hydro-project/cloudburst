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

import os

from distutils.core import setup
from setuptools.command.install import install
from setuptools import find_packages


class InstallWrapper(install):
    def run(self):
        # compile the relevant protobufs
        self.compile_proto()

        # Run the standard PyPi copy
        install.run(self)

        # remove the compiled protobufs
        self.cleanup()

    def compile_proto(self):
        os.system('./scripts/clean.sh')
        os.system('./scripts/build.sh')

    def cleanup(self):
        os.system('./scripts/clean.sh')
        os.system('rm -rf Cloudburst.egg-info')


setup(
        name='Cloudburst',
        version='0.1.0',
        packages=find_packages(),
        license='Apache v2',
        long_description='The Cloudburst Client and Server',
        install_requires=['zmq', 'protobuf', 'anna'],
        cmdclass={'install': InstallWrapper}
)
