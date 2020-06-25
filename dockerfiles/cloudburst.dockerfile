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

FROM hydroproject/base:latest

MAINTAINER Vikram Sreekanti <vsreekanti@gmail.com> version: 0.1

ARG repo_org=hydro-project
ARG source_branch=master
ARG build_branch=docker-build

USER root

# Download latest version of the code from relevant repository & branch -- if
# none are specified, we use hydro-project/cloudburst by default. Install the KVS
# client from the Anna project.
WORKDIR $HYDRO_HOME/cloudburst
RUN git remote remove origin && git remote add origin https://github.com/$repo_org/cloudburst
RUN git fetch -p origin && git checkout -b $build_branch origin/$source_branch
RUN rm -rf /usr/lib/python3/dist-packages/yaml
RUN rm -rf /usr/lib/python3/dist-packages/PyYAML-*
RUN pip3 install -r requirements.txt
WORKDIR $HYDRO_HOME
RUN rm -rf anna
RUN git clone --recurse-submodules https://github.com/hydro-project/anna
WORKDIR anna
RUN cd client/python && python3.6 setup.py install
WORKDIR /

# These installations are currently pipeline specific until we figure out a
# better way to do package management for Python.
RUN pip3 install tensorflow==1.12.0 tensorboard==1.12.2 scikit-image

COPY start-cloudburst.sh /start-cloudburst.sh

RUN pip3 install pandas s3fs 

RUN touch a
RUN pip3 install --upgrade git+https://github.com/devin-petersohn/modin@engines/cloudburst_init

CMD bash start-cloudburst.sh
