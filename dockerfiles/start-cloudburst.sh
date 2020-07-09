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

IP=`ifconfig eth0 | grep 'inet' | grep -v inet6 | sed -e 's/^[ \t]*//' | cut -d' ' -f2`

# A helper function that takes a space separated list and generates a string
# that parses as a YAML list.
gen_yml_list() {
  IFS=' ' read -r -a ARR <<< $1
  RESULT=""

  for IP in "${ARR[@]}"; do
    RESULT=$"$RESULT        - $IP\n"
  done

  echo -e "$RESULT"
}


# Download latest version of the code from relevant repository & branch -- if
# none are specified, we use hydro-project/cloudburst by default. Install the KVS
# client from the Anna project.
cd $HYDRO_HOME/anna
git remote remove origin
git remote add origin https://github.com/$ANNA_REPO_ORG/anna
while !(git fetch -p origin); do
   echo "git fetch failed, retrying..."
done

git checkout -b brnch origin/$ANNA_REPO_BRANCH
git submodule sync
git submodule update

cd client/python
python3.6 setup.py install

cd $HYDRO_HOME/cloudburst
if [[ -z "$REPO_ORG" ]]; then
  REPO_ORG="hydro-project"
fi

if [[ -z "$REPO_BRANCH" ]]; then
  REPO_BRANCH="master"
fi

git remote remove origin
git remote add origin https://github.com/$REPO_ORG/cloudburst
while !(git fetch -p origin); do
   echo "git fetch failed, retrying..."
done

git checkout -b brnch origin/$REPO_BRANCH
git submodule sync
git submodule update

# Compile protobufs and run other installation procedures before starting.
./scripts/build.sh

touch conf/cloudburst-config.yml
echo "ip: $IP" >> conf/cloudburst-config.yml
echo "mgmt_ip: $MGMT_IP" >> conf/cloudburst-config.yml

# Add the current directory to the PYTHONPATH in order to resolve imports
# correctly.
export PYTHONPATH=$PYTHONPATH:$(pwd)

if [[ "$ROLE" = "executor" ]]; then
  echo "executor:" >> conf/cloudburst-config.yml
  echo "    thread_id: $THREAD_ID" >> conf/cloudburst-config.yml
  LST=$(gen_yml_list "$SCHED_IPS")

  echo "    scheduler_ips:" >> conf/cloudburst-config.yml
  echo "$LST" >> conf/cloudburst-config.yml

  while true; do
    python3.6 cloudburst/server/executor/server.py

    if [[ "$?" = "1" ]]; then
      exit 1
    fi
  done
elif [[ "$ROLE" = "scheduler" ]]; then
  echo "scheduler:" >> conf/cloudburst-config.yml
  echo "    routing_address: $ROUTE_ADDR" >> conf/cloudburst-config.yml
  echo "    policy: $POLICY" >> conf/cloudburst-config.yml

  python3.6 cloudburst/server/scheduler/server.py
elif [[ "$ROLE" = "benchmark" ]]; then
  echo "benchmark:" >> conf/cloudburst-config.yml
  echo "    cloudburst_address: $FUNCTION_ADDR" >> conf/cloudburst-config.yml
  echo "    thread_id: $THREAD_ID" >> conf/cloudburst-config.yml

  python3.6 cloudburst/server/benchmarks/server.py
fi

