# Droplet

[![Build Status](https://travis-ci.com/hydro-project/droplet.svg?branch=master)](https://travis-ci.com/hydro-project/droplet)
[![codecov](https://codecov.io/gh/hydro-project/droplet/branch/master/graph/badge.svg)](https://codecov.io/gh/hydro-project/droplet)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Droplet is a low-latency, stateful serverless programming framework built on top of the [Anna KVS](https://github.com/hydro-project/anna). Droplet enables users to execute compositions of functions at low latency, and the system builds on top of Anna in order to enable stateful computation. Droplet is co-deployed with the [Anna caching system](https://github.com/hydro-project/anna-cache) to achieve low-latency access to shared state, and the system relies on Anna's lattice data structures to resolve conflicting updates to shared state.

## Getting Started

You can install Droplet's dependencies with `pip` and use the bash scripts included in this repository to run the system locally. You can find the Droplet client in `droplet/client/client.py`. We are working on adding comprehensive documentation for the client interface, which will be available soon. An example interaction is modeled below.

```bash
$ pip install -r requirements.txt
$ ./scripts/start-droplet-local.sh
...
$ ./scripts/stop-droplet-local.sh
```

The `DropletConnection` is the main client interface; when running in local mode, all interaction between the client and server happens on `localhost`. Users can register functions and execute them. The executions return `DropletFuture`s, which can be retrieved asynchronously via the `get` method. Users can also register DAGs (directed, acylic graphs) of functions, where results from one function will be passed to downstream functions. 

```python
>>> from droplet.client.client import DropletConnection
>>> local_cloud = DropletConnection('127.0.0.1', '127.0.0.1', local=True)
>>> cloud_sq = local_cloud.register(lambda _, x: x * x, 'square')
>>> cloud_sq(2).get
4
>>> local_cloud.register_dag('dag', ['square'], square)
>>> local_cloud.call_dag('dag', { 'square': [2] }).get()
4
```

To run Anna and Droplet in cluster mode, you will need to use the cluster management setup, which can be found in the [hydro-project/cluster](https://github.com/hydro-project/cluster) repo. Instructions on how to use the cluster management tools can be found in that repo.

## License

The Hydro Project is licensed under the [Apache v2 License](LICENSE).
