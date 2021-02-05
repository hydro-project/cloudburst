# Cloudburst

[![Build Status](https://travis-ci.com/hydro-project/cloudburst.svg?branch=master)](https://travis-ci.com/hydro-project/cloudburst)
[![codecov](https://codecov.io/gh/hydro-project/cloudburst/branch/master/graph/badge.svg)](https://codecov.io/gh/hydro-project/cloudburst)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


Cloudburst is a low-latency, stateful serverless programming framework built on top of the [Anna KVS](https://github.com/hydro-project/anna). Cloudburst enables users to execute compositions of functions at low latency, and the system builds on top of Anna in order to enable stateful computation. Cloudburst is co-deployed with the [Anna caching system](https://github.com/hydro-project/anna-cache) to achieve low-latency access to shared state, and the system relies on Anna's lattice data structures to resolve conflicting updates to shared state.

### Experimental Branch

The `aft-support` branch has experimental support for Cloudburst with AFT transaction support. You can read more about the motivations and features for this integration in [Section 3.3 of this dissertation](https://www.vikrams.io/papers/thesis.pdf).

## Getting Started

You can install Cloudburst's dependencies with `pip` and use the bash scripts included in this repository to run the system locally. You can find the Cloudburst client in `cloudburst/client/client.py`. Full documentation on starting a cluster in local mode can be found [here](docs/local-mode.md); documentation for the Cloudburst client can be found [here](docs/function-execution.md). An example interaction is modeled below.

```bash
$ pip3 install -r requirements.txt
$ ./scripts/start-cloudburst-local.sh n n
...
$ ./scripts/stop-cloudburst-local.sh n
```

The `CloudburstConnection` is the main client interface; when running in local mode, all interaction between the client and server happens on `localhost`. Users can register functions and execute them. The executions return `CloudburstFuture`s, which can be retrieved asynchronously via the `get` method. Users can also register DAGs (directed, acylic graphs) of functions, where results from one function will be passed to downstream functions. 

```python
>>> from cloudburst.client.client import CloudburstConnection
>>> local_cloud = CloudburstConnection('127.0.0.1', '127.0.0.1', local=True)
>>> cloud_sq = local_cloud.register(lambda _, x: x * x, 'square')
>>> cloud_sq(2).get()
4
>>> local_cloud.register_dag('dag', ['square'], [])
>>> local_cloud.call_dag('dag', { 'square': [2] }).get()
4
```

To run Anna and Cloudburst in cluster mode, you will need to use the cluster management setup, which can be found in the [hydro-project/cluster](https://github.com/hydro-project/cluster) repo. Instructions on how to use the cluster management tools can be found in that repo.

## License

The Hydro Project is licensed under the [Apache v2 License](LICENSE).
