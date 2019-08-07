# Running Droplet in Local Mode

In order to run Droplet, whether in local mode or in [cluster mode](https://github.com/hydro-project/cluster/blob/master/docs/getting-started-aws.md), the Droplet runtime requires a copy of the Anna KVS to be running. Both Droplet and Anna can be run with limited capabilities on a single machine. You can find documentation on running Anna in local mode [here](https://github.com/hydro-project/anna/blob/master/docs/local-mode.md). The rest of this document assumes that Anna is already running in local mode on your machine. 

## Prerequisites

Droplet currently only supports Python3. To install Python dependencies, simply run `pip install -r requiments.txt` from the Droplet source directory.

Before running Droplet, we need to compile its Protobufs locally to generate the Python dependency files. `scripts/build.sh` automatically does this for you and installs them in the correct location, but it requires having the `protoc` tool installed. If you need to remove the locally compiled protobufs, you can run `bash scripts/clean.sh`.

Finally, Droplet requires access to the Anna Python client, which is in the Anna KVS repository. A default script to clone the Anna repository and install the client (the client is not currently `pip`-installable) can be found in `scripts/install-anna.sh`. You can customize the installation location by adding the `--prefix` flag to the `setup.py` command.

## Running Droplet

Once all the protobufs have been compiled, `scripts/start-droplet-local.sh` will start a local Droplet server. You can stop these processes with `scripts/stop-droplet-local.sh`. For more information on how to interact with Droplet once it is running, see the [function execution docs](docs/function-execution.md).