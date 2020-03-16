# Executing Functions in Cloudburst

You will either need to run Cloudburst in [local mode](local-mode.md) or run a Hydro cluster. You can find instructions for running a Hydro cluster in the `hydro-project/cluster` repo, [here](https://github.com/hydro-project/cluster/blob/master/docs/getting-started-aws.md). Once you have either of these modes set up, you are ready to run functions in Cloudburst.

First, we'll create two new functions:

```python3
>>> local = True # or False if you are running against a HydroCluster
>>> elb_address = '127.0.0.1 ' # or the address of the ELB returned by the 
>>> from cloudburst.client.client import CloudburstConnection
>>> cloudburst = CloudburstConnection(AWS_FUNCTION_ELB, MY_IP, local=local)
>>> incr = lambda _, a: a + 1
>>> cloud_incr = cloudburst.register(incr, 'incr')
>>> cloud_incr(1).get()
2
>>> square = lambda _, a: a * a
>>> cloud_square = cloudburst.register(square, 'square')
>>> cloud_square(2).get()
4
```

Note that every function takes a first argument that is the [Cloudburst User Library](#CloudburstUserLibrary). We ignore that variable in these functions because we do not need it; the API is fully documented below.

Now we'll chain those functions together and execute them at once:

```python3
# Create a DAG with two functions, incr and square, where incr comes before square.
>>> cloudburst.register_dag('test_dag', ['incr', 'square'], [('incr', 'square')])
True # returns False if registration fails, e.g., if one of the referenced functions does not exist
>>> cloudburst.call_dag('test_dag', { 'incr': 1 }).get()
4
```

* All calls to functions and DAGs are by default asynchronous. Results are stored in the key-value store, and object IDs are returned. DAG calls can optionally specify synchronous calls by setting the `direct_response` argument to `True`.
* DAGs can have arbitrary branches and connections and have multiple sources, but there must be only one sink function in the DAG. The result of this sink function is what is returned to the caller.
* For those familiar with the Anna KVS, all use of lattices is abstracted away from the Cloudburst user. The serialization and deserialization is done automatically by the runtime, and only Python values are passed into and out of all API functions.

## Registering and Executing Classes

Some functions require preinitialization that can be potentially expensive (e.g., loading a machine learning model).
Instead of repeating this expensive initialization process on every request, users can also register a Python class with the runtime instead of a function.
When this class is initialized as part of a DAG<sup>1</sup>, the initialized state will persist until the resources are deallocated
When registering a class, the client expects a tuple with two arguments: The first is the class itself, and the second is the set of initialization arguments.
The class itself must have a `run` method, which will be invoked for each request.
The example below shows the expected structure.

```python3
class Expensive:
  def __init__(self, arg):
    expensive_operation(arg)

  def run(self, arg):
    # ... do some work ...
    return result 

cloud.register((Expensive, init_arg), 'expensive_class')
```

<sup>1</sup> Note that the benefits of using a class will not work with one-shot function execution, as the class will be reinitialized for each request.

## Cloudburst User Library

| API Name  | Functionality | 
|-----------|---------------|
| `get_object(key)`| Retrieves `key` from the KVS |
| `put_object(key, value)`| Puts `value` into the KVS at key `key` |
| `get_id()`| Returns the unique messaging identifier for this function |
| `send(id, msg)`| Sends message contents `msg` to the function at ID `id` |
| `recv()`| Receives any messages sent to this function |
