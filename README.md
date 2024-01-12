# stream-d4py

## New dispel4py (stream-d4py) streaming workflow repository 

dispel4py is a free and open-source Python library for describing abstract stream-based workflows for distributed data-intensive applications. It enables users to focus on their scientific methods, avoiding distracting details and retaining flexibility over the computing infrastructure they use.  It delivers mappings to diverse computing infrastructures, including cloud technologies, HPC architectures and  specialised data-intensive machines, to move seamlessly into production with large-scale data loads. The dispel4py system maps workflows dynamically onto multiple enactment systems, and supports parallel processing on distributed memory systems with MPI and shared memory systems with multiprocessing, without users having to modify their workflows.


## Dependencies

This version of dispel4py has been tested with Python *3.10*

For earlier versions of dispel4py compatible with Python <3.10 ( e.g *2.7.5*, *2.7.2*, *2.6.6* and Python *3.4.3*, *3.6*, *3.7*) we recommend to go [here](https://gitlab.com/project-dare/dispel4py).

The dependencies required for running dispel4py are listed in the requirements.txt file. However those are automatically installed running `pip install stream-py` or `python setup.py install` commands. Therefore there is not need to manually install those.

If using the MPI mapping, please install [mpi4py](http://mpi4py.scipy.org/)

## Installation

We recommend to install first a conda enviroment with python 3.10. Then, you can install dispel4py via pip or cloning this repo. See bellow:

### Via pip 
1. `conda create --name stream-d4py_env python=3.10`
2. `conda activate stream-d4py_env`
3. `conda install -c conda-forge mpi4py mpich` OR `pip install mpi4py` (Linux)
4. `pip install stream-d4py`

### Via cloning this repo

1. `conda create --name stream-d4py_env python=3.10`
2. `conda activate stream-d4py_env`
3. `https://github.com/StreamingFlow/stream-d4py.git`
4. `cd dispel4py`
5. `conda install -c conda-forge mpi4py mpich` OR `pip install mpi4py` (Linux)
6. `python setup.py install`


## Known Issues

1. Multiprocessing (multi) does not seem to work properly in MacOS (M1 chip).See bellow:


```
File "/Users/...../anaconda3/envs/.../lib/python3.10/multiprocessing/spawn.py", line 126, in _main
    self = reduction.pickle.load(from_parent)
AttributeError: 'TestProducer' object has no attribute 'simple_logger'
```

For those users, we do recommend to user our Docker file to create an image, and later a container.

2. You might have to use the following command to install mpi in your MacOS laptop:
```
conda install -c conda-forge mpi4py mpich
```
   In Linux enviroments to install mpi you can use:
```
pip install mpi4py
```

3. For the mpi mapping, we need to indicate **twice** the number of processes, using twice the -n flag -- one at te beginning and one at the end --:

```
mpiexec -n 10 dispel4py mpi dispel4py.examples.graph_testing.pipeline_test -i 20 -n 10
```

4. In some enviroments, you might need these flags for the mpi mapping: 

```
--allow-run-as-root --oversubscribe 
```

5. When running workflows with **mpi mapping**  you may encounter messages like `Read -1, expected 56295, errno = 1`. There's no need for concern; these messages are typical and do not indicate a problem. Rest assured, your workflow is still running as expected.


## Docker

The Dockerfile in the dispel4py root directory installs dispel4py and mpi4py.

```
docker build . -t mydispel4py
```

Note: If you want to re-built an image without cache, use this flag: `--no-cache`

Start a Docker container with the dispel4py image in interactive mode with a bash shell:

```
docker run -it mydispel4py /bin/bash
```

## Mappings

The mappings of dispel4py refer to the connections between the processing elements (PEs) in a dataflow graph. Dispel4py is a Python library used for specifying and executing data-intensive workflows. In a dataflow graph, each PE represents a processing step, and the mappings define how data flows between the PEs during execution. These mappings ensure that data is correctly routed and processed through the dataflow, enabling efficient and parallel execution of tasks. We currently support the following ones:

- **Sequential**
  - "simple": it executes dataflow graphs sequentially on a single process, suitable for small-scale data processing tasks. 
- **Parallel**:  
  -  **Fixed fixed workload distribution - support stateful and stateless PEs:**
    	- "mpi": it distributes dataflow graph computations across multiple nodes (distributed memory) using the **Message Passing Interface (MPI)**. 
    	- "multi": it runs multiple instances of a dataflow graph concurrently using **multiprocessing Python library**, offering parallel processing on a single machine. 
    	- "zmq_multi": it runs multiple instances of a dataflow graph concurrently using **ZMQ library**, offering parallel processing on a single machine.
        - "redis" : it runs multiple instances of a dataflow graph concurrently using **Redis library**. 
  - **Dynamic workfload distribution -  support only stateless PEs** 
    - "dyn_multi": it runs multiple instances of a dataflow graph concurrently using **multiprocessing Python library**. Worload assigned dynamically (but no autoscaling). 
    - "dyn_auto_multi": same as above, but allows autoscaling. We can indicate the number of threads to use.
    - "dyn_redis": it runs multiple instances of a dataflow graph concurrently using **Redis library**. Workload assigned dynamically (but no autocasling). 
    - "dyn_auto_redis": same as above, but allows autoscaling. We can indicate the number of threads to use.
  - **Hybrid workload distribution - supports stateful and stateless PEs**
    - "hybrid_redis": it runs multiple instances of a dataflow graph concurrently using **Redis library**. Hybrid approach for workloads: Stafeless PEs assigned dynamically, while Stateful PEs are assigned from the begining.
  

## Examples

[This directory](https://github.com/StreamingFlow/d4py/tree/main/dispel4py/examples/graph_testing) contains a collection of dispel4py workflows used for testing and validating the functionalities and behavior of dataflow graphs. These workflows are primarily used for testing purposes and ensure that the different mappings (e.g., simple, MPI, Storm) and various features of dispel4py work as expected. They help in verifying the correctness and efficiency of dataflow graphs during development and maintenance of the dispel4py library

For more complex "real-world" examples for specific scientific domains, such as seismology, please go to [this repository](https://github.com/StreamingFlow/d4py_workflows)


### Pipeline_test

For each mapping we have always two options: either to use the `dispel4py` command ; or use `python -m` command. 

##### Simple mapping
```shell
dispel4py simple dispel4py.examples.graph_testing.pipeline_test -i 10 
```
OR 

```shell
python -m dispel4py.new.processor dispel4py.new.simple_process dispel4py.examples.graph_testing.pipeline_test -i 10
```

##### Multi mapping
```shell
dispel4py multi dispel4py.examples.graph_testing.pipeline_test -i 10 -n 6
```
OR 

```shell
python -m dispel4py.new.processor dispel4py.new.multi_process dispel4py.examples.graph_testing.pipeline_test -i 10 -n 6
```

##### MPI mapping
```shell
mpiexec -n 10 dispel4py mpi dispel4py.examples.graph_testing.pipeline_test -i 20 -n 10
```
OR 

```shell
mpiexec -n 10 python -m dispel4py.new.processor dispel4py.new.mpi_process dispel4py.examples.graph_testing.pipeline_test -i 20 -n 10
```

Remember that you might to use the `--allow-run-as-root --oversubscribe` flags for some enviroments:

```shell
mpiexec -n 10 --allow-run-as-root --oversubscribe  dispel4py mpi dispel4py.examples.graph_testing.pipeline_test -i 20 -n 10
```
#### Redis mapping

Note: In another tab, we need to have REDIS working in background:

In Tab 1: 

```shell
redis-server
```

In Tab 2: 

```shell
dispel4py redis dispel4py.examples.graph_testing.word_count -ri localhost -n 4 -i 10
```

OR

```shell
python -m dispel4py.new.processor dispel4py.new.dynamic_redis dispel4py.examples.graph_testing.word_count -ri localhost -n 4 -i 10
```

**Note**: You can have just one tab, running redis-server in the background: `redis-server &` 


#### Hibrid Redis  with two stateful workflows 

*Note 1*: This mapping also uses multiprocessing (appart from redis) - therefore you might have issues with MacOS (M1 chip). For this mapping, we recommed to use our Docker container. 

*Note 2*: You need to have redis-server running. Either in a separete tab, or in the same tab, but in background. 

###### Split and Merge workflow

```shell
python -m dispel4py.new.processor hybrid_redis dispel4py.examples.graph_testing.split_merge -i 100 -n 10
```
OR

```shell
dispel4py hybrid_redis dispel4py.examples.graph_testing.split_merge -i 100 -n 10
```

###### All to one stateful workflow
```shell
python -m dispel4py.new.processor hybrid_redis dispel4py.examples.graph_testing.grouping_alltoone_stateful -i 100 -n 10
```
OR
```shell
dispel4py hybrid_redis dispel4py.examples.graph_testing.grouping_alltoone_stateful -i 100 -n 10
```

## Google Colab Testing

Notebook for [testing dispel4py (stream-d4py) in Google Col](https://colab.research.google.com/drive/1rSkwBgu42YG3o2AAHweVkJPuqVZF62te?usp=sharing)

