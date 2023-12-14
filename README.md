# d4py
New dispel4py streaming workflow repository 

dispel4py is a free and open-source Python library for describing abstract stream-based workflows for distributed data-intensive applications. It enables users to focus on their scientific methods, avoiding distracting details and retaining flexibility over the computing infrastructure they use.  It delivers mappings to diverse computing infrastructures, including cloud technologies, HPC architectures and  specialised data-intensive machines, to move seamlessly into production with large-scale data loads. The dispel4py system maps workflows dynamically onto multiple enactment systems, and supports parallel processing on distributed memory systems with MPI and shared memory systems with multiprocessing, without users having to modify their workflows.


## Dependencies

dispel4py has been tested with Python *2.7.6*, *2.7.5*, *2.7.2*, *2.6.6* and Python *3.4.3*, *3.6*, *3.7*, *3.10*.

The dependencies required for running dispel4py are listed in the requirements.txt file. To install them, please run:
```shell
pip install -r requirements.txt
```

You will also need the following installed on your system:

- If using the MPI mapping, please install [mpi4py](http://mpi4py.scipy.org/)
- If using the Redis mapping, please install [redis](https://redis.io/download/)



## Installation

In order to install dispel4py on your system:

- Clone the git repository
- Make sure that `redis` and the `mpi4py` Python package are installed on your system
- It is optional but recommended to create a virtual environment for dispel4py. Please refer to instructions bellow for setting it up with Conda.
- Install the requirements by running: `pip install -r requirements.txt`
- Run the dispel4py setup script: `python setup.py install`
- Run dispel4py using one of the following commands:
  - `dispel4py <mapping name> <workflow file> <args>`, OR
  - `python -m dispel4py.new.processor <mapping module> <workflow module> <args>`
- See "Examples" section bellow for more details

### Conda Environment

For installing for development with a conda environment, please run the following commands in your terminal.

1. `conda create --name py310 python=3.10`
2. `conda activate py310`
4. `https://github.com/StreamingFlow/d4py.git`
5. `cd dispel4py`
6. `pip install -r requirements.txt`
7. `conda install -c conda-forge mpi4py mpich`
8. `conda install -c conda-forge mpi4py mpich`
9. `python setup.py install`


## Known Issues

Multiprocessing (multi) does not seem to work properly in MacOS (M1 chip). For those users, we do recommend to user our Docker container. See bellow:

## Docker

The Dockerfile in the dispel4py root directory installs dispel4py and OpenMPI.

```
docker build . -t mydispel4py
```

Start a Docker container with the dispel4py image in interactive mode with a bash shell:

```
docker run -it dare-dispel4py /bin/bash
```


## Examples

Some simple examples, intended for testing, are included in this repository.
For more complex "real-world" examples for specific scientific domains, such as seismology, please see:
https://github.com/rosafilgueira/dispel4py_workflows


### Pipeline_test

##### Simple mapping
```shell
dispel4py simple dispel4py.examples.graph_testing.pipeline_test -i 10 
```

```shell
python -m dispel4py.new.processor dispel4py.new.simple_process dispel4py.examples.graph_testing.pipeline_test -i 10
```

##### Multi mapping
```shell
dispel4py multi dispel4py.examples.graph_testing.pipeline_test -i 10 -n 6
```
```shell
python -m dispel4py.new.processor dispel4py.new.multi_process dispel4py.examples.graph_testing.pipeline_test -i 10 -n 6
```

##### MPI mapping
```shell
mpiexec -n 10 dispel4py mpi dispel4py.examples.graph_testing.pipeline_test -i 20 -n 10
```

```shell
mpiexec -n 10 python -m dispel4py.new.processor dispel4py.new.mpi_process dispel4py.examples.graph_testing.pipeline_test -i 20 -n 10
```


#### Redis mapping

RDD:
```shell
python -m dispel4py.new.processor dispel4py.new.dynamic_redis dispel4py.examples.graph_testing.word_count -ri localhost -n 4 -i 10
```

Note: In another tab, we need to have REDIS working in background:
```shell
redis-server
```

## Google Colab Testing

Notebook for [testing_dispel4py2.0 in Google Col](https://colab.research.google.com/drive/1rSkwBgu42YG3o2AAHweVkJPuqVZF62te?usp=sharing)


