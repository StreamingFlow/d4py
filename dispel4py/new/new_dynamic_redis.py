# from internal_extinction.int_ext_graph import read, graph

import argparse
import copy
import json
import time
from multiprocessing import Process, Value

from dispel4py.core import WRITER
from dispel4py.new.dyn_redis_config import connect
from dispel4py.new.logger import logger
from dispel4py.new.processor import get_inputs

DISPEL4PY_REDIS_PREFIX = "DISPEL4PY_DYN"
STREAM_KEY = DISPEL4PY_REDIS_PREFIX + "_STREAM"
GROUP_NAME = DISPEL4PY_REDIS_PREFIX + "_GROUP"
FIELD_KEY = "KEY"


TIMEOUT_IN_SECONDS = 1
MAX_RETRIES = 2

REDIS_TIMEOUT = TIMEOUT_IN_SECONDS * 1000


def parse_args(args, namespace):
    parser = argparse.ArgumentParser(
        prog="dispel4py",
        description="Submit a dispel4py graph to dynamic auto redis processing",
    )
    parser.add_argument(
        "-ct",
        "--consumer-timeout",
        help="stop consumers after timeout in ms",
        type=int,
    )
    parser.add_argument(
        "-n",
        "--num",
        metavar="num_processes",
        required=True,
        type=int,
        help="number of processes to run",
    )

    # parser.add_argument('-it', '--idle', metavar='idle_time',
    #                     type=int, help='idle time threshold for auto-scaling', default=100)

    return parser.parse_args(args, namespace)


class TimerDecorator:
    def __init__(self):
        # Creating a multiprocessing.Value of type double ('d') with initial value 0.0
        self.total_time = Value("d", 0.0)

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            duration = end_time - start_time

            with self.total_time.get_lock():
                self.total_time.value += duration

            # print(f"'{func.__name__}' took {duration:.5f} seconds to execute.")
            return result

        return wrapper


timer = TimerDecorator()


class RedisWriter:
    def __init__(self, redis, destinations):
        self.redis = redis
        self.destinations = destinations

    def write(self, data):
        if self.destinations:
            for dest_id, input_name in self.destinations:
                payload = {FIELD_KEY: json.dumps((dest_id, {input_name: data}))}
                self.redis.xadd(STREAM_KEY, payload, "*")


class DynamicWrapper:
    def __init__(self, pe, provided_inputs):
        self.pe = pe
        self.provided_inputs = provided_inputs
        self.pe.wrapper = self
        self.id = pe.id
        self.inputconnections = pe.inputconnections
        self.outputconnections = pe.outputconnections
        # self.name = pe.name
        # self.pe.log = types.MethodType(simpleLogger, self.pe)

        # logger.debug(f"self.pe = {self.pe!r}\n \
        #                 self.pe.wrapper = {self.pe.wrapper!r}\n \
        #                 self.provided_inputs = {self.provided_inputs!r}\n \
        #                 self.pe.outputconnections = {self.pe.outputconnections!r}")

    def process(self, data):
        # logger.debug(f"data = {data}")
        return self.pe.process(data)


class DynamicRedisWorker:
    def __init__(self, cp_graph, rank):
        self.graph = cp_graph
        self.rank = rank
        self.node_pe = {
            node.obj.id: {"node": node, "pe": node.obj} for node in self.graph.nodes()
        }

        # self.redis = connect(name="worker")

    @timer
    def process(self):
        redis = connect(name="worker")
        # timeout = INIT_TIMEOUT
        retries = 0

        # logger.debug(f"self.cp_graph = {self.graph!r}, redis = {redis!r}")

        while True:
            try:
                response = redis.xreadgroup(
                    GROUP_NAME,
                    f"worker_{self.rank}",
                    {STREAM_KEY: ">"},
                    count=1,
                    block=REDIS_TIMEOUT,
                )

                # posion pill need here

                if response:
                    enrty_id, pe_id, data = unpack_response(response)

                    pe = self.node_pe[pe_id]["pe"]
                    node = self.node_pe[pe_id]["node"]

                    for output_name in pe.outputconnections:
                        # logger.debug(f"output_name = {output_name!r}")

                        destinations = self._get_destination(node, output_name)
                        pe.outputconnections[output_name][WRITER] = RedisWriter(
                            redis,
                            destinations,
                        )

                    output = pe.process(data)

                    if output:
                        for output_name, output_value in output.items():
                            destinations = self._get_destination(node, output_name)
                            if destinations:
                                for dest_id, input_name in destinations:
                                    # logger.debug(f"dest_id = {dest_id!r}, input_name = {input_name!r}")
                                    payload = {
                                        FIELD_KEY: json.dumps(
                                            (dest_id, {input_name: output_value}),
                                        ),
                                    }
                                    redis.xadd(STREAM_KEY, payload, "*")

                    redis.xack(STREAM_KEY, GROUP_NAME, enrty_id)

                else:
                    if retries == MAX_RETRIES:
                        # logger.error(f"Empty queue, timeout = {REDIS_TIMEOUT * MAX_RETRIES}")
                        # logger.error(f"Worker {self.rank} has been idle for too long. Exiting...")
                        break

                    retries += 1
                    # timeout *= 2
                    continue

            except Exception as e:
                logger.error(f"Exception = {e!r}")

    def _get_destination(self, node, output_name):
        """
        Function to get the destinations of a certain node in the graph.

        Args:
            node: The node for which we want to find the destinations.
            output_name: The name of the output.

        Returns:
            A set of destinations for the node.
        """
        destinations = set()

        pe_id = node.obj.id

        for edge in self.graph.edges(node, data=True):
            direction = edge[2]["DIRECTION"]
            source, dest = direction

        # ensure it's the source and not the first PE
        if source.id == pe_id and output_name == edge[2]["FROM_CONNECTION"]:
            dest_input = edge[2]["TO_CONNECTION"]
            destinations.add((dest.id, dest_input))

        return destinations


def reset_redis(redis):
    if redis.exists(STREAM_KEY):
        redis.delete(STREAM_KEY)
    if redis.exists(GROUP_NAME):
        redis.xgroup_destroy(STREAM_KEY, GROUP_NAME)

    redis.xgroup_create(STREAM_KEY, GROUP_NAME, mkstream=True)


def unpack_response(response):  # -> (entry_id, pe_id, data)
    """
    response = [[key, [(entry_id, {FIELD_KEY: value})]]]
    """

    # logger.debug(f"response = {response}")

    key, msg = response[0]
    entry_id, payload = msg[0]
    value = json.loads(payload[FIELD_KEY])

    pe_id, data = value

    # # logger.debug(f"key = {key}")
    # # logger.debug(f"msg = {str(msg)}")
    # logger.debug(f"entry_id = {entry_id}")
    # # logger.debug(f"payload = {str(payload)}")
    # # logger.debug(f"value = {str(value)}")
    # logger.debug(f"pe_id = {pe_id}")
    # logger.debug(f"data = {data}")

    return entry_id, pe_id, data


def process(workflow, inputs=None, args=None):
    start_time = time.time()

    # logger.info(f"workflow = {workflow}, dir(workflow) = {dir(workflow)})")

    size = args.num - 1
    graph = workflow.graph

    redis = connect(name="master")

    reset_redis(redis)

    for node in graph.nodes():
        provided_inputs = get_inputs(node.obj, inputs)
        node.obj = DynamicWrapper(node.obj, provided_inputs)

        if provided_inputs:
            # logger.debug(f"provided_inputs = {provided_inputs}")

            if isinstance(provided_inputs, int):
                for _ in range(provided_inputs):
                    payload = {FIELD_KEY: json.dumps((node.obj.id, {}))}
                    redis.xadd(STREAM_KEY, payload, "*")
            else:
                for d in provided_inputs:
                    payload = {FIELD_KEY: json.dumps((node.obj.id, d))}
                    redis.xadd(STREAM_KEY, payload, "*")
                    # logger.debug(f"payload = {payload}")

    workers = []

    for rank in range(size):
        cp_graph = copy.deepcopy(graph)

        worker = DynamicRedisWorker(cp_graph, rank)
        proc = Process(target=worker.process)
        workers.append(proc)

    print("Starting %s workers communicating" % (len(workers)))

    for worker in workers:
        worker.start()

    for worker in workers:
        worker.join()

        # worker = DynamicWroker(queue, cp_graph, rank)
        # proc = multiprocessing.Process(target=worker.process)
        # workers.append(proc)

    print(f"NEW ELAPSED TIME: {(time.time()-start_time):.5f}")
    # print(f"NEW ELAPSED TIME Without TERMINATION: {(time.time()-start_time- TIMEOUT_IN_SECONDS * MAX_RETRIES):.5f}")

    print(f"NEW ELAPSED TOTAL CPU TIME: {timer.total_time.value:.5f}")
