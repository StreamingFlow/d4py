import argparse
import copy
import time
from multiprocessing import (
    Condition,
    Manager,
    Pool,
    Value,
)
from queue import Empty

from dispel4py.core import WRITER

# from test_workflow import producer, graph
# from dispel4py.examples.internal_extinction.int_ext_graph import read, graph
# from internal_extinction.int_ext_graph import read, graph
from dispel4py.new.logger import logger

# from dispel4py.new import processor
from dispel4py.new.processor import get_inputs

TIMEOUT_IN_SECONDS = 1
MAX_RETRIES = 2

MULTI_TIMEOUT = TIMEOUT_IN_SECONDS


CPU_TOTAL_TIME = Value("d", 0.0)


def parse_args(args, namespace):
    parser = argparse.ArgumentParser(
        prog="dispel4py",
        description="Submit a dispel4py graph to zeromq multi processing",
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

    parser.add_argument(
        "-thr",
        "--queue_threshold",
        metavar="queue_threshold",
        type=int,
        help="queue size threshold for auto-scaling",
        default=10,
    )

    return parser.parse_args(args, namespace)


class GenericWriter:
    def __init__(self, queue, destinations):
        self.queue = queue
        self.destinations = destinations

    def write(self, data):
        if self.destinations:
            for dest_id, input_name in self.destinations:
                self.queue.put((dest_id, {input_name: data}))


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
        # self.test_string = "test string"

        # self.pe.log = lambda msg: DynamicWrapper.simpleLogger(self.pe, msg)

        # self.pe.log = simpleLogger(self.pe)

        # logger.debug(f"self.pe = {self.pe!r}\n \
        #                 self.pe.wrapper = {self.pe.wrapper!r}\n \
        #                 self.provided_inputs = {self.provided_inputs!r}\n \
        #                 self.pe.outputconnections = {self.pe.outputconnections!r}")

    def process(self, data):
        # logger.debug(f"data = {data}")
        return self.pe.process(data)

    # @staticmethod
    # def simpleLogger(instance, message):
    #     print(f"Instance ID: {instance.id}, Message: {message}")


class DynamicWroker:
    def __init__(self, queue, cp_graph, rank):
        self.graph = cp_graph
        self.rank = rank
        self.queue = queue
        self.node_pe = {
            node.obj.id: {"node": node, "pe": node.obj} for node in self.graph.nodes()
        }

    def process(self):
        """
        Function to process a worker in the workflow.
        """

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

        # logger.debug(f"rank = {self.rank}, destinations = {destinations}")
        return destinations


class AutoDynamicWroker(DynamicWroker):
    def __init__(self, queue, cp_graph, rank):
        super().__init__(queue, cp_graph, rank)
        # logger.info(f"self.rank = {self.rank}")

    def process(self):
        start_time = time.time()

        retries = 0
        while retries < MAX_RETRIES:
            try:
                # Initially, block until an item is available
                value = self.queue.get(timeout=MULTI_TIMEOUT)

                # if value == 'STOP':
                #     # self.queue.put('STOP')
                #     break

                # logger.debug(f"here rank = {self.rank}, value = {value}")

                pe_id, data = value
                pe = self.node_pe[pe_id]["pe"]
                node = self.node_pe[pe_id]["node"]

                # logger.debug(f"rank = {self.rank}, pe_id = {pe_id}, pe = {pe}, node = {node}")

                for output_name in pe.outputconnections:
                    destinations = self._get_destination(node, output_name)
                    pe.outputconnections[output_name][WRITER] = GenericWriter(
                        self.queue,
                        destinations,
                    )

                # logger.debug(f"outputconnections = {pe.outputconnections}")

                output = pe.process(data)
                # logger.debug(f"rank = {self.rank}, output = {output}")
                if output:
                    for output_name, output_value in output.items():
                        destinations = self._get_destination(node, output_name)
                        if destinations:
                            for dest_id, input_name in destinations:
                                self.queue.put((dest_id, {input_name: output_value}))

                # If everything goes smoothly, break out of the loop
                break

            except Empty:
                retries += 1
                if retries == MAX_RETRIES:
                    # self.queue.put('STOP')
                    # logger.error(f"Here lol Empty queue, timeout = {MULTI_TIMEOUT * MAX_RETRIES}")

                    break

            except Exception as e:
                logger.error(f"Exception = {e}")

                break

        duration = time.time() - start_time

        with CPU_TOTAL_TIME.get_lock():
            CPU_TOTAL_TIME.value += duration

        # logger.info(f"rank = {self.rank}, Done processing")


class AutoScaler:
    def __init__(self, queue, max_pool_size, initial_actice_size, queue_threshold):
        self.max_pool_size = max_pool_size
        self.pool = Pool(processes=self.max_pool_size)

        self.queue_threshold = queue_threshold

        self.queue = queue
        #  synchronization primitive allow for further extedning multiple auto scalers
        self.active_size = Value("i", initial_actice_size)
        self.active_count = Value("i", 0)
        # self.task_counter = Value('i', 0)
        self.condition = Condition()

        self.prev_queue_size = queue_threshold

    def shrink(self, size_to_shrink):
        with self.active_size.get_lock():
            self.active_size.value = max(1, self.active_size.value - size_to_shrink)

        # logger.info(f"Shrink: active size = {self.active_size.value}")

    def grow(self, size_to_grow):
        with self.active_size.get_lock():
            self.active_size.value = min(
                self.max_pool_size,
                self.active_size.value + size_to_grow,
            )

        # logger.info(f"Grow: active size = {self.active_size.value}")

    def process(self, graph):
        # logger.debug(f"dir(node.obj) = {dir(node.obj.pe)}")

        # for node in graph.nodes():

        #     logger.debug(f"dir(node.obj) = {type(node.obj.pe)}")
        #     provided_inputs = None
        #     node.obj = DynamicWrapper(node.obj, provided_inputs)

        results = []
        total_workers_spawned = 0
        while True:
            self.auto_scale()

            if self.queue.empty() and self.active_count.value == 0:
                # print("queue is empty")
                [result.get() for result in results]
                break

            else:
                cp_graph = copy.deepcopy(graph)
                worker = AutoDynamicWroker(self.queue, cp_graph, total_workers_spawned)
                total_workers_spawned += 1
                # self.start(self.queue.get())

                results.append(self.start(worker.process, args=[]))

                # logger.debug(f"here1")
                # self.start(worker.process, args=[]).get()

                # logger.debug(f"queue.size = {self.queue.qsize()}, self.active_count.value = {self.active_count.value}")

                # break

            if self.active_count.value == self.active_size.value:
                [result.get() for result in results]
                results = []

    def auto_scale(self):
        curr_queue_size = self.queue.qsize()

        if curr_queue_size > self.queue_threshold:
            if curr_queue_size >= self.prev_queue_size:
                self.grow(1)

            else:
                self.shrink(1)
        else:
            self.shrink(1)

        self.prev_queue_size = curr_queue_size

        # print(f"MONITOR: ACTIVE SIZE = {self.active_size.value} QUEUE_SIZE = {curr_queue_size}")

    # def auto_scale(self):

    #     if self.queue.qsize() > self.queue_threshold:
    #         # logger.info(f"queue size = {self.queue.qsize()}, active size = {self.active_size.value}")
    #         self.grow(1)

    #     elif self.queue.qsize() < self.queue_threshold and self.active_count.value > 0:
    #         # logger.info(f"queue size = {self.queue.qsize()}, active size = {self.active_size.value}")
    #         self.shrink(1)

    def start(self, task_func, args=None):
        with self.condition:
            while self.active_count.value >= self.active_size.value:
                self.condition.wait()

            with self.active_count.get_lock():
                self.active_count.value += 1

        # logger.debug(f"Start: active count = {self.active_count.value}")
        return self.pool.apply_async(task_func, args=args, callback=self.done)

    def done(self, result):
        with self.condition:
            self.active_count.value -= 1
            self.condition.notify_all()

        # logger.debug(f"Done: active count = {self.active_count.value}")


# def test_process(workflow, inputs=None, args=None):
def process(workflow, inputs=None, args=None):
    start_time = time.time()

    # logger.debug(f"workflow = {workflow}, dir(workflow) = {dir(workflow)})")
    queue = Manager().Queue()

    graph = workflow.graph

    size = args.num - 1
    queue_threshold = args.queue_threshold

    for node in graph.nodes():
        provided_inputs = get_inputs(node.obj, inputs)

        node.obj = DynamicWrapper(node.obj, provided_inputs)
        # logger.debug(f"dir(node.obj) = {dir(node.obj.pe)}")
        if provided_inputs:
            # logger.debug(f"provided_inputs = {provided_inputs}")

            if isinstance(provided_inputs, int):
                for _i in range(provided_inputs):
                    queue.put((node.obj.id, {}))
            else:
                for d in provided_inputs:
                    queue.put((node.obj.id, d))

    initial_size = int(size / 2) + 1
    auto_scaler = AutoScaler(queue, size, initial_size, queue_threshold)

    auto_scaler.process(graph)

    print(f"NEW ELAPSED TIME: {(time.time()-start_time):.5f}")
    # print(f"NEW ELAPSED TIME Without TERMINATION: {(time.time()-start_time- TIMEOUT_IN_SECONDS * MAX_RETRIES):.5f}")
    print(f"NEW ELAPSED TOTAL CPU TIME: {CPU_TOTAL_TIME.value:.5f}")
