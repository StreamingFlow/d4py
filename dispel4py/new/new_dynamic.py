import argparse
import copy

# import multiprocessing
import time
from multiprocessing import Manager, Process, Value
from queue import Empty

from dispel4py.core import WRITER

# from dispel4py.new import processor
from dispel4py.new.processor import get_inputs

TIMEOUT_IN_SECONDS = 1
MAX_RETRIES = 2

MULTI_TIMEOUT = TIMEOUT_IN_SECONDS


# from test_workflow import producer, graph
# from dispel4py.examples.internal_extinction.int_ext_graph import read, graph
# from internal_extinction.int_ext_graph import read, graph

from dispel4py.new.logger import logger


class TimerDecorator:
    def __init__(self):
        # Creating a multiprocessing.Value of type double ('d') with initial value 0.0
        self.total_time = Value("d", 0.0)
        # self.lock = multiprocessing.Lock()

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

        logger.debug(
            f"self.pe = {self.pe!r}\n \
                        self.pe.wrapper = {self.pe.wrapper!r}\n \
                        self.provided_inputs = {self.provided_inputs!r}\n \
                        self.pe.outputconnections = {self.pe.outputconnections!r}",
        )

    def process(self, data):
        # logger.debug(f"data = {data}")
        return self.pe.process(data)


class DynamicWroker:
    def __init__(self, queue, cp_graph, rank):
        self.graph = cp_graph
        self.rank = rank
        self.queue = queue
        self.node_pe = {
            node.obj.id: {"node": node, "pe": node.obj} for node in self.graph.nodes()
        }

    @timer
    def process(self):
        """
        Function to process a worker in the workflow.
        """

        retries = 0
        # timeout = INIT_TIMEOUT

        while True:
            try:
                # Initially, block until an item is available
                # logger.debug(f"rank = {self.rank}, {self.queue.empty()}")
                # value = self.queue.get(not self.queue.empty())
                value = self.queue.get(timeout=MULTI_TIMEOUT)

                if value == "STOP":
                    # self.queue.put('STOP')
                    break

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

                # Reset retries after successful process
                retries = 0
                # timeout = INIT_TIMEOUT

            except Empty:
                # Implement the retry mechanism here
                # timeout += INIT_TIMEOUT
                retries += 1

                if retries == MAX_RETRIES:
                    self.queue.put("STOP")

                    # logger.error(f"Empty queue, timeout = {MULTI_TIMEOUT * MAX_RETRIES}")
                    break

            # except Empty:
            #     # pass
            #     # logger.info(f"Empty queue")
            #     break

            except Exception as e:
                logger.error(f"Exception = {e}")

        # self.queue.put('STOP')

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


def process(workflow, inputs=None, args=None):
    # logger.info(f"workflow = {workflow}, dir(workflow) = {dir(workflow)})")

    start_time = time.time()

    size = args.num - 1
    graph = workflow.graph

    queue = Manager().Queue()

    # pes = {node.getContainedObject().id: node.getContainedObject() for node in workflow.graph.nodes()}
    # nodes = {node.getContainedObject().id: node for node in workflow.graph.nodes()}

    for node in graph.nodes():
        provided_inputs = get_inputs(node.obj, inputs)
        # logger.debug(f"dir(node.obj) = {dir(node.obj)}")
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

    workers = []

    for rank in range(size):
        cp_graph = copy.deepcopy(graph)

        worker = DynamicWroker(queue, cp_graph, rank)
        proc = Process(target=worker.process)
        workers.append(proc)

    print("Starting %s workers communicating" % (len(workers)))

    for worker in workers:
        worker.start()

    for worker in workers:
        worker.join()

    # print ("NEW ELAPSED TIME: "+str(time.time()-start_time))
    # print ("NEW ELAPSED TIME Without Termination: "+str(time.time()-start_time- MULTI_TIMEOUT * MAX_RETRIES))
    # print(f"Total cpu time taken: {timer.total_time.value:.5f} seconds.")

    print(f"NEW ELAPSED TIME: {(time.time()-start_time):.5f}")
    # print(f"NEW ELAPSED TIME Without TERMINATION: {(time.time()-start_time- TIMEOUT_IN_SECONDS * MAX_RETRIES):.5f}")

    print(f"NEW ELAPSED TOTAL CPU TIME: {timer.total_time.value:.5f}")
