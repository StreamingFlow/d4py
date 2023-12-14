"""
Requirements:
    pip install pyzmq
Example:
    python -m dispel4py.new.processor zmq_multi dispel4py/examples/graph_testing/pipeline_test.py -i 10 -n 5
where
-n <INT> is the number of parallel processes
-i <INT> is the number of iterations
Other parameters:
-d <JSON> input data
-f <PATH> input data file
"""

import argparse
import copy
import multiprocessing
import uuid

import msgpack
import zmq
from zmq.devices.basedevice import ProcessDevice

from dispel4py.new import processor


def init_streamer(frontend_port, backend_port):
    streamerdevice = ProcessDevice(zmq.STREAMER, zmq.PULL, zmq.PUSH)
    streamerdevice.bind_in("tcp://127.0.0.1:%d" % frontend_port)
    streamerdevice.bind_out("tcp://127.0.0.1:%d" % backend_port)
    streamerdevice.setsockopt_in(zmq.IDENTITY, b"PULL")
    streamerdevice.setsockopt_out(zmq.IDENTITY, b"PUSH")
    streamerdevice.start()


def map_output(graph, node, output_name):
    result = set()
    pe_id = node.get_contained_object().id
    for edge in graph.edges(node, data=True):
        direction = edge[2]["DIRECTION"]
        source = direction[0]
        dest = direction[1]
        if source.id == pe_id and output_name == edge[2]["FROM_CONNECTION"]:
            dest_input = edge[2]["TO_CONNECTION"]
            result.add((dest.id, dest_input))
    return result


def process(workflow, inputs, args):
    size = args.num
    topic = ""
    frontend_port = 5559
    backend_port = 5560
    init_streamer(frontend_port, backend_port)
    print("Streamer initialised")
    producer = ZMQProducer(port=frontend_port, value_serializer=msgpack.packb)
    workers = {}
    for node in workflow.graph.nodes():
        pe = node.get_contained_object()
        for proc in range(size):
            cp = copy.deepcopy(workflow)
            cp.rank = proc
            workers[proc] = cp
        # add all the provided inputs to the queue
        provided_inputs = processor.get_inputs(pe, inputs)
        if provided_inputs is not None:
            if isinstance(provided_inputs, int):
                for i in range(provided_inputs):
                    print(f"writing initial input: {i}")
                    producer.send(topic, value=(pe.id, {}))
            else:
                for d in provided_inputs:
                    print(f"writing initial input: {d}")
                    producer.send(topic, value=(pe.id, d))

    jobs = []
    for proc, workflow in workers.items():
        p = multiprocessing.Process(
            target=_process_worker,
            args=(
                topic,
                proc,
                workflow,
            ),
        )
        jobs.append(p)

    print(f"Starting {len(workers)} workers communicating via topic {topic}")
    for j in jobs:
        j.start()
    for j in jobs:
        j.join()


class ZMQProducer:
    def __init__(self, port, value_serializer=msgpack.packb):
        self.port = port
        self.value_serializer = value_serializer
        context = zmq.Context()
        self.socket = context.socket(zmq.PUSH)
        self.socket.connect("tcp://127.0.0.1:%d" % port)

    def write(self, value):
        msg = msgpack.packb(value, use_bin_type=True)
        self.socket.send(msg)

    def send(self, topic, value):
        self.write(value=value)


class ZMQConsumer:
    def __init__(self, port, value_serializer=msgpack.packb):
        self.port = port
        self.value_serializer = value_serializer
        context = zmq.Context()
        self.socket = context.socket(zmq.PULL)
        self.socket.connect("tcp://127.0.0.1:%d" % port)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            message = self.socket.recv()
            # print ("Consumer got message: %s" % message)
            value = msgpack.unpackb(message, encoding="utf-8")
            # print ("Unpacked: %s" %  value)
        except IndexError:
            raise StopIteration from IndexError
        return value


class GenericWriter:
    def __init__(self, producer, pe_id, output_name):
        self.producer = producer
        self.pe_id = pe_id
        self.output_name = output_name

    def write(self, data):
        # FIXME:
        #  There are variables coming from nowhere.
        #  Clearly it's meant to be either an argument or attribute of the object
        #  However, I have no idea what it's supposed to be
        #  Issue: #15
        destinations = map_output(workflow.graph, self.pe_id, self.output_name)
        if not destinations:
            print(f"Output collected from {self.pe_id}: {data}")
        for dest_id, input_name in destinations:
            self.producer.send(topic, value=(dest_id, {input_name: data}))


def _process_worker(topic, workflow) -> None:
    frontend_port = 5559
    backend_port = 5560
    nodes = {node.get_contained_object().id: node for node in workflow.graph.nodes()}
    producer = ZMQProducer(port=frontend_port)
    consumer = ZMQConsumer(port=backend_port)
    pes = {
        node.get_contained_object().id: node.get_contained_object()
        for node in workflow.graph.nodes()
    }

    while True:
        try:
            value = next(consumer)
            pe_id, data = value
            print(f"{pe_id} receiver input: {data}")
            pe = pes[pe_id]
            for o in pe.outputconnections:
                pe.outputconnections[o]["writer"] = GenericWriter(producer, pe_id, o)
            output = pe.process(data)
            print(f"{pe.id} writing output: {output}")
            for output_name, output_value in output.items():
                destinations = map_output(workflow.graph, nodes[pe_id], output_name)
                if not destinations:
                    print(f"Output collected from {pe_id}: {output_value}")
                for dest_id, input_name in destinations:
                    producer.send(topic, value=(dest_id, {input_name: output_value}))
        except StopIteration:
            return
        except Exception as e:
            print(e)


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
    parser.add_argument("-t", "--topic", default=str(uuid.uuid4()), help="topic name")
    return parser.parse_args(args, namespace)
