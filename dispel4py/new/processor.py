# Copyright (c) The University of Edinburgh 2014-2015
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Enactment of dispel4py graphs.
This module contains methods that are used by different mappings.

From the commandline, run the following command::

    dispel4py <mapping> <module>  [-h] [-a attribute] [-f inputfile] \
                                  [-i iterations] \
                                  [--provenance-config [provenance-config-path]] \
                                  [...]

with parameters

:mapping:   target mapping
:module:    module that creates a Dispel4Py graph
:-n num:    number of processes (required)
:-a attr:   name of the graph attribute within the module (optional)
:-f file:   file containing input data in JSON format (optional)
:-i iter:   number of iterations to compute (default is 1)
:--provenance-config:
            file containing the provenance configuration
:-h:        print this help page

Other parameters might be required by the target mapping, for example the
number of processes if running in a parallel environment.

"""
import argparse
import os
import os.path
import sys
import types
from abc import abstractmethod
from typing import Any, Tuple

from dispel4py.core import GROUPING
from dispel4py.new.mappings import config
from dispel4py.utils import make_hash

STATUS_ACTIVE = 10
STATUS_INACTIVE = 11
STATUS_TERMINATED = 12
# mapping for name to value
STATUS = {
    STATUS_ACTIVE: "ACTIVE",
    STATUS_INACTIVE: "INACTIVE",
    STATUS_TERMINATED: "TERMINATED",
}


def simple_logger(self, msg) -> None:
    try:
        print(f"{self.id} (rank {self.rank}): {msg}")
    except AttributeError:
        print(f"{self.id}: {msg}")


def get_inputs(pe, inputs) -> Any | None:  # ToDo proper typing
    try:
        return inputs[pe]
    except KeyError:
        pass

    try:
        return inputs[pe.name]
    except (KeyError, AttributeError):
        pass

    try:
        return inputs[pe.id]
    except (KeyError, AttributeError):
        pass

    # print(f"Could not find inputs in {inputs} for object {pe}")  ToDo optionally print (add a "verbose" flag)
    return None


class GenericWriter:
    def __init__(self, wrapper, name):
        self.wrapper = wrapper
        self.name = name

    def write(self, data):
        self.wrapper._write(self.name, data)


class GenericWrapper:
    def __init__(self, pe):
        self.provided_inputs = None  # ToDo: what's that? seems to never be set
        self._num_sources = 0
        self.pe = pe
        self.pe.wrapper = self
        self.targets = {}
        self._sources = {}

        for o in self.pe.outputconnections:
            self.pe.outputconnections[o]["writer"] = GenericWriter(self, o)

    @property
    def sources(self):
        return self._sources

    @sources.setter
    def sources(self, sources):
        # count and store number of inputs when setting the sources
        num_inputs = 0

        for i in sources.values():
            num_inputs += len(i)

        self._num_sources = num_inputs
        self._sources = sources

    def process(self) -> None:
        num_iterations = 0
        self.pe.preprocess()
        result = self._read()
        inputs, status = result

        while status != STATUS_TERMINATED:
            if inputs is not None:
                outputs = self.pe.process(inputs)
                num_iterations += 1
                if outputs is not None:
                    for key, value in outputs.items():
                        self._write(key, value)
            inputs, status = self._read()

        self.pe.postprocess()
        self._terminate()

        try:
            if num_iterations == 1:
                self.pe.log("Processed 1 iteration.")
            else:
                self.pe.log(f"Processed {num_iterations} iterations.")
        except Exception as e:
            print(f"Could not log: exception {e}")

    def _read(self):
        # check the provided inputs
        if self.provided_inputs is not None:
            if isinstance(self.provided_inputs, int) and self.provided_inputs > 0:
                self.provided_inputs -= 1
                return {}, STATUS_ACTIVE
            elif self.provided_inputs:
                return self.provided_inputs.pop(0), STATUS_ACTIVE
            else:
                return None, STATUS_TERMINATED
        return None

    @abstractmethod
    def _write(self, name, data):
        return None

    @abstractmethod
    def _terminate(self):
        return None


class ShuffleCommunication:
    def __init__(self, rank, sources, destinations):
        self.destinations = destinations
        self.currentIndex = (sources.index(rank) % len(self.destinations)) - 1
        self.name = None

    def get_destination(self, data):
        self.currentIndex = (self.currentIndex + 1) % len(self.destinations)
        return [self.destinations[self.currentIndex]]


class GroupByCommunication:
    def __init__(self, destinations, input_name, group_by):
        self.group_by = group_by
        self.destinations = destinations
        self.input_name = input_name
        self.name = group_by

    def get_destination(self, data):
        output = tuple([data[self.input_name][x] for x in self.group_by])
        dest_index = abs(make_hash(output)) % len(self.destinations)
        return [self.destinations[dest_index]]


class AllToOneCommunication:
    def __init__(self, destinations):
        self.destinations = destinations
        self.name = "global"

    def get_destination(self, data):
        return [self.destinations[0]]


class OneToAllCommunication:
    def __init__(self, destinations):
        self.destinations = destinations
        self.name = "all"

    def get_destination(self, data):
        return self.destinations


def _get_connected_inputs(node, graph):
    names = []

    for edge in graph.edges(node, data=True):
        direction = edge[2]["DIRECTION"]
        dest = direction[1]
        dest_input = edge[2]["TO_CONNECTION"]

        if dest == node.get_contained_object():
            names.append(dest_input)

    return names


def _get_num_processes(size, num_sources, num_processes, total_processes):
    if (num_processes > 1) or (
        num_processes == 0
    ):  # ToDo: Can num_processes be negative?
        return num_processes

    div = max(1, total_processes - num_sources)
    return int(num_processes * (size - num_sources) / div)


def _assign_processes(workflow, size) -> Tuple[bool, list[str], dict[str, range]]:
    graph = workflow.graph
    processes: dict[str, range] = {}
    success = True
    total_processes = 0
    num_sources = 0
    sources: list[str] = []

    for node in graph.nodes():
        pe = node.get_contained_object()
        if _get_connected_inputs(node, graph):
            total_processes = total_processes + pe.numprocesses
        else:
            sources.append(pe.id)
            total_processes += 1
            num_sources += 1

    if total_processes > size:
        success = False
        # we need at least one process for each node in the graph
        print(f"Graph is larger than job size: {total_processes} > {size}.")
    else:
        node_counter = 0
        for node in graph.nodes():
            pe = node.get_contained_object()

            num_processes = 1
            if not ((pe.id in sources) or (hasattr(pe, "single") and pe.single)):
                num_processes = _get_num_processes(
                    size,
                    num_sources,
                    pe.numprocesses,
                    total_processes,
                )

            processes[pe.id] = range(node_counter, node_counter + num_processes)
            node_counter = node_counter + num_processes

    return success, sources, processes


def _get_communication(rank, source_processes, dest, dest_input, dest_processes):
    communication = ShuffleCommunication(rank, source_processes, dest_processes)
    try:
        if GROUPING in dest.inputconnections[dest_input]:
            grouping_type = dest.inputconnections[dest_input][GROUPING]
            if isinstance(grouping_type, list):
                communication = GroupByCommunication(
                    dest_processes,
                    dest_input,
                    grouping_type,
                )
            elif grouping_type == "all":
                communication = OneToAllCommunication(dest_processes)
            elif grouping_type == "global":
                communication = AllToOneCommunication(dest_processes)
    except KeyError:
        print(f"No input '{dest_input}' defined for PE '{dest.id}'")
        raise
    return communication


def _create_connections(graph, node, processes):
    pe = node.get_contained_object()
    input_mappings = {i: {} for i in processes[pe.id]}
    output_mappings = {i: {} for i in processes[pe.id]}

    for edge in graph.edges(node, data=True):
        direction = edge[2]["DIRECTION"]
        source = direction[0]
        source_output = edge[2]["FROM_CONNECTION"]
        dest = direction[1]
        dest_processes = list(processes[dest.id])
        source_processes = list(processes[source.id])
        dest_input = edge[2]["TO_CONNECTION"]
        all_connections = edge[2]["ALL_CONNECTIONS"]

        if dest == pe:
            for i in processes[pe.id]:
                for _, dest_input in all_connections:
                    try:
                        input_mappings[i][dest_input] += source_processes
                    except KeyError:
                        input_mappings[i][dest_input] = source_processes

        if source == pe:
            for i in processes[pe.id]:
                for source_output, dest_input in all_connections:
                    communication = _get_communication(
                        i,
                        source_processes,
                        dest,
                        dest_input,
                        dest_processes,
                    )
                    try:
                        output_mappings[i][source_output].append(
                            (dest_input, communication),
                        )
                    except KeyError:
                        output_mappings[i][source_output] = [
                            (dest_input, communication),
                        ]

    return input_mappings, output_mappings


def _connect(workflow, processes) -> Tuple[dict, dict]:
    graph = workflow.graph
    output_mappings = {}
    input_mappings = {}

    for node in graph.nodes():
        inc, outc = _create_connections(graph, node, processes)
        input_mappings.update(inc)
        output_mappings.update(outc)

    return input_mappings, output_mappings


def assign_and_connect(workflow, size) -> Tuple[dict[Any, range], dict, dict] | None:
    success, sources, processes = _assign_processes(workflow, size)
    if success:
        input_mappings, output_mappings = _connect(workflow, processes)
        return processes, input_mappings, output_mappings
    return None


import copy

from dispel4py.workflow_graph import WorkflowGraph


def get_partitions(workflow):
    try:
        partitions = workflow.partitions
    except AttributeError:
        print("no predefined partitions")

        source_partition = []
        other_partition = []
        graph = workflow.graph

        for node in graph.nodes():
            pe = node.get_contained_object()
            if not _get_connected_inputs(node, graph):
                source_partition.append(pe)
            else:
                other_partition.append(pe)

        partitions = [source_partition, other_partition]
        workflow.partitions = partitions

    return partitions


def get_partitions_adv(workflow):
    try:
        partitions = workflow.partitions
    except AttributeError:
        print("no predefined partitions")

        source_partition = []
        other_partition = []
        graph = workflow.graph

        for node in list(graph.nodes()):
            pe = node.get_contained_object()
            if not _get_connected_inputs(node, graph):
                source_partition.append(pe)
            else:
                other_partition.append(pe)

        partitions = [source_partition, other_partition]
        workflow.partitions = partitions

    return partitions


def create_partitioned(workflow_all):
    processes_all, input_mappings_all, output_mappings_all = assign_and_connect(
        workflow_all,
        len(workflow_all.graph.nodes()),
    )
    proc_to_pe_all = {v[0]: k for k, v in processes_all.items()}
    partitions = get_partitions(workflow_all)
    external_connections = []
    pe_to_partition = {}
    partition_pes = []

    for i in range(len(partitions)):
        for pe in partitions[i]:
            pe_to_partition[pe.id] = i

    for index in range(len(partitions)):
        result_mappings: dict[str, Any] = {}
        part = partitions[index]
        partition_id = index
        component_ids = [pe.id for pe in part]
        workflow = copy.deepcopy(workflow_all)
        graph = workflow.graph

        for node in list(graph.nodes()):
            if node.get_contained_object().id not in component_ids:
                graph.remove_node(node)

        processes, input_mappings, _outputmappings = assign_and_connect(
            workflow,
            len(graph.nodes()),
        )
        proc_to_pe = {}

        for node in graph.nodes():
            pe = node.get_contained_object()
            proc_to_pe[processes[pe.id][0]] = pe

        for node in graph.nodes():
            pe = node.get_contained_object()
            pe.rank = index
            proc_all = processes_all[pe.id][0]
            for output_name in output_mappings_all[proc_all]:
                for dest_input, comm_all in output_mappings_all[proc_all][output_name]:
                    dest = proc_to_pe_all[comm_all.destinations[0]]
                    if dest not in processes:
                        # it's an external connection
                        external_connections.append(
                            (
                                comm_all,
                                partition_id,
                                pe.id,
                                output_name,
                                pe_to_partition[dest],
                                dest,
                                dest_input,
                            ),
                        )

                        if pe.id not in result_mappings:
                            result_mappings[pe.id] = []
                        result_mappings[pe.id].append(output_name)

        partition_pe = SimpleProcessingPE(input_mappings, _outputmappings, proc_to_pe)

        # use number of processes if specified in graph
        try:  # ToDo WorkflowGraph has no such attribute, so number of processes will always be one
            partition_pe.numprocesses = workflow_all.numprocesses[partition_id]
        except KeyError:
            pass
        except AttributeError:
            pass

        partition_pe.workflow = workflow
        partition_pe.partition_id = partition_id
        if result_mappings:
            partition_pe.result_mappings = result_mappings
        partition_pe.map_inputs = _map_inputs_to_pes
        partition_pe.map_outputs = _map_outputs_from_pes
        partition_pes.append(partition_pe)
    # print 'EXTERNAL CONNECTIONS : %s' % external_connections
    ubergraph = WorkflowGraph()
    ubergraph.pe_to_partition = pe_to_partition
    ubergraph.partition_pes = partition_pes
    # sort the external connections so that nodes are added in the same order
    # if doing this in multiple processes in parallel this is important
    external_connections.sort(key=lambda connections: len(connections))
    for connection in external_connections:
        (
            comm,
            source_partition,
            source_id,
            source_output,
            dest_partition,
            dest_id,
            dest_input,
        ) = connection

        partition_pes[source_partition]._add_output((source_id, source_output))
        partition_pes[dest_partition]._add_input(
            (dest_id, dest_input),
            grouping=comm.name,
        )
        ubergraph.connect(
            partition_pes[source_partition],
            (source_id, source_output),
            partition_pes[dest_partition],
            (dest_id, dest_input),
        )

    return ubergraph


def map_inputs_to_partitions(ubergraph, inputs):
    mapped_input = {}

    for pe in inputs:
        try:
            partition_id = ubergraph.pe_to_partition[pe]
            pe_id = pe
        except KeyError:
            try:
                partition_id = ubergraph.pe_to_partition[pe.id]
                pe_id = pe.id
            except Exception as exc:
                raise Exception(
                    f'Could not map input name "{pe}" to a PE.'
                    f"{exc.__class__.__name__}: {exc}",
                ) from exc

        mapped_pe = ubergraph.partition_pes[partition_id]
        try:
            mapped_pe_input = []
            for i in inputs[pe]:
                mapped_data = {(pe_id, name): [data] for name, data in i.items()}
                mapped_pe_input.append(mapped_data)
        except TypeError:
            mapped_pe_input = inputs[pe]
        mapped_input[mapped_pe] = mapped_pe_input

    return mapped_input


def _map_inputs_to_pes(data):
    result = {}
    for i in data:
        pe_id, input_name = i
        mapped_data = [{input_name: block} for block in data[i]]
        try:
            result[pe_id].update(mapped_data)
        except KeyError:
            result[pe_id] = mapped_data
    return result


def _map_outputs_from_pes(data):
    result = {}
    for pe_id in data:
        for i in data[pe_id]:
            result[(pe_id, i)] = data[pe_id][i]
    return result


def _no_map(data):
    return data


import contextlib

from dispel4py.core import GenericPE


def _is_root(node, workflow):
    result = True
    pe = node.get_contained_object()
    for edge in workflow.graph[node].values():
        if pe == edge["DIRECTION"][1]:
            result = False
            break
    return result


def _get_dependencies(proc, inputmappings):
    dep = []
    for sources in inputmappings[proc].values():
        for s in sources:
            sdep = _get_dependencies(s, inputmappings)
            for n in sdep:
                if n not in dep:
                    dep.append(n)
            if s not in dep:
                dep.append(s)
    return dep


def _order_by_dependency(inputmappings, outputmappings):
    ordered = []

    for proc in outputmappings:
        if not outputmappings[proc]:
            dep = _get_dependencies(proc, inputmappings)

            for n in ordered:
                try:
                    dep.remove(n)
                except ValueError:  # never mind if the element wasn't in the list
                    pass

            ordered += dep
            ordered.append(proc)

    return ordered


class SimpleProcessingPE(GenericPE):
    """
    A PE that processes a subgraph of PEs in sequence.
    """

    def __init__(self, input_mappings, output_mappings, proc_to_pe):
        GenericPE.__init__(self)
        self.ordered = _order_by_dependency(input_mappings, output_mappings)
        self.input_mappings = input_mappings
        self.output_mappings = output_mappings
        self.proc_to_pe = proc_to_pe
        self.result_mappings = None
        self.map_inputs = _no_map
        self.map_outputs = _no_map

    def _preprocess(self):
        for proc in self.ordered:
            pe = self.proc_to_pe[proc]

            with contextlib.suppress(AttributeError):
                pe.rank = self.rank

            pe.log = types.MethodType(simple_logger, pe)
            pe.preprocess()

    def _postprocess(self):
        all_inputs = {}
        results = {}
        for proc in self.ordered:
            pe = self.proc_to_pe[proc]
            pe.writer = SimpleWriter(
                self,
                pe,
                self.output_mappings[proc],
                self.result_mappings,
            )
            pe._write = types.MethodType(_simple_write, pe)
            # if there was data produced in postprocessing
            # then we need to process that data in the PEs downstream
            if proc in all_inputs:
                for data in all_inputs[proc]:
                    result = pe.process(data)
                    if result is not None:
                        for output_name in result:
                            pe.write(output_name, result[output_name])
            # once all the input data is processed this PE can finish
            pe.postprocess()
            # PE might write data during postprocessing
            for p, input_data in pe.writer.all_inputs.items():
                try:
                    all_inputs[p].extend(input_data)
                except KeyError:
                    all_inputs[p] = input_data

            if pe.writer.results:
                results[pe] = pe.writer.results
            pe.writer.all_inputs = {}
            pe.writer.results = {}
        results = self.map_outputs(results)
        for key, value in results.items():
            self._write(key, value)

    def _process(self, inputs):
        all_inputs = {}
        results = {}
        inputs = self.map_inputs(inputs)
        for proc in self.ordered:
            pe = self.proc_to_pe[proc]
            pe.writer = SimpleWriter(
                self,
                pe,
                self.output_mappings[proc],
                self.result_mappings,
            )
            pe._write = types.MethodType(_simple_write, pe)
            provided_inputs = get_inputs(pe, inputs)
            try:
                other_inputs = all_inputs[proc]
                try:
                    provided_inputs.append(other_inputs)
                except AttributeError:
                    provided_inputs = other_inputs
            except KeyError:
                pass

            if isinstance(provided_inputs, int):
                for _ in range(provided_inputs):
                    _process_data(pe, {})
            else:
                if provided_inputs is None:
                    provided_inputs = [{}] if not pe.inputconnections else []

                for data in provided_inputs:
                    _process_data(pe, data)

            for p, input_data in pe.writer.all_inputs.items():
                try:
                    all_inputs[p].extend(input_data)
                except (KeyError, AttributeError):
                    all_inputs[p] = input_data

            if pe.writer.results:
                results[pe.id] = pe.writer.results
            # discard data from the PE writer
            pe.writer.all_inputs = {}
            pe.writer.results = {}
        return self.map_outputs(results)


def _process_data(pe, data):
    result = pe.process(data)
    if result is not None:
        for output_name in result:
            pe.write(output_name, result[output_name])


def _simple_write(self, name, data):
    self.writer.write(name, data)


class SimpleWriter:
    def __init__(self, simple_pe, pe, output_mappings, result_mappings=None):
        self.simple_pe = simple_pe
        self.pe = pe
        self.output_mappings = output_mappings
        self.result_mappings = result_mappings
        self.all_inputs = {}
        self.results = {}

    def write(self, output_name, data):
        try:
            destinations = self.output_mappings[output_name]
            dest_data = data
            for input_name, comm in destinations:
                copy_data = len(destinations) > 1 or len(comm.destinations) > 1
                for p in comm.destinations:
                    if copy_data:
                        dest_data = copy.deepcopy(dest_data)
                    input_data = {input_name: dest_data}

                    if p not in self.all_inputs:
                        self.all_inputs[p] = []
                    self.all_inputs[p].append(input_data)

        except KeyError:
            # no destinations for this output
            # if there are no named result outputs
            # the data is added to the results of the PE
            if self.result_mappings is None:
                self.simple_pe.wrapper._write((self.pe.id, output_name), [data])

        # now check if the output is in the named results
        # (in case of a Tee) then data gets written to the PE results as well
        try:
            if output_name in self.result_mappings[self.pe.id]:
                self.simple_pe.wrapper._write((self.pe.id, output_name), [data])
        except Exception as e:
            pass


def create_arg_parser():  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Submit a dispel4py graph for processing.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("target", help="target execution platform")
    parser.add_argument(
        "module",
        help="module that creates a dispel4py graph (python module or file name)",
    )
    parser.add_argument(
        "-a",
        "--attr",
        metavar="attribute",
        help="name of graph variable in the module",
    )
    parser.add_argument(
        "-f",
        "--file",
        metavar="inputfile",
        help="file containing input dataset in JSON format \n(has priority over -d)",
    )
    parser.add_argument(
        "-d",
        "--data",
        metavar="inputdata",
        help="input dataset in JSON format",
    )
    parser.add_argument(
        "-i",
        "--iter",
        metavar="iterations",
        type=int,
        help="number of iterations",
        default=1,
    )
    parser.add_argument(
        "--provenance-config",
        dest="provenance",
        metavar="provenance-config-path",
        type=str,
        nargs="?",
        help="This argument is MANDATORY to process commandline provenance config!\n"
        'optionally specify a path to file with given config (JSON) or specify "inline".\n'
        "This option has priority over inline specified provenance configuration\n"
        'if its value is a valid provenance file. Specify "inline" as value if the\n'
        "provenance configuration is embedded in the workflow graph(=module argument).\n"
        'Attention: "s-prov:WFExecutionInputs" is deprecated. \n'
        '"--provenance --help" for help on additional options.',
    )
    return parser


def get_inputs_from_arguments(args):
    import json

    inputs = {}

    if args.file:
        if not os.path.exists(args.file):
            raise ValueError("File '%s' does not exist." % args.file)
        try:
            with open(args.file) as inputfile:
                inputs = json.loads(inputfile.read())
        except Exception as e:
            print(f"Error reading JSON file '{args.file}': {e}")
            raise
    elif args.data:
        inputs = json.loads(args.data)
    return inputs


def create_inputs(args, graph):
    inputs = {}

    inputs = get_inputs_from_arguments(args)

    if not inputs:
        if args.iter == 1:
            print("Processing 1 iteration.")
        else:
            print(f"Processing {args.iter} iterations.")
        for node in graph.graph.nodes():
            if _is_root(node, graph):
                inputs[node.get_contained_object()] = args.iter

    # map input names to ids if necessary
    for node in graph.graph.nodes():
        pe = node.get_contained_object()

        try:
            d = inputs.pop(pe)
            inputs[pe.id] = d
        except (KeyError, AttributeError):
            pass

        try:
            d = inputs.pop(pe.name)
            inputs[pe.id] = d
        except (KeyError, AttributeError):
            pass

    return inputs


def check_commandline_argument(argument):
    argument_present = False
    for arg in sys.argv:
        if argument == arg[: len(argument)]:
            argument_present = True
            break
    return argument_present


def load_graph_and_inputs(args):
    from dispel4py.provenance import CommandLineInputs
    from dispel4py.utils import load_graph

    # Checking if --provenance-config is part of arguments in commandline,
    # to set the flag to process all present commandline provenance config arguments.
    # So, in order to process commandline provenance, the user should give a --provenance-config
    # argument.
    # If the value of --provenance-config is not a file, it should be 'inline', to indicate the
    # configuration is in the graph/workflow and not as a separate file.
    if check_commandline_argument("--provenance-config"):
        print("DEBUG: User specified --provenance-config on cli")
        CommandLineInputs.provenanceCommandLineConfigPresent = True

    CommandLineInputs.inputs = get_inputs_from_arguments(args)
    CommandLineInputs.module = args.module
    graph = load_graph(args.module, args.attr)
    if graph is None:
        return None, None

    graph.flatten()
    inputs = create_inputs(args, graph)

    if CommandLineInputs.provenanceCommandLineConfigPresent:
        if (
            args.provenance
            and args.provenance != "inline"
            and not os.path.exists(args.provenance)
        ):
            print(f"Can't load provenance configuration {args.provenance}")
        elif args.provenance and (
            args.provenance == "inline" or os.path.exists(args.provenance)
        ):
            from dispel4py.provenance import (
                ProvenanceType,
                configure_prov_run,
                init_provenance_config,
            )

            print(
                "Reading provenance config from cli supplied file (could be inline if explicitly specified).",
            )
            prov_config, remaining = init_provenance_config(args)
            if not prov_config:
                print(
                    "WARNING: Provenance confguration missing, processing will continue without provenance recordings",
                )
            else:
                ## Ignore returned remaining command line arguments. Will be taken care of in main()
                print(prov_config)
                configure_prov_run(
                    graph,
                    provImpClass=(ProvenanceType,),
                    sprovConfig=prov_config,
                    force=True,
                )
        else:
            print(
                "WARNING: --provenance-config supplied, but no config seems to be specified.\n"
                "Command line argument parsing may break and/or no provenance may be generated.\n"
                "Specify --provenance-config=inline if the configuration is in the workflow.",
            )
    return graph, inputs


def parse_common_args():  # pragma: no cover
    parser = create_arg_parser()
    return parser.parse_known_args()


import time


def main():  # pragma: no cover
    from importlib import import_module

    args, remaining = parse_common_args()

    graph, inputs = load_graph_and_inputs(args)
    if graph is None:
        return

    try:
        # see if platform is in the mappings file as a simple name
        target = config[args.target]
    except KeyError:
        # it is a proper module name - fingers crossed...
        target = args.target

    try:
        mod = import_module(target)
        args = mod.parse_args(remaining, args)
    except SystemExit:
        # the sub parser raised an error
        raise
    except Exception as e:
        # no other arguments required for target
        pass

    print("RUN ARGS: ")
    print(args)
    print("==========")

    mod = import_module(target)
    start_time = time.time()
    errormsg = mod.process(graph, inputs=inputs, args=args)
    if errormsg:
        print(errormsg)

    print("ELAPSED TIME: " + str(time.time() - start_time))


if __name__ == "__main__":  # pragma: no cover
    main()
