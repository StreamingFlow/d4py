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
The dispel4py workflow graph.
"""

import sys

import networkx as nx

from dispel4py.core import GenericPE


class WorkflowNode:
    """
    Wrapper class for workflow nodes - wraps around general subclasses
    of classes denoting PEs, that is GenericPEs.
    """

    # Supported types of workflow nodes:
    WORKFLOW_NODE_PE = 0
    WORKFLOW_NODE_FN = 1
    WORKFLOW_NODE_CP = 2
    node_counter = 0

    def __init__(self, obj):
        self.obj = obj
        self.outputs = []
        self.inputs = []

        if isinstance(obj, GenericPE):
            obj.id = obj.name + str(WorkflowNode.node_counter)
            WorkflowNode.node_counter += 1
            self.nodeType = self.WORKFLOW_NODE_PE

            for _i in obj.inputconnections.values():
                # Empty for the time being - only the index matters
                self.inputs.append({})

            for _i in obj.outputconnections.values():
                self.outputs.append({})
        elif isinstance(obj, WorkflowGraph):
            self.nodeType = self.WORKFLOW_NODE_CP
            try:
                for _i in obj.inputmappings:
                    self.inputs.append({})
            except AttributeError:
                pass
            try:
                for _i in obj.outputmappings:
                    self.outputs.append({})
            except AttributeError:
                pass
        else:
            sys.stderr.write(
                f"Error: Unknown type of object passed as a \
                              Workflow Node: {type(obj)}\n",
            )
            raise TypeError(
                f"Unknown type of object passed as a \
                             Workflow Node: {type(obj)}",
            )

    def get_contained_object(self):
        """Returns the wrapped PE or function."""
        return self.obj


# Used as attribute names
FROM_CONNECTION = "from_connection"
TO_CONNECTION = "to_connection"
DIRECTION = "direction"


class WorkflowGraph:
    """
    A graph representing the workflow and related methods
    """

    def __init__(self):
        self.graph = nx.Graph()
        self.objToNode = {}

    def add(self, n):
        """
        Adds node n, which must be an instance of
        :py:class:`dispel4py.core.GenericPE`, and returns the created workflow
        node.

        :rtype: WorkflowNode
        """
        nd = WorkflowNode(n)
        self.graph.add_node(nd)
        self.objToNode[n] = nd
        return nd

    def connect(self, from_node, from_connection, to_node, to_connection):
        """
        Connect the two given nodes from the given output to the given input.
        If the nodes are not in the graph, they will be added.

        :param from_node: the source PE of the connection
        :param from_connection: the name of the output of the source node
        'fromNode'
        :type from_connection: String
        :param to_node: the destination PE of the connection
        :param to_connection: the name of the input of the destination node
        'toNode'
        :type to_connection: String
        """

        if from_node not in self.objToNode:
            self.add(from_node)
        if to_node not in self.objToNode:
            self.add(to_node)

        from_wf_node = self.objToNode[from_node]
        to_wf_node = self.objToNode[to_node]

        if self.graph.has_edge(from_wf_node, to_wf_node):
            self.graph[from_wf_node][to_wf_node]["ALL_CONNECTIONS"].append(
                (from_connection, to_connection),
            )

        else:
            self.graph.add_edge(
                from_wf_node,
                to_wf_node,
                **{
                    "FROM_CONNECTION": from_connection,
                    "TO_CONNECTION": to_connection,
                    "DIRECTION": (from_node, to_node),
                    "ALL_CONNECTIONS": [(from_connection, to_connection)],
                },
            )

    def get_contained_objects(self):
        nodes = [node.get_contained_object() for node in self.graph.nodes()]
        return sorted(nodes, key=lambda x: x.id)

    def propagate_types(self):
        """
        Propagates the types throughout the graph by retrieving the output
        types from each node, starting from the root, and providing them to
        connected consumers.
        """
        visited = set()
        for node in self.graph.nodes():
            if node not in visited:
                self.__assign_types(node, visited)

    def __assign_types(self, node, visited):
        pe = node.get_contained_object()
        inputTypes = {}
        for edge in self.graph[node].values():
            if pe == edge["DIRECTION"][1]:
                # pe is destination so look up the types produced by sources
                source = edge["DIRECTION"][0]
                sourceNode = self.objToNode[source]
                if sourceNode not in visited:
                    self.__assign_types(sourceNode, visited)
                inType = source.get_output_types()[edge["FROM_CONNECTION"]]
                inputTypes[edge["TO_CONNECTION"]] = inType
        pe.set_input_types(inputTypes)
        visited.add(node)

    def flatten(self):
        """
        Subgraphs contained within composite PEs are added to the top level
        workflow.
        """
        hasComposites = True
        toRemove = set()
        while hasComposites:
            hasComposites = False
            toRemove = set()
            for node in list(self.graph.nodes()):
                if node.nodeType == WorkflowNode.WORKFLOW_NODE_CP:
                    hasComposites = True
                    toRemove.add(node)
                    wfGraph = node.get_contained_object()
                    subgraph = wfGraph.graph
                    self.graph.add_nodes_from(subgraph.nodes(data=True))
                    self.graph.add_edges_from(subgraph.edges(data=True))
                    self.objToNode.update(wfGraph.objToNode)
                    for inputname in wfGraph.inputmappings:
                        toPE, toConnection = wfGraph.inputmappings[inputname]
                        edge = None
                        fromPE, fromConnection = None, None
                        for e in self.graph[node].values():
                            if (
                                wfGraph == e["DIRECTION"][1]
                                and inputname == e["TO_CONNECTION"]
                            ):
                                fromPE = e["DIRECTION"][0]
                                fromConnection = e["FROM_CONNECTION"]
                                edge = self.objToNode[fromPE], self.objToNode[wfGraph]
                                break
                        if edge is not None:
                            self.connect(fromPE, fromConnection, toPE, toConnection)
                    for outputname in wfGraph.outputmappings:
                        fromPE, fromConnection = wfGraph.outputmappings[outputname]
                        destinations = []
                        for e in self.graph[node].values():
                            if (
                                wfGraph == e["DIRECTION"][0]
                                and outputname == e["FROM_CONNECTION"]
                            ):
                                toPE = e["DIRECTION"][1]
                                toConnection = e["TO_CONNECTION"]
                                destinations.append((toPE, toConnection))
                        for toPE, toConnection in destinations:
                            self.connect(fromPE, fromConnection, toPE, toConnection)
            self.graph.remove_nodes_from(toRemove)


def _create_dot(graph, instance_names=None, counter=0):
    if instance_names is None:
        instance_names = {}

    dot = ""

    # Assign unique names
    for node in graph.graph.nodes():
        try:
            name = node.get_contained_object().id, counter
        except Exception:
            name = node.get_contained_object().__class__.__name__, counter
        instance_names[node] = name
        counter += 1

    # Now add all the nodes and their input and output connections
    cluster_index = 0
    for node in graph.graph.nodes():
        pe = node.get_contained_object()
        if isinstance(pe, WorkflowGraph):
            dot += _create_cluster(pe, cluster_index, instance_names, counter)
            cluster_index += 1
            continue

        name, index = instance_names[node]
        dot += f'{name}{index}[label="{{ '
        # Add inputs
        inputNames = []
        outputNames = []
        for edge in graph.graph[node].values():
            if pe == edge["DIRECTION"][1]:
                inputName = edge["TO_CONNECTION"]
                dotName = f"<in_{inputName}>{inputName}"
                if dotName not in inputNames:
                    inputNames.append(dotName)
            else:
                outputName = edge["FROM_CONNECTION"]
                dotName = f"<out_{outputName}>{outputName}"
                if dotName not in outputNames:
                    outputNames.append(dotName)

        if inputNames:
            dot += "{{{}}} | ".format(" | ".join(inputNames))
        dot += name
        if outputNames:
            dot += "| {{{}}}".format(" | ".join(outputNames))
        dot += ' }"];\n'

    # connect the inputs and outputs
    for node in graph.graph.nodes():
        pe = node.get_contained_object()
        for edge in graph.graph[node].values():
            if pe == edge["DIRECTION"][0]:
                if isinstance(pe, WorkflowGraph):
                    inner_source, source_output = pe.outputmappings[
                        edge["FROM_CONNECTION"]
                    ]
                    node = pe.objToNode[inner_source]
                else:
                    source_output = edge["FROM_CONNECTION"]
                # pe is the source so look up the connected destination
                dest = edge["DIRECTION"][1]
                if isinstance(dest, WorkflowGraph):
                    inner_dest, dest_input = dest.inputmappings[edge["TO_CONNECTION"]]
                    destNode = dest.objToNode[inner_dest]
                else:
                    destNode = graph.objToNode[dest]
                    dest_input = edge["TO_CONNECTION"]
                dot += "{}{}:out_{}->{}{}:in_{};\n".format(
                    *instance_names[node],
                    source_output,
                    *instance_names[destNode],
                    dest_input,
                )
    return dot


def _create_cluster(graph, index, instanceNames, counter):
    dot = f"subgraph cluster_%s {{{index}\n"
    try:
        # names for composite PEs are optional
        dot += f'label = "{graph.name}";'
    except:
        pass
    dot += "style=filled;\ncolor=lightgrey;\n"
    if index % 2:
        dot += "fillcolor=lightgrey;\n"
    dot += _create_dot(graph, instanceNames, counter) + "}\n"
    return dot


def draw(graph):
    """
    Creates a representation of the workflow graph in the dot language.
    """
    return f"digraph request\n{{\nnode [shape=Mrecord, \
           style=filled, fillcolor=white];\n{_create_dot(graph)}}}\n"


def drawDot(graph, img_type="png"):  # pragma: no cover
    """
    Draws the workflow as a graph and creates an image using graphviz dot.
    The image type is PNG by default.
    See https://graphviz.gitlab.io/_pages/doc/info/output.html for supported
    output types.
    """
    from subprocess import PIPE, Popen

    dot = draw(graph)

    p = Popen(["dot", "-T", img_type], stdout=PIPE, stdin=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate(dot.encode("utf-8"))
    return stdout


def write_image(graph, filename, img_type="png"):
    """
    Draws the workflow as a graph using graphviz dot and writes the image
    to the named output file.
    The output format is PNG by default.
    See https://graphviz.gitlab.io/_pages/doc/info/output.html for supported
    output formats.
    """

    with open(filename, "wb") as f:
        f.write(drawDot(graph, img_type))
