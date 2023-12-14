# Copyright (c) The University of Edinburgh 2014
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
Tests for simple sequential processing engine.

"""

from dispel4py.examples.graph_testing import testing_PEs as t
from dispel4py.new import simple_process
from dispel4py.workflow_graph import WorkflowGraph


def testPipeline():
    prod = t.TestProducer()
    cons1 = t.TestOneInOneOut()
    cons2 = t.TestOneInOneOut()
    graph = WorkflowGraph()
    graph.connect(prod, "output", cons1, "input")
    graph.connect(cons1, "output", cons2, "input")
    results = simple_process.process_and_return(
        graph,
        inputs={prod: [{}, {}, {}, {}, {}]},
    )
    assert {cons2.id: {"output": [1, 2, 3, 4, 5]}} == results


def testSquare():
    graph = WorkflowGraph()
    prod = t.TestProducer(2)
    cons1 = t.TestOneInOneOut()
    cons2 = t.TestOneInOneOut()
    last = t.TestTwoInOneOut()
    graph.connect(prod, "output0", cons1, "input")
    graph.connect(prod, "output1", cons2, "input")
    graph.connect(cons1, "output", last, "input0")
    graph.connect(cons2, "output", last, "input1")
    results = simple_process.process_and_return(graph, {prod: [{}]})
    assert {last.id: {"output": ["1", "1"]}} == results


def testTee():
    graph = WorkflowGraph()
    prod = t.TestProducer()
    cons1 = t.TestOneInOneOut()
    cons2 = t.TestOneInOneOut()
    graph.connect(prod, "output", cons1, "input")
    graph.connect(prod, "output", cons2, "input")
    results = simple_process.process_and_return(graph, {prod: [{}, {}, {}, {}, {}]})
    assert {
        cons1.id: {"output": [1, 2, 3, 4, 5]},
        cons2.id: {"output": [1, 2, 3, 4, 5]},
    } == results


def testWriter():
    graph = WorkflowGraph()
    prod = t.TestProducer()
    cons1 = t.TestOneInOneOutWriter()
    graph.connect(prod, "output", cons1, "input")
    results = simple_process.process_and_return(graph, {prod: [{}, {}, {}, {}, {}]})
    assert {cons1.id: {"output": [1, 2, 3, 4, 5]}} == results
