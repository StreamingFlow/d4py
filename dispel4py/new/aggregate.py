"""
Processing elements that implement aggregation functions
(AVG, SUM, COUNT, MIN, MAX).
These are composite PEs that are automatically parallelised if the mapping
supports this.
"""

import math

from dispel4py.core import GenericPE
from dispel4py.workflow_graph import WorkflowGraph


class AggregatePE(GenericPE):
    INPUT_NAME = "input"
    OUTPUT_NAME = "output"

    def __init__(self, indexes=None):
        if indexes is None:
            indexes = [0]
        GenericPE.__init__(self)
        self._add_input(self.INPUT_NAME)
        self._add_output(self.OUTPUT_NAME)
        self.indexes = indexes
        self.value = [0 for i in indexes]

    def _postprocess(self):
        self.write(AggregatePE.OUTPUT_NAME, self.value)


class ContinuousReducePE(GenericPE):
    INPUT_NAME = "input"
    OUTPUT_NAME = "output"

    def __init__(self, indexes=None):
        if indexes is None:
            indexes = [0]
        GenericPE.__init__(self)
        self._add_input(self.INPUT_NAME)
        self._add_output(self.OUTPUT_NAME)
        self.indexes = indexes
        self.value = [0 for i in indexes]

    def process(self, inputs):
        self._process(inputs[self.INPUT_NAME])
        self.write(AggregatePE.OUTPUT_NAME, self.value)


class CountPE(AggregatePE):
    def __init__(self):
        AggregatePE.__init__(self, [0])

    def _process(self, inputs):
        self.value = [self.value[0] + 1]


class MaxPE(AggregatePE):
    def __init__(self, indexes=None):
        if indexes is None:
            indexes = [0]
        AggregatePE.__init__(self, indexes)

    def _process(self, inputs):
        v = inputs[AggregatePE.INPUT_NAME]
        self.value = [max(v[i], self.value[i]) for i in self.indexes]


class MinPE(AggregatePE):
    def __init__(self, indexes=None):
        if indexes is None:
            indexes = [0]
        AggregatePE.__init__(self, indexes)
        self.value = [None for i in self.indexes]

    def _process(self, inputs):
        v = inputs[AggregatePE.INPUT_NAME]
        for i in self.indexes:
            self.value[i] = (
                min(v[i], self.value[i]) if self.value[i] is not None else v[i]
            )


class SumPE(AggregatePE):
    def __init__(self, indexes=None):
        if indexes is None:
            indexes = [0]
        AggregatePE.__init__(self, indexes)

    def _process(self, inputs):
        v = inputs[AggregatePE.INPUT_NAME]
        self.value = [self.value[i] + v[i] for i in self.indexes]


class AverageParallelPE(GenericPE):
    INPUT_NAME = "input"
    OUTPUT_NAME = "output"

    def __init__(self, index=0):
        GenericPE.__init__(self)
        self._add_input(self.INPUT_NAME)
        self._add_output(self.OUTPUT_NAME)
        self.index = 0
        self.sum = 0
        self.count = 0

    def _process(self, inputs):
        v = inputs[self.INPUT_NAME][self.index]
        self.sum += v
        self.count += 1

    def _postprocess(self):
        avg = float(self.sum) / self.count
        self.write(self.OUTPUT_NAME, [avg, self.count, self.sum])


class AverageReducePE(GenericPE):
    INPUT_NAME = "input"
    OUTPUT_NAME = "output"

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input(self.INPUT_NAME, grouping="global")
        self._add_output(self.OUTPUT_NAME)
        self.index = 0
        self.sum = 0
        self.count = 0

    def _process(self, inputs):
        v = inputs[self.INPUT_NAME]
        self.count += v[1]
        self.sum += v[2]

    def _postprocess(self):
        if self.count != 0:
            avg = float(self.sum) / self.count
            self.write(self.OUTPUT_NAME, [avg, self.count, self.sum])


class StdDevPE(GenericPE):
    INPUT_NAME = "input"
    OUTPUT_NAME = "output"

    def __init__(self, index=0):
        GenericPE.__init__(self)
        self._add_input(self.INPUT_NAME)
        self._add_output(self.OUTPUT_NAME)
        self.index = index
        self.sum = 0
        self.sum_squared = 0
        self.count = 0

    def _process(self, inputs):
        v = inputs[self.INPUT_NAME][self.index]
        self.sum += v
        self.sum_squared += v * v
        self.count += 1

    def _postprocess(self):
        std_dev = math.sqrt(
            (self.count * self.sum_squared - self.sum * self.sum)
            / (self.count * (self.count - 1)),
        )
        self.write(self.OUTPUT_NAME, (std_dev, self.count, self.sum, self.sum_squared))


class StdDevReducePE(GenericPE):
    INPUT_NAME = "input"
    OUTPUT_NAME = "output"

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input(self.INPUT_NAME, grouping="global")
        self._add_output(self.OUTPUT_NAME)
        self.sum = 0
        self.sum_squared = 0
        self.count = 0

    def _process(self, inputs):
        values = inputs[self.INPUT_NAME]
        self.count += values[0]
        self.sum += values[1]
        self.sum_squared += values[2]

    def _postprocess(self):
        std_dev = math.sqrt(
            (self.count * self.sum_squared - self.sum * self.sum)
            / (self.count * (self.count - 1)),
        )
        self.write(self.OUTPUT_NAME, (std_dev, self.count, self.sum, self.sum_squared))


def parallel_aggregate(inst_pe, reduce_pe):
    composite = WorkflowGraph()
    reduce_pe.inputconnections[AggregatePE.INPUT_NAME]["grouping"] = "global"
    reduce_pe.numprocesses = 1
    composite.connect(inst_pe, AggregatePE.OUTPUT_NAME, reduce_pe, AggregatePE.INPUT_NAME)
    composite.inputmappings = {"input": (inst_pe, AggregatePE.INPUT_NAME)}
    composite.outputmappings = {"output": (reduce_pe, AggregatePE.OUTPUT_NAME)}
    return composite


def parallel_count():
    """
    Creates a counter composite PE that is parallelisable using a
    map-reduce pattern.
    The first part of the composite PE is a counter that counts all the inputs,
    the second part sums up the counts of the counter instances.
    The output of this PE is a single value that is the number of input items.
    """
    pe_sum = SumPE([0])
    pe_sum.name = "CountReduce"
    return parallel_aggregate(CountPE(), pe_sum)


def parallel_sum(indexes=None):
    """
    Creates a SUM composite PE that can be parallelised using a
    map-reduce pattern.
    """
    if indexes is None:
        indexes = [0]
    return parallel_aggregate(SumPE(indexes), SumPE(indexes))


def parallel_min(indexes=None):
    """
    Creates a MIN composite PE that can be parallelised using a
    map-reduce pattern.
    """
    if indexes is None:
        indexes = [0]
    return parallel_aggregate(MinPE(indexes), MinPE(indexes))


def parallel_max(indexes=None):
    """
    Creates a MAX composite PE that can be parallelised using a
    map-reduce pattern.
    """
    if indexes is None:
        indexes = [0]
    return parallel_aggregate(MaxPE(indexes), MaxPE(indexes))


def parallel_avg(index=0):
    """
    Creates an AVG composite PE that can be parallelised using a
    map-reduce pattern.
    """
    composite = WorkflowGraph()
    parAvg = AverageParallelPE(index)
    reduceAvg = AverageReducePE()
    composite.connect(parAvg, parAvg.OUTPUT_NAME, reduceAvg, reduceAvg.INPUT_NAME)
    composite.inputmappings = {"input": (parAvg, parAvg.INPUT_NAME)}
    composite.outputmappings = {"output": (reduceAvg, reduceAvg.OUTPUT_NAME)}
    return composite


def parallel_std_dev(index=0):
    """
    Creates a STDDEV composite PE that can be parallelised using a
    map-reduce pattern.
    """
    composite = WorkflowGraph()
    par_std_dev = StdDevPE(index)
    reduce_std_dev = StdDevReducePE()

    composite.connect(
        par_std_dev,
        par_std_dev.OUTPUT_NAME,
        reduce_std_dev,
        reduce_std_dev.INPUT_NAME,
    )
    
    composite.inputmappings = {"input": (par_std_dev, par_std_dev.INPUT_NAME)}
    composite.outputmappings = {"output": (reduce_std_dev, reduce_std_dev.OUTPUT_NAME)}
    return composite
