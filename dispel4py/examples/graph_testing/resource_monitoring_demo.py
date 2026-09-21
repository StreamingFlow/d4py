"""Offline CPU, transient-memory, and waiting example for all timed mappings.

    dispel4py timed_simple dispel4py.examples.graph_testing.resource_monitoring_demo -i 8
    dispel4py timed_multi dispel4py.examples.graph_testing.resource_monitoring_demo -i 8 -n 4
    mpiexec -n 4 dispel4py timed_mpi dispel4py.examples.graph_testing.resource_monitoring_demo -i 8

The parallel mapping has 1 source, 2 BusyMemoryPE instances and 1 SleepPE.
"""
import time

from dispel4py.base import IterativePE, ProducerPE
from dispel4py.workflow_graph import WorkflowGraph


class SourcePE(ProducerPE):
    def __init__(self):
        super().__init__()
        self.index = 0

    def _process(self, inputs):
        self.index += 1
        return self.index


class BusyMemoryPE(IterativePE):
    def _process(self, data):
        # Touch 16 MiB and hold it across several sampling intervals.
        temporary = bytearray(16 * 1024 * 1024)
        checksum = sum(i * i for i in range(400_000))
        time.sleep(0.04)
        # It is released before the after-call reading: sampling may capture it.
        return {"item": data, "checksum": checksum, "allocated_bytes": len(temporary)}


class SleepPE(IterativePE):
    def _process(self, data):
        time.sleep(0.04)
        return data


source = SourcePE()
busy = BusyMemoryPE()
busy.numprocesses = 2
sleeper = SleepPE()
sleeper.numprocesses = 1
graph = WorkflowGraph()
graph.connect(source, "output", busy, "input")
graph.connect(busy, "output", sleeper, "input")
