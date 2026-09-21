# Resource monitoring validation

Base repository: https://github.com/StreamingFlow/d4py

Base commit: `07467e43df7a8acc8b78e6354e2c09ac3d18f247` (the `main` checkout inspected for this delivery).

This is a local, reviewable implementation. No changes have been pushed to GitHub.

## Results

- Focused unit and integration suite: **20 passed, 1 skipped**.
- Real `timed_simple` execution: passed, with 3 logical instances sharing 1 OS process.
- Real `timed_multi` execution: passed, with 4 instances in 4 worker processes, including 2 replicas of the compute PE.
- Partitioned `timed_multi -s` execution: passed, retaining the identities of 6 leaf PEs in 2 worker processes.
- Timing-only mode, endpoint-only mode and an idle multiprocessing worker: passed.
- Original supplied traces reaggregated: all original abstract-PE timing/count/rank values preserved (numeric tolerance 1e-9); absent CPU/memory observations remain blank.
- Python compile/static undefined-name checks and patch whitespace checks: passed.

The supplied legacy trace archive contains:

| Mapping | Abstract PEs | Instances | Processing calls |
|---|---:|---:|---:|
| simple | 7 | 7 | 36 |
| multi | 7 | 16 | 506 |

These legacy runs were inspected/reaggregated, **not rerun against their external LLM service**.

## MPI limitation

A real four-rank MPI command was attempted. MPI/UCX failed during startup because
this execution environment denies the required socket operations, including:

`Failed to create unix domain socket for signal: Operation not permitted`

Consequently **no successful live MPI run is claimed**. The timed MPI mapping
uses the same tested instrumentation/exporters and retains its rank-0
aggregation after a barrier. Its integration test is included but skipped by
default. It still needs execution on a working MPI installation, and multi-node
operation needs a shared output filesystem.

Run on the target machine:

```bash
python -m pip install -e .
python -m pip install pytest
D4PY_TEST_MPI=1 python -m pytest -q tests/test_resource_monitoring.py tests/test_resource_monitoring_integration.py
```

This delivery was checked on Linux. macOS/Windows execution, distributed
multi-node MPI and provenance-enabled execution were not validated here.

## Recorded offline demonstration

These are measured outputs, not estimates for the user's agentic workflow.
The example ran 4 source iterations. RSS refers to the whole worker process.
Values vary by machine and run, and monitoring itself adds overhead.

| Mapping | Instance | Calls | CPU seconds | CPU % during calls | Max observed process RSS (MiB) |
|---|---|---:|---:|---:|---:|
| simple | `BusyMemoryPE1@1` | 4 | 0.081714 | 33.97 | 52.05 |
| simple | `SleepPE2@2` | 4 | 0.001136 | 0.71 | 52.00 |
| simple | `SourcePE0@0` | 4 | 0.000007 | 69.97 | 36.02 |
| multi | `BusyMemoryPE1@1` | 2 | 0.121630 | 60.51 | 48.04 |
| multi | `BusyMemoryPE1@2` | 2 | 0.121586 | 60.50 | 48.04 |
| multi | `SleepPE2@3` | 4 | 0.001172 | 0.73 | 32.02 |
| multi | `SourcePE0@0` | 4 | 0.000037 | 105.12 | 32.11 |

## Runtime

- Python 3.12.14
- psutil 7.2.2
- networkx 3.7
- pytest 9.1.1
- mpi4py 4.1.2
- mpich 5.0.1.post1
