# dispel4py monitoring: time, CPU and memory

The monitoring mappings `timed_simple`, `timed_multi` and `timed_mpi` run the
corresponding workflow mapping and record elapsed time, process CPU time and
process memory observations. Ordinary `simple`, `multi` and `mpi` remain
uninstrumented. Use their `timed_*` equivalents to enable monitoring.

Resource monitoring is enabled by default in this extension. Existing timing
columns and graph outputs are retained, with extra resource columns appended.

## 1. Abstract PE, PE instance and processing call

| Level | Example | Meaning |
|---|---|---|
| Abstract PE | `ParallelLLMSensorAgentPE4` | One logical node in the workflow. Its summary pools observations from all its runtime instances. |
| PE instance | `ParallelLLMSensorAgentPE4@9` | That logical PE executing at rank 9. Other replicas might be `@10`, `@11` and `@12`. |
| Processing call | Instance `@9`, iteration 2 | One invocation of that instance's `process(inputs)` method. |

`pe_id` identifies a graph node, **not a Python class**. Two graph nodes created
from the same class remain separate abstract PEs. `instance_id` is `pe_id@rank`
and is unique within a run. Include `run_id` when combining different runs.

In `timed_simple`, ranks are logical PE positions: several different ranks share
one actual OS process. In normal `timed_multi`, each instance has its own worker
process. In normal `timed_mpi`, each instance executes in an MPI rank. The trace
also records `hostname` and OS `pid`, so process identity is not confused with
logical rank. Partitioned execution can place several PEs in one process.

## 2. Installation and commands

From the updated repository:

```bash
python -m pip install -e .
```

The new dependency is `psutil>=5.9`, included in `requirements.txt`. MPI additionally
requires a working MPI runtime and a compatible `mpi4py` installation on every
participating node. Use the MPI stack recommended by your cluster.

The included offline example needs no API keys or external services:

```bash
# Sequential: one source, one compute PE, one waiting PE in one OS process.
dispel4py timed_simple dispel4py.examples.graph_testing.resource_monitoring_demo \
  -i 8 --timing-dir monitoring_simple

# Multiprocessing: one source, two compute instances, one waiting instance.
dispel4py timed_multi dispel4py.examples.graph_testing.resource_monitoring_demo \
  -i 8 -n 4 --timing-dir monitoring_multi

# MPI: the same four-instance parallel allocation.
mpiexec -n 4 dispel4py timed_mpi dispel4py.examples.graph_testing.resource_monitoring_demo \
  -i 8 --timing-dir monitoring_mpi
```

Replace the example module with your workflow module or Python file. Existing
input flags such as `-f sensor_data.json` and `-i` continue to work. You can also
replace `dispel4py` with `python -m dispel4py.new.processor`.

**MPI output directory:** all ranks must see the same shared `--timing-dir` path.
Workers write their files, synchronize at the existing MPI barrier, and rank 0
reads and aggregates them. This implementation does not gather files from
node-local disks. Use a shared filesystem for multi-node runs.

### Resource options

| Flag | Default | Meaning |
|---|---|---|
| `--memory-sampling-interval SECONDS` | `0.01` | Best-effort RSS sampling during processing calls, in addition to call-boundary readings. |
| `--memory-sampling-interval 0` | — | Read RSS only before and after each call; no background sampler thread. |
| `--no-resource-monitoring` | Off | Record elapsed time only. Resource measurements remain empty in the CSVs. |

For example:

```bash
# 5 ms target sampling interval.
dispel4py timed_multi your_workflow.py -i 100 -n 8 \
  --memory-sampling-interval 0.005 --timing-dir monitoring_5ms

# Timing-only comparison run.
dispel4py timed_simple your_workflow.py -i 100 \
  --no-resource-monitoring --timing-dir monitoring_time_only
```

Intervals must be finite and nonnegative. Smaller intervals increase overhead
and do not guarantee that every short-lived allocation will be observed.

## 3. What is measured

Each successful `PE.process(inputs)` call is surrounded by measurements.

| Measurement | Definition |
|---|---|
| Elapsed time | Duration of the call from the monotonic `time.perf_counter_ns()` clock. Includes waiting inside the call. |
| CPU seconds | Difference in `time.process_time_ns()` across the call: OS-accounted user and system CPU time of the current process, including its threads. |
| CPU percent | `100 × CPU seconds / elapsed seconds`, relative to one logical CPU. |
| RSS before/after | Resident memory of the current OS process just before/after the call, using `psutil.Process().memory_info().rss`. |
| RSS change | `RSS after − RSS before`, which can be positive, zero or negative. |
| Observed RSS peak | Maximum of the boundary readings and successful periodic readings associated with the call. |

CPU measurements use the process executing the PE, not the coordinator process.
Process handles and sampler threads are created lazily inside the executing
worker, after copying/spawning the workflow. Each active instance reuses its
sampler across calls; the sampler sleeps between calls and is stopped during
postprocessing. No background sampler is created in endpoint-only mode.

### CPU interpretation

- A CPU-bound single-threaded call can approach 100%; a waiting call generally
  has low CPU utilisation despite a long elapsed time.
- A process using several native threads can exceed 100%. Percentages are not
  divided by the host's CPU count.
- Process CPU includes background threads, MPI progress threads and small
  monitoring costs occurring during the call. It is not exact exclusive CPU
  attribution to the PE's Python function.
- Child-process CPU and remote service/GPU usage are not included. A PE waiting
  for an external LLM may have long elapsed time and little **local** CPU work.
- Very short calls can have noisy percentages because the clocks have finite
  resolution and are read sequentially. Prefer aggregated CPU seconds/percent.

### Memory interpretation

RSS describes the **whole hosting process observed while the PE runs**. It
includes the Python interpreter, imports, native allocations, buffers, retained
objects and monitoring data. It is not the size of the PE object or a count of
bytes exclusively allocated by that PE.

This distinction is essential for `timed_simple`: memory retained by an earlier
PE can be visible during a later PE's calls. In partitioned multi/MPI execution,
PEs sharing a worker also share its RSS. Even in a dedicated worker, RSS includes
the worker runtime and can count shared pages that also appear in another
process's RSS.

The observed peak is sampled, not an exact peak. Short allocations can be missed;
the Python GIL, native code and OS scheduling can delay the sampling thread.
With interval zero, the peak is just `max(before, after)`. It does not use a
process-lifetime high-water mark that would carry an earlier PE's peak forward.
RSS may remain high after objects are freed because allocators retain memory.

**Never sum these memory values to claim total workflow memory.** Abstract-PE
memory summaries describe the observed distribution and maximum across its
instances; they do not estimate simultaneous aggregate memory consumption.

### Scope and overhead

The window follows the existing timing monitor: successful `process()` calls.
It excludes explicit `preprocess()`/`postprocess()` execution, input-queue waiting
outside `process()`, and output forwarding performed by the mapping after
`process()` returns. Writes and waits made inside `process()` are included.
A PE whose work is entirely in `postprocess()` is therefore not fully profiled.

Boundary memory reads are outside the reported CPU/time interval, but still add
to overall run overhead. The sampler, extra CSV columns and buffered iteration
records also cost CPU/memory. As before, traces are buffered and written during
postprocessing; very large runs need memory for those records. Aborted/killed
runs can have incomplete files. Failed calls are not counted as successful
iterations, and this extension does not change mapping-level failure handling.
A metadata file marks the run configuration, not successful completion.

## 4. Output files

All files default to `timings/`; the default prefix is `monitor`.

| File | Level/content |
|---|---|
| `monitor_<PE>_rank<R>_run<ID>.csv` | One instance: totals plus CPU/memory summary and actual process identity. |
| `monitor_iterations_<PE>_rank<R>_run<ID>.csv` | Each successful call of one PE instance: elapsed time, CPU and memory observations. |
| `monitor_instances_run<ID>.csv` | One row per PE instance, including allocated instances that processed no calls. |
| `monitor_summary_run<ID>.csv` | One row per abstract PE, aggregating its instances. |
| `monitor_iteration_timings_run<ID>.csv` | All iteration rows, including CPU/memory and instance timing percentiles. |
| `monitor_iteration_timings_summary_run<ID>.csv` | Instance summaries derived from iteration records. Instances with no calls have no row here. |
| `monitor_resources_run<ID>.json` | Schema version 2, mapping, clocks, resource settings and attribution rules. |
| `monitor_shape_run<ID>.json` | Abstract workflow nodes, connections and topological order where available. |
| `monitor_concrete_shape_run<ID>.json` | Runtime allocation graph. |
| `monitor_abstract_graph_run<ID>.png` | Optional abstract graph figure. |
| `monitor_concrete_graph_run<ID>.png` | Optional runtime graph figure. |

The existing graph generation is preserved. With partitioned fallback, concrete
graphs show partition wrappers; the resource CSVs retain the underlying PE IDs
and runtime ranks. Use CSV `process_ids` to identify co-located PEs. Ordinary
unpartitioned executions show the PE instances in the concrete graph.

### Per-call fields

The five original fields are unchanged and remain first:
`pe_id, rank, instance_id, iteration_index, iteration_secs`.

| Added field | Meaning |
|---|---|
| `run_id`, `mapping` | Run and mapping identity. |
| `hostname`, `pid` | Actual executing OS process. Use both together within a run. |
| `resource_enabled` | 1 if CPU/memory monitoring is enabled, otherwise 0. |
| `cpu_secs` | Process CPU seconds consumed during the call. |
| `cpu_percent` | CPU seconds / elapsed seconds × 100. |
| `rss_before_bytes`, `rss_after_bytes` | Boundary RSS readings in bytes. |
| `rss_delta_bytes` | Signed after-minus-before difference. |
| `rss_peak_observed_bytes` | Largest observed RSS for that call. |
| `rss_sample_count` | Successful RSS readings, including both endpoints; normally at least 2. |
| `rss_sample_errors` | Failed periodic RSS reads; inspect before interpreting peaks. Boundary-read failures raise an error. |
| `memory_sampling_interval_secs` | Configured periodic interval; zero means endpoints only. |
| `cpu_scope` | `process_during_call`. |
| `memory_scope` | `process_rss_during_call`. |

The merged iteration file keeps its existing `instance_p50_secs`,
`instance_p95_secs` and `instance_max_secs` columns before the new fields.
These are processing-call durations, not end-to-end event latency through the
whole workflow. An iteration is a call, not necessarily one input record: a
source call may emit many records.

### Summary fields and aggregation

Existing timing fields are retained: counts, `total_secs`, `avg_secs`,
`min_secs`, `p50_secs`, `p95_secs`, `max_secs`. The per-worker total file retains
its original `count` column. The abstract summary retains `rank_count` and `ranks`.

| Added summary field | Aggregation over calls in the instance or abstract PE |
|---|---|
| `run_id`, `mapping` | Run/mapping values. |
| `process_ids`, `process_count` | Distinct `hostname:pid` values and their count, including idle instances where recorded. |
| `resource_enabled` | Resource configuration reported in source records. |
| `resource_count` | Number of calls with resource observations. |
| `total_cpu_secs` | Sum of observed CPU seconds. |
| `avg_cpu_secs` | Total CPU seconds / resource count. |
| `cpu_percent` | 100 × summed CPU seconds / summed elapsed seconds **for calls with resource observations**. |
| `rss_min_bytes` | Minimum call-boundary RSS. |
| `rss_max_bytes` | Maximum observed RSS across the calls/instances; **not a sum**. |
| `rss_mean_endpoint_bytes` | Mean of all before and after values, equally weighted per endpoint; not a time-weighted mean. |
| `rss_delta_mean_bytes` | Mean signed per-call RSS change. |
| `rss_delta_min_bytes`, `rss_delta_max_bytes` | Minimum/maximum signed per-call RSS change. |
| `rss_sample_count`, `rss_sample_errors` | Sums of the corresponding call counters. |
| `memory_sampling_interval_secs` | Distinct configured intervals represented in the observations. |
| `cpu_scope`, `memory_scope` | Same process-level attribution labels as above. |

CPU percentages are recalculated from totals, not averaged across workers.
For example, calls using 1 CPU second over 1 elapsed second and 1 CPU second over
3 elapsed seconds produce `100 × 2 / 4 = 50%`. This is average utilisation during
processing across the contributing call windows, not combined machine-wide
utilisation or the number of cores used over whole-run wall time.

Latency percentiles pool all raw call durations of the relevant PE/instance;
they do not average worker percentiles. Summed elapsed seconds can exceed whole
workflow elapsed time when instances execute concurrently.

Idle instances have `total_count=0`, `resource_count=0`, and blank CPU/memory
measurements. Blank means unobserved, not zero memory. Timing-only and older
traces also have blank resource measurements. Resource values cannot be
reconstructed retrospectively from timing-only files.

## 5. Relating this to the supplied agentic traces

The supplied September 2026 traces contain timing/count columns, not CPU or
memory observations. In the multiprocessing run,
`ParallelLLMSensorAgentPE4` has four instances (`@9`, `@10`, `@11`, `@12`) and
five processing calls overall. Its recorded 85.71 seconds are summed elapsed
processing time, **not CPU seconds**.

Rerunning that workflow with the updated `timed_multi` will add resource
measurements for each of those instances and pool them into the abstract row.
The existing six timing CSV layouts remain recognizable. The original traces
remain useful for timing comparison, but cannot supply missing CPU/RSS values.

## 6. Reading results

Choose the exact run you want when several runs share a directory:

```python
import csv
from pathlib import Path

path = Path("monitoring_multi/monitor_instances_runYOUR_RUN_ID.csv")
with path.open(newline="") as source:
    for row in csv.DictReader(source):
        cpu = row["total_cpu_secs"] or "not observed"
        rss = row["rss_max_bytes"]
        rss_mib = f"{int(rss) / 1024**2:.2f}" if rss else "not observed"
        print(row["instance_id"], "CPU seconds:", cpu,
              "max observed process RSS (MiB):", rss_mib)
```

Use `monitor_summary_run<ID>.csv` instead to compare abstract PEs. Use the merged
iteration file to inspect outlier calls. RSS bytes / `1024**2` gives MiB.

## 7. Other supported flags

Shared by all three timed mappings:

- `--timing-dir` (default `timings`), `--timing-prefix` (default `monitor`).
- `--run-id`: optional custom ID; otherwise an automatic UTC timestamp with microseconds.
- `--summary-file`: abstract-PE summary CSV path.
- `--instance-summary-file`: PE-instance summary CSV path.
- `--iteration-summary-file`: merged iteration CSV path.
- `--iteration-latency-summary-file`: iteration-derived instance summary path.
- `--shape-file`, `--concrete-shape-file`: graph JSON paths.
- `--abstract-figure-file`, `--concrete-figure-file`: graph PNG paths.
- `--no-graph-figures`: skip PNG generation.
- `--print-shape`: print abstract/concrete topology.

Relative output paths are resolved against `--timing-dir`. A previously used
prefix/run ID in that directory is rejected to prevent old/new traces being
mixed. Choose a fresh run ID or directory for another execution.

`timed_multi` accepts `-n`/`--num` and `-s`/`--simple` for the existing partitioned
fallback. `timed_mpi` accepts `-n`/`--num_processes` (otherwise inferred from MPI
world size) and `-s`/`--simple`. Keep MPI process count consistent with `mpiexec`.
`timed_simple` has no process-count flag.

Partition construction and allocation rules are inherited from the original
mappings. Explicit replicated `numprocesses` settings can exceed the capacity of
an undersized/partitioned run; the extension does not change that allocator.

PNG figures require compatible matplotlib; CSV and JSON output remains usable
without figures. For performance comparisons, run without provenance where
possible. If provenance is enabled, its work contributes to the operational
measurements. Readers using fixed positional CSV column counts must be updated
for the appended fields; prefer named columns.

## 8. Validation

```bash
python -m pip install pytest
python -m pytest -q tests/test_resource_monitoring.py tests/test_resource_monitoring_integration.py

# On a machine with a working MPI runtime, also run the real MPI integration case:
D4PY_TEST_MPI=1 python -m pytest -q tests/test_resource_monitoring_integration.py
```

The tests cover CPU versus waiting, transient sampled memory peaks, per-call
peak reset, weighted aggregation, shared-process identity, idle workers,
timing-only/endpoint modes, legacy trace loading, sampler cleanup and partitioned
multiprocessing. The MPI test is opt-in and launches four actual MPI ranks.
See the accompanying `MONITORING_VALIDATION.md` for the results achieved for this delivery.

Existing monitoring tutorial (predates the new resource fields):
[Google Colab tutorial](https://colab.research.google.com/drive/1nlwvYh2hBjPuorGAzq2TjbvzD7n5azyS?usp=sharing).
