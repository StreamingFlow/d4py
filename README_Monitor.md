# dispel4py Monitoring Guide

This document explains the three monitoring-enabled mappings:

- `timed_multi`
- `timed_simple`
- `timed_mpi`

These mappings run your workflow and automatically produce timing and graph artifacts under a monitoring output directory (default: `timings/`).

## 1. What These Mappings Are For

### `timed_simple`
Use this for sequential/local debugging and baseline measurements.

- Runs workflow in a single Python process (simple mapping behavior).
- Best when you want easy reproducibility and low setup complexity.
- Good for checking logic and quick latency checks before parallel runs.

### `timed_multi`
Use this for local multiprocessing performance analysis.

- Runs workflow with Python multiprocessing.
- Uses mapping/process allocation logic from `multi`.
- Best for measuring how PE instance distribution and local parallelism affect timing.

### `timed_mpi`
Use this for MPI/distributed performance analysis.

- Runs workflow with MPI ranks.
- Best for cluster/HPC-style runs or true distributed execution.
- Launch with `mpiexec`/`mpirun`.

## 2. Common Output Goal

All three timed mappings produce:

- Per-instance totals and averages.
- Per-iteration timings for each PE instance.
- Aggregate summaries with latency statistics (`min`, `p50`, `p95`, `max`).
- Abstract graph shape (workflow as defined by user).
- Concrete graph shape (runtime PE instances and edges).
- Optional PNG figures for abstract and concrete graphs.

## 3. Command Examples

## `timed_multi` (local parallel)

Basic example:

```bash
dispel4py timed_multi dispel4py.examples.graph_testing.word_count -i 10 -n 10 
```
**Note** that if we do not specify `--timing-dir` directory, it will create automatically and store traces in `./timings` directory. 

Recommended explicit version 

```bash
dispel4py timed_multi dispel4py.examples.graph_testing.word_count -i 10 -n 10 --print-shape

```

Custom output location:

```bash
dispel4py timed_multi dispel4py.examples.graph_testing.word_count -i 10 -n 10 \
  --timing-dir timings_wc --timing-prefix wc_monitor
```

## `timed_simple` (sequential)

```bash
dispel4py timed_simple dispel4py.examples.graph_testing.word_count -i 10 --print-shape
```

## `timed_mpi` (MPI/distributed)

```bash
mpiexec -n 10 dispel4py timed_mpi dispel4py.examples.graph_testing.word_count -i 10 --print-shape
```

Optional explicit MPI process count:

```bash
mpiexec -n 10 dispel4py timed_mpi dispel4py.examples.graph_testing.word_count -i 10 --num_processes 10
```

## 4. Mapping-Specific Flags

### `timed_multi`

- `-n`, `--num`: number of local worker processes.
- `-s`, `--simple`: force partitioned/simple-style fallback behavior from multi mapping.

### `timed_mpi`

- `-n`, `--num_processes`: number of MPI processes (optional if inferred from MPI world size).
- `-s`, `--simple`: force partitioned/simple-style fallback behavior.

### `timed_simple`

- No mapping-specific count flag.
- Uses sequential/simple processing.

## 5. Shared Monitoring Flags (All Three)

- `--timing-dir`: output directory (default: `timings`)
- `--timing-prefix`: filename prefix (default: `monitor`)
- `--run-id`: custom run identifier; if omitted, generated automatically
- `--summary-file`: output path for per-PE summary CSV
- `--instance-summary-file`: output path for per-instance summary CSV
- `--iteration-summary-file`: output path for merged per-iteration CSV
- `--iteration-latency-summary-file`: output path for per-instance latency stats derived from iteration timings
- `--shape-file`: output path for abstract graph JSON
- `--concrete-shape-file`: output path for concrete graph JSON
- `--abstract-figure-file`: output path for abstract graph PNG
- `--concrete-figure-file`: output path for concrete graph PNG
- `--no-graph-figures`: skip PNG generation
- `--print-shape`: print abstract and concrete shape details to stdout

## 6. Understanding Each Generated File

Given a run like:

```bash
ls -lht timings
```

You may see files such as:

- `monitor_shape_run<id>.json`
- `monitor_concrete_shape_run<id>.json`
- `monitor_summary_run<id>.csv`
- `monitor_instances_run<id>.csv`
- `monitor_iteration_timings_run<id>.csv`
- `monitor_iteration_timings_summary_run<id>.csv`
- `monitor_<PE>_rank<R>_run<id>.csv`
- `monitor_iterations_<PE>_rank<R>_run<id>.csv`
- `monitor_abstract_graph_run<id>.png`
- `monitor_concrete_graph_run<id>.png`

### `monitor_shape_run<id>.json`
Abstract workflow graph.

- Represents the user-defined workflow topology.
- Nodes are logical PEs.
- Edges are logical workflow connections.
- Includes topological order when possible.

### `monitor_concrete_shape_run<id>.json`
Concrete runtime instance graph.

- Represents instantiated PE ranks used in execution.
- Nodes are PE instances such as `WordCounter1@2`.
- Edges represent concrete communication paths between ranks.
- Includes process allocation table and topological order.

### `monitor_<PE>_rank<R>_run<id>.csv`
Per-instance total timing summary.

- One file per PE instance/rank.
- Contains `count`, `total_secs`, `avg_secs`.
- Useful for fast instance-level totals.

### `monitor_iterations_<PE>_rank<R>_run<id>.csv`
Per-instance per-iteration trace.

- One row per iteration processed by that PE instance.
- Core file when you need iteration-level latencies.

### `monitor_summary_run<id>.csv`
Per-PE aggregate summary across all ranks.

- Combines all instances of each PE.
- Includes:
  - `total_count`, `total_secs`, `avg_secs`
  - `min_secs`, `p50_secs`, `p95_secs`, `max_secs`

### `monitor_instances_run<id>.csv`
Per-PE-instance aggregate summary.

- One row per instance (`PE@rank`).
- Includes:
  - `total_count`, `total_secs`, `avg_secs`
  - `min_secs`, `p50_secs`, `p95_secs`, `max_secs`

### `monitor_iteration_timings_run<id>.csv`
Merged iteration-level table across all instances.

- Each row is an iteration timing event.
- Includes iteration latency plus instance-level context columns:
  - `instance_p50_secs`, `instance_p95_secs`, `instance_max_secs`

### `monitor_iteration_timings_summary_run<id>.csv`
Latency summary derived from per-iteration data.

- One row per instance.
- Computed directly from iteration traces.
- Includes:
  - `iteration_count`, `total_secs`, `avg_secs`
  - `min_secs`, `p50_secs`, `p95_secs`, `max_secs`

### `monitor_abstract_graph_run<id>.png`
Figure of abstract graph.

- Visual of user-defined workflow topology.

### `monitor_concrete_graph_run<id>.png`
Figure of concrete graph.

- Visual of instantiated runtime graph (ranks/instances).

## 7. Abstract vs Concrete (Key Difference)

Abstract graph:

- What you define in workflow code.
- Independent of runtime process assignment.

Concrete graph:

- What is actually executed after mapping allocates ranks/instances.
- Depends on mapping (`simple`, `multi`, `mpi`) and process assignment rules.

## 8. Practical Interpretation Tips

- Use `monitor_iterations_*.csv` for detailed latency analysis and jitter/outlier detection.
- Use `monitor_instances_*.csv` to compare load balance between ranks.
- Use `monitor_summary_*.csv` to compare PE-level hotspots.
- Use concrete shape JSON/PNG to explain why some ranks do more work.

## 9. Latency Metrics Explained

These columns are computed from per-iteration timings (in seconds):

- `p50_secs`: 50th percentile latency (median). About half of iterations are faster, half are slower.
- `p95_secs`: 95th percentile latency. 95% of iterations are at or below this value; highlights tail/slower behavior.
- `max_secs`: maximum observed latency (slowest iteration).

Related columns:

- `min_secs`: minimum observed latency (fastest iteration).
- `avg_secs`: arithmetic mean latency (can be influenced by outliers).

Quick intuition:

- If `p95_secs` is much larger than `p50_secs`, latency is bursty/has outliers.
- If `max_secs` is far above `p95_secs`, there may be rare extreme slow iterations.

## 10. Notes

- If matplotlib is unavailable or incompatible, PNGs may be skipped.
- JSON and CSV outputs are still generated even when PNGs are skipped.
- `run_id` is timestamp-based and includes microseconds to avoid collisions between rapid consecutive runs.
