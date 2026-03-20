# dispel4py Provenance Guide

This guide explains how provenance currently works in this repository, how to configure it, how to run it, and how to interpret the generated `bulk_*` files.

## 1. What Provenance Captures

When provenance is enabled, dispel4py records:

- `workflow_run` metadata: run-level information (workflow id/name, mapping, user, modules, start time, etc.).
- `lineage` metadata: per-iteration lineage information for processing elements (PEs), including streams and derivations.

Storage is controlled by `s-prov:save-mode`:

- `file`: write provenance to local `bulk_*` files.
- `service`: send provenance to an external endpoint.
- `sensor`: route provenance to recorder PEs.

## 2. Provenance Fixes Included In This Codebase

The following issues were fixed to make provenance work reliably with modern Python and timed mappings:

- Python compatibility fix: `collections.Iterable` -> `collections.abc.Iterable`.
- Python packaging metadata fix: replaced removed `get_installed_distributions()` with `importlib.metadata`.
- File-mode robustness: provenance output directory is now auto-created for `--provenance-path`.
- Processing robustness: safer handling in wrapper dispatch when `INPUT_NAME`/`OUTPUT_NAME` are missing.
- Compatibility fix for `process(...)`-based PEs: provenance dispatch now preserves and invokes original PE implementations (important for workflows like `dispel4py.examples.graph_testing.word_count` in `multi`/`timed_multi`).
- Save-mode bug fix: corrected assignment for file mode fallback.
- `timed_multi` + provenance on macOS: multiprocessing uses `fork` context when provenance is enabled (avoids dynamic class pickling failures under spawn).

## 3. Minimal Provenance Config File

Example `prov_config_example.json`:

```json
{
  "s-prov:save-mode": "file",
  "s-prov:workflowName": "word_count",
  "s-prov:workflowType": "dispel4py",
  "s-prov:workflowId": "urn:workflow:word_count",
  "s-prov:creator": "Rosa"
}
```

Required in practice:

- `s-prov:workflowName`
- `s-prov:workflowId`
- `s-prov:save-mode` (`file` or `service`)

Recommended:

- `s-prov:workflowType`
- `s-prov:creator`

User and run id are commonly set from CLI:

- `--provenance-userid`
- `--provenance-runid`

If `--provenance-userid` is omitted, user defaults to `anonymous`.

## 4. CLI Flags (Provenance)

Main activation flag:

- `--provenance-config <path-or-inline>`

Additional provenance options:

- `--provenance-path <dir>`: required for `s-prov:save-mode = file`
- `--provenance-repository-url <url>`: required for `s-prov:save-mode = service`
- `--provenance-export-url <url>`
- `--provenance-bulk-size <int>`
- `--provenance-runid <id>`
- `--provenance-userid <id>`
- `--provenance-bearer-token <jwt>`

## 5. Run Examples

## Simple mapping

```bash
dispel4py simple dispel4py.examples.graph_testing.word_count -i 10 \
  --provenance-config prov_config_example.json \
  --provenance-path prov_out \
  --provenance-userid rosa \
  --provenance-runid run_001
```

## Timed simple mapping

```bash
dispel4py timed_simple dispel4py.examples.graph_testing.word_count -i 10 \
  --provenance-config prov_config_example.json \
  --provenance-path prov_out \
  --provenance-userid rosa \
  --provenance-runid run_001 \
  --run-id 001 \
  --timing-dir timings
```

## Timed multi mapping

```bash
dispel4py timed_multi dispel4py.examples.graph_testing.word_count -i 10 -n 4 \
  --provenance-config prov_config_example.json \
  --provenance-path prov_out \
  --provenance-userid rosa \
  --provenance-runid run_001 \
  --run-id 001 \
  --timing-dir timings
```

Note: if you get `Graph is larger than job size`, increase `-n`.

## Timed MPI mapping

```bash
mpiexec -n 4 dispel4py timed_mpi dispel4py.examples.graph_testing.word_count -i 10 \
  --provenance-config prov_config_example.json \
  --provenance-path prov_out \
  --provenance-userid rosa \
  --provenance-runid run_001 \
  --run-id 001 \
  --timing-dir timings
```

MPI note: run id auto-generation is not supported for provenance in MPI mode; provide `--provenance-runid`.

## 6. File-Mode Output: `bulk_*`

With `s-prov:save-mode = file`, provenance is written to files such as:

- `prov_out/bulk_<hostname>-<pid>-<uuid>`

Important:

- File names do not include your logical run id.
- Use `runId` inside each JSON record to identify a run.
- Each `bulk_*` file contains a JSON list.
- With default bulk size (`1`), each file often contains one provenance record.

## 7. Record Types In `bulk_*`

## `workflow_run` record

Typical fields:

- `_id`
- `runId`
- `startTime`
- `username`
- `workflowId`
- `workflowName`
- `mapping`
- `type` (`workflow_run`)
- `modules` (installed package snapshot)
- `source` (workflow PE metadata)
- `prov:type`
- `status`

Purpose:

- provenance metadata for the overall workflow execution.

## `lineage` record

Typical fields:

- `_id`
- `type` (`lineage`)
- `runId`
- `name` (PE name)
- `instanceId`
- `iterationId`
- `iterationIndex`
- `startTime`, `endTime`
- `streams`
- `derivationIds`
- `mapping`
- `prov_cluster`
- `username`

Purpose:

- fine-grained lineage per PE invocation/iteration and data stream transfer.

Depending on workflow/mapping behavior, you may see mostly `workflow_run`, or both `workflow_run` and `lineage`.

## 8. Quick Inspection Commands

Count and preview bulk files:

```bash
ls -lh prov_out
```

Print `type` and `runId` from all records:

```bash
python3 - <<'PY'
import json, glob
for f in sorted(glob.glob('prov_out/bulk_*')):
    arr = json.load(open(f))
    for r in arr:
        if isinstance(r, dict):
            print(f, r.get('type'), r.get('runId'))
PY
```

Summarize record counts by type:

```bash
python3 - <<'PY'
import json, glob, collections
c = collections.Counter()
for f in glob.glob('prov_out/bulk_*'):
    for r in json.load(open(f)):
        if isinstance(r, dict):
            c[r.get('type', '<none>')] += 1
print(dict(c))
PY
```

## 9. Matching Timing And Provenance Runs

When using timed mappings and provenance together, use coordinated IDs:

- Provenance run id: `--provenance-runid run_001`
- Timing run id: `--run-id 001` (timing flag)

This makes post-run analysis and joins much easier.

Naming note:

- timing files are named with pattern `_run{timing_run_id}`.
- if you pass `--run-id run_001`, filenames become `..._runrun_001...`.
- prefer `--run-id 001` (or another id without leading `run`).

## 10. Troubleshooting

## `FileNotFoundError` for `prov_out/bulk_*`

- Ensure `--provenance-path` is set.
- In this codebase, path auto-creation is enabled; older installed versions may require manual `mkdir -p`.

## `ModuleNotFoundError` for timed mappings

- Ensure you are running the repo version (editable install):
  - `pip install -e .`

## No lineage records, only `workflow_run`

- Check whether the workflow actually produced stream events for the monitored PE path.
- Increase iterations and test another workflow (for example pipeline-style workflows) to validate lineage generation.

## `Graph is larger than job size` in multi mapping

- Increase `-n` (number of processes).

## 11. Recommended Workflow

1. Start with `timed_simple` + provenance (`file` mode).
2. Validate `prov_out/bulk_*` contains expected `runId`.
3. Move to `timed_multi`/`timed_mpi` with larger process counts.
4. Keep timing and provenance run ids coordinated for analysis.

## 12. Timing + Provenance Caveat

Running `timed_*` with provenance enabled is supported and useful, but:

- it is **not** a clean baseline-performance mode;
- provenance adds lineage capture overhead per iteration;
- multiprocessing behavior can differ by platform/runtime setup.

Recommended practice:

- use `timed_*` (without provenance) for baseline/scaling benchmarks;
- use `multi`/`mpi` + provenance for provenance validation;
- use `timed_*` + provenance when you explicitly want provenance-aware timing traces.
