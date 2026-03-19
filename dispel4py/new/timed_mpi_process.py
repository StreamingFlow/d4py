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

from __future__ import annotations

import argparse
import os

from dispel4py.new import mpi_process
from dispel4py.new.timed_multi_process import (
    _capture_abstract_shape,
    _capture_concrete_shape,
    _default_run_id,
    _instrument_graph,
    _load_iteration_rows,
    _load_timing_rows,
    _persist_graph_figure,
    _persist_json,
    _print_abstract_shape,
    _print_concrete_shape,
    _safe_token,
    _write_instance_summary,
    _write_iteration_latency_summary,
    _write_iteration_summary,
    _write_pe_summary,
    add_timing_arguments,
)


def parse_args(args, namespace):
    parser = argparse.ArgumentParser(
        description="Submit a dispel4py graph to timed MPI processes.",
    )
    parser.add_argument(
        "-s",
        "--simple",
        help="force simple processing",
        action="store_true",
    )
    parser.add_argument(
        "-n",
        "--num_processes",
        help="number of MPI processes",
        action="store",
        type=int,
    )
    add_timing_arguments(parser)
    result, _remaining = parser.parse_known_args(args, namespace)
    return result


def process(workflow, inputs, args):
    if args.num_processes is None:
        args.num_processes = mpi_process.size

    run_id = None
    if mpi_process.rank == 0:
        run_id = _safe_token(args.timing_run_id or _default_run_id())
    args.timing_run_id = mpi_process.comm.bcast(run_id, root=0)
    os.makedirs(args.timing_dir, exist_ok=True)

    abstract_shape = None
    abstract_shape_path = None
    if mpi_process.rank == 0:
        abstract_shape = _capture_abstract_shape(workflow)
        abstract_shape_path = _persist_json(
            args,
            {"run_id": args.timing_run_id, **abstract_shape},
            args.shape_file,
            f"{args.timing_prefix}_shape_run{args.timing_run_id}.json",
        )
        if args.print_shape:
            _print_abstract_shape(abstract_shape)

    wrapped = _instrument_graph(workflow, args)
    if mpi_process.rank == 0:
        print(f"Timing monitor enabled for {wrapped} PEs (run_id={args.timing_run_id}).")

    result = mpi_process.process(workflow, inputs, args)
    mpi_process.comm.Barrier()

    if mpi_process.rank != 0:
        return result

    concrete_shape = _capture_concrete_shape(args)
    concrete_shape_path = None
    if concrete_shape is not None:
        concrete_shape_path = _persist_json(
            args,
            {"run_id": args.timing_run_id, **concrete_shape},
            args.concrete_shape_file,
            f"{args.timing_prefix}_concrete_shape_run{args.timing_run_id}.json",
        )
        if args.print_shape:
            _print_concrete_shape(concrete_shape)

    timing_rows = _load_timing_rows(args)
    iteration_rows = _load_iteration_rows(args)
    pe_summary_path = _write_pe_summary(args, timing_rows, iteration_rows)
    instance_summary_path = _write_instance_summary(args, timing_rows, iteration_rows)
    iteration_summary_path = _write_iteration_summary(args, iteration_rows)
    iteration_latency_summary_path = _write_iteration_latency_summary(args, iteration_rows)

    abstract_figure_path = None
    concrete_figure_path = None
    if not args.no_graph_figures:
        abstract_figure_path = _persist_graph_figure(
            args,
            abstract_shape,
            args.abstract_figure_file,
            f"{args.timing_prefix}_abstract_graph_run{args.timing_run_id}.png",
            "Abstract Workflow Graph",
        )
        if concrete_shape is not None:
            concrete_figure_path = _persist_graph_figure(
                args,
                concrete_shape,
                args.concrete_figure_file,
                f"{args.timing_prefix}_concrete_graph_run{args.timing_run_id}.png",
                "Concrete PE-Instance Graph",
            )

    print(f"Abstract workflow shape written to: {abstract_shape_path}")
    if concrete_shape_path:
        print(f"Concrete instance shape written to: {concrete_shape_path}")
    if pe_summary_path:
        print(f"Timing summary (per PE) written to: {pe_summary_path}")
    else:
        print("No timing files found to aggregate per PE.")
    if instance_summary_path:
        print(f"Timing summary (per instance) written to: {instance_summary_path}")
    else:
        print("No timing files found to aggregate per instance.")
    if iteration_summary_path:
        print(f"Timing summary (per iteration) written to: {iteration_summary_path}")
    else:
        print("No per-iteration timing files found to aggregate.")
    if iteration_latency_summary_path:
        print(
            f"Timing latency summary (from iterations) written to: {iteration_latency_summary_path}",
        )
    else:
        print("No per-iteration timing files found to compute latency summary.")
    if abstract_figure_path:
        print(f"Abstract workflow figure written to: {abstract_figure_path}")
    if concrete_figure_path:
        print(f"Concrete instance figure written to: {concrete_figure_path}")
    if (not args.no_graph_figures) and (not abstract_figure_path):
        print("Graph figures were not generated (matplotlib unavailable or graph empty).")

    return result
