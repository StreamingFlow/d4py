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
Timed enactment of dispel4py graphs using multiprocessing.

This mapping behaves like ``dispel4py multi`` and additionally:
1. wraps every PE with a processing-time monitor,
2. writes one timing CSV per PE/rank and per-iteration timing traces,
3. writes aggregated summaries per PE and per PE instance,
4. stores abstract and concrete graph shapes as JSON,
5. writes PNG figures for abstract and concrete graphs.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import glob
import io
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import networkx as nx

from dispel4py.new import multi_process
from dispel4py.new.monitoring import Timer


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _safe_token(value: Any) -> str:
    token = "".join(c if c.isalnum() or c in "._-" else "_" for c in str(value))
    return token.strip("._") or "unknown"


def _resolve_output_path(base_dir: str, file_name: str) -> str:
    if os.path.isabs(file_name):
        return file_name
    return os.path.join(base_dir, file_name)


def _rank_sort_token(rank: str) -> tuple[int, str]:
    try:
        return int(rank), ""
    except Exception:
        return 10**9, rank


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (percentile / 100.0) * (len(ordered) - 1)
    left = int(position)
    right = min(left + 1, len(ordered) - 1)
    weight = position - left
    return ordered[left] * (1.0 - weight) + ordered[right] * weight


def _latency_stats(values: list[float]) -> dict[str, float]:
    if not values:
        return {"min": 0.0, "p50": 0.0, "p95": 0.0, "max": 0.0}
    ordered = sorted(values)
    return {
        "min": ordered[0],
        "p50": _percentile(ordered, 50.0),
        "p95": _percentile(ordered, 95.0),
        "max": ordered[-1],
    }


class CsvProcessTimingPE:
    _LOCAL_ATTRS = {
        "baseObject",
        "_timing_dir",
        "_timing_prefix",
        "_timing_run_id",
        "times_total",
        "times_count",
        "iteration_times",
    }

    def __init__(self, base_object, timing_dir: str, timing_prefix: str, run_id: str):
        self.baseObject = base_object
        self._timing_dir = timing_dir
        self._timing_prefix = timing_prefix
        self._timing_run_id = run_id
        self.times_total = 0.0
        self.times_count = 0
        self.iteration_times = []

    def __getattr__(self, name):
        base_object = self.__dict__.get("baseObject")
        if base_object is None:
            raise AttributeError(name)
        return getattr(base_object, name)

    def __setattr__(self, name, value):
        if name in self._LOCAL_ATTRS or "baseObject" not in self.__dict__:
            object.__setattr__(self, name, value)
            return
        setattr(self.baseObject, name, value)

    def process(self, inputs):
        with Timer() as timer:
            result = self.baseObject.process(inputs)
        self.times_total += timer.secs
        self.times_count += 1
        self.iteration_times.append(timer.secs)
        return result

    def _timing_postprocess(self):
        pe_id = getattr(self, "id", "PE")
        rank = getattr(self, "rank", "NA")
        avg = (self.times_total / self.times_count) if self.times_count else 0.0

        os.makedirs(self._timing_dir, exist_ok=True)
        filename = (
            f"{self._timing_prefix}_{_safe_token(pe_id)}_rank{_safe_token(rank)}"
            f"_run{self._timing_run_id}.csv"
        )
        output_path = os.path.join(self._timing_dir, filename)
        write_header = not os.path.exists(output_path)

        with open(output_path, "a", newline="", encoding="utf-8") as output_file:
            writer = csv.writer(output_file)
            if write_header:
                writer.writerow(["pe_id", "rank", "count", "total_secs", "avg_secs"])
            writer.writerow([pe_id, rank, self.times_count, self.times_total, avg])

        iteration_file = (
            f"{self._timing_prefix}_iterations_{_safe_token(pe_id)}"
            f"_rank{_safe_token(rank)}_run{self._timing_run_id}.csv"
        )
        iteration_path = os.path.join(self._timing_dir, iteration_file)
        iteration_header = not os.path.exists(iteration_path)
        with open(iteration_path, "a", newline="", encoding="utf-8") as output_file:
            writer = csv.writer(output_file)
            if iteration_header:
                writer.writerow(
                    ["pe_id", "rank", "instance_id", "iteration_index", "iteration_secs"],
                )
            for index, value in enumerate(self.iteration_times, start=1):
                writer.writerow([pe_id, rank, f"{pe_id}@{rank}", index, value])

        try:
            self.log(f"Average processing time: {avg}")
        except Exception:
            print(f"{pe_id} (rank {rank}): Average processing time: {avg}")

    def postprocess(self):
        try:
            self.baseObject.postprocess()
        finally:
            self._timing_postprocess()


def _capture_abstract_shape(workflow) -> dict[str, Any]:
    nodes = []
    edges = []
    graph = nx.DiGraph()

    for node in workflow.graph.nodes():
        pe = node.get_contained_object()
        pe_id = getattr(pe, "id", str(pe))
        nodes.append(pe_id)
        graph.add_node(pe_id)

    for _source_node, _dest_node, edge_data in workflow.graph.edges(data=True):
        source_pe, dest_pe = edge_data["DIRECTION"]
        source_id = getattr(source_pe, "id", str(source_pe))
        dest_id = getattr(dest_pe, "id", str(dest_pe))
        graph.add_edge(source_id, dest_id)
        edges.append(
            {
                "from": source_id,
                "to": dest_id,
                "from_connection": edge_data["FROM_CONNECTION"],
                "to_connection": edge_data["TO_CONNECTION"],
            },
        )

    topological_order = []
    try:
        topological_order = list(nx.topological_sort(graph))
    except Exception:
        pass

    return {
        "nodes": sorted(set(nodes)),
        "edges": edges,
        "topological_order": topological_order,
    }


def _capture_concrete_shape(args) -> dict[str, Any] | None:
    processes = getattr(args, "_d4py_processes", None)
    output_mappings = getattr(args, "_d4py_output_mappings", None)
    if not processes or output_mappings is None:
        return None

    process_table = {}
    proc_to_pe = {}
    node_rows = []
    concrete_graph = nx.DiGraph()

    for pe_id, ranks in processes.items():
        rank_list = [int(rank) for rank in list(ranks)]
        process_table[pe_id] = rank_list
        for rank in rank_list:
            proc_to_pe[rank] = pe_id
            instance_id = f"{pe_id}@{rank}"
            node_rows.append({"instance_id": instance_id, "pe_id": pe_id, "rank": rank})
            concrete_graph.add_node(instance_id)

    edge_rows = []
    seen_edges = set()
    for source_rank, per_output in output_mappings.items():
        source_rank = int(source_rank)
        source_pe = proc_to_pe.get(source_rank, f"rank{source_rank}")
        source_instance = f"{source_pe}@{source_rank}"
        for output_name, targets in per_output.items():
            for input_name, communication in targets:
                comm_name = str(getattr(communication, "name", "shuffle"))
                for dest_rank in getattr(communication, "destinations", []):
                    dest_rank = int(dest_rank)
                    dest_pe = proc_to_pe.get(dest_rank, f"rank{dest_rank}")
                    dest_instance = f"{dest_pe}@{dest_rank}"
                    key = (
                        source_instance,
                        dest_instance,
                        output_name,
                        input_name,
                        comm_name,
                    )
                    if key in seen_edges:
                        continue
                    seen_edges.add(key)
                    concrete_graph.add_edge(source_instance, dest_instance)
                    edge_rows.append(
                        {
                            "from": source_instance,
                            "to": dest_instance,
                            "from_pe_id": source_pe,
                            "to_pe_id": dest_pe,
                            "from_rank": source_rank,
                            "to_rank": dest_rank,
                            "from_connection": output_name,
                            "to_connection": input_name,
                            "communication": comm_name,
                        },
                    )

    topological_order = []
    try:
        topological_order = list(nx.topological_sort(concrete_graph))
    except Exception:
        pass

    return {
        "processes": process_table,
        "nodes": sorted(node_rows, key=lambda row: row["instance_id"]),
        "edges": edge_rows,
        "topological_order": topological_order,
    }


def _persist_json(args, payload: dict[str, Any], output_file: str | None, default_name: str):
    path = _resolve_output_path(args.timing_dir, output_file or default_name)
    with open(path, "w", encoding="utf-8") as output:
        json.dump(payload, output, indent=2)
    return path


def _print_abstract_shape(shape_data: dict[str, Any]) -> None:
    print(f"Abstract nodes: {shape_data['nodes']}")
    edge_pairs = [(edge["from"], edge["to"]) for edge in shape_data["edges"]]
    print(f"Abstract edges: {edge_pairs}")
    if shape_data["topological_order"]:
        print(f"Abstract topological order: {shape_data['topological_order']}")
    else:
        print("Abstract topological order: unavailable")


def _print_concrete_shape(shape_data: dict[str, Any]) -> None:
    node_ids = [node["instance_id"] for node in shape_data["nodes"]]
    edge_pairs = [(edge["from"], edge["to"]) for edge in shape_data["edges"]]
    print(f"Concrete nodes: {node_ids}")
    print(f"Concrete edges: {edge_pairs}")
    if shape_data["topological_order"]:
        print(f"Concrete topological order: {shape_data['topological_order']}")
    else:
        print("Concrete topological order: unavailable")


def _instrument_graph(workflow, args) -> int:
    replacements = {}
    wrapped = 0
    for node in workflow.graph.nodes():
        base_pe = node.get_contained_object()
        wrapped_pe = CsvProcessTimingPE(
            base_pe,
            timing_dir=args.timing_dir,
            timing_prefix=args.timing_prefix,
            run_id=args.timing_run_id,
        )
        node.obj = wrapped_pe
        replacements[base_pe] = wrapped_pe
        wrapped += 1

    for _source, _dest, edge_data in workflow.graph.edges(data=True):
        source_pe, dest_pe = edge_data["DIRECTION"]
        edge_data["DIRECTION"] = (
            replacements.get(source_pe, source_pe),
            replacements.get(dest_pe, dest_pe),
        )

    workflow.objToNode = {
        replacements.get(pe, pe): node for pe, node in workflow.objToNode.items()
    }

    return wrapped


def _load_timing_rows(args) -> list[dict[str, Any]]:
    pattern = os.path.join(
        args.timing_dir,
        f"{args.timing_prefix}_*_rank*_run{args.timing_run_id}.csv",
    )
    timing_rows = []
    for timing_file in sorted(glob.glob(pattern)):
        if os.path.basename(timing_file).startswith(f"{args.timing_prefix}_iterations_"):
            continue
        with open(timing_file, newline="", encoding="utf-8") as input_file:
            reader = csv.DictReader(input_file)
            for row in reader:
                timing_rows.append(
                    {
                        "pe_id": row.get("pe_id", "unknown"),
                        "rank": str(row.get("rank", "NA")),
                        "count": int(row.get("count", 0)),
                        "total_secs": float(row.get("total_secs", 0.0)),
                    },
                )
    return timing_rows


def _load_iteration_rows(args) -> list[dict[str, Any]]:
    pattern = os.path.join(
        args.timing_dir,
        f"{args.timing_prefix}_iterations_*_rank*_run{args.timing_run_id}.csv",
    )
    iteration_rows = []
    for iteration_file in sorted(glob.glob(pattern)):
        with open(iteration_file, newline="", encoding="utf-8") as input_file:
            reader = csv.DictReader(input_file)
            for row in reader:
                iteration_rows.append(
                    {
                        "pe_id": row.get("pe_id", "unknown"),
                        "rank": str(row.get("rank", "NA")),
                        "instance_id": row.get("instance_id", "unknown"),
                        "iteration_index": int(row.get("iteration_index", 0)),
                        "iteration_secs": float(row.get("iteration_secs", 0.0)),
                    },
                )
    return iteration_rows


def _write_instance_summary(
    args,
    timing_rows: list[dict[str, Any]],
    iteration_rows: list[dict[str, Any]],
) -> str | None:
    if not timing_rows:
        return None

    grouped = defaultdict(lambda: {"total_count": 0, "total_secs": 0.0})
    for row in timing_rows:
        key = (row["pe_id"], row["rank"])
        grouped[key]["total_count"] += row["count"]
        grouped[key]["total_secs"] += row["total_secs"]
    iteration_grouped = defaultdict(list)
    for row in iteration_rows:
        key = (row["pe_id"], row["rank"])
        iteration_grouped[key].append(row["iteration_secs"])

    summary_file = (
        args.instance_summary_file
        or f"{args.timing_prefix}_instances_run{args.timing_run_id}.csv"
    )
    summary_path = _resolve_output_path(args.timing_dir, summary_file)

    with open(summary_path, "w", newline="", encoding="utf-8") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(
            [
                "pe_id",
                "rank",
                "instance_id",
                "total_count",
                "total_secs",
                "avg_secs",
                "min_secs",
                "p50_secs",
                "p95_secs",
                "max_secs",
            ],
        )
        ordered_keys = sorted(grouped, key=lambda item: (item[0], _rank_sort_token(item[1])))
        for pe_id, rank in ordered_keys:
            entry = grouped[(pe_id, rank)]
            avg = entry["total_secs"] / entry["total_count"] if entry["total_count"] else 0.0
            lat = _latency_stats(iteration_grouped.get((pe_id, rank), []))
            writer.writerow(
                [
                    pe_id,
                    rank,
                    f"{pe_id}@{rank}",
                    entry["total_count"],
                    entry["total_secs"],
                    avg,
                    lat["min"],
                    lat["p50"],
                    lat["p95"],
                    lat["max"],
                ],
            )

    return summary_path


def _write_iteration_summary(args, iteration_rows: list[dict[str, Any]]) -> str | None:
    if not iteration_rows:
        return None

    summary_file = (
        args.iteration_summary_file
        or f"{args.timing_prefix}_iteration_timings_run{args.timing_run_id}.csv"
    )
    summary_path = _resolve_output_path(args.timing_dir, summary_file)
    stats_by_instance = defaultdict(list)
    for row in iteration_rows:
        stats_by_instance[(row["pe_id"], row["rank"], row["instance_id"])].append(
            row["iteration_secs"],
        )

    ordered = sorted(
        iteration_rows,
        key=lambda row: (row["pe_id"], _rank_sort_token(row["rank"]), row["iteration_index"]),
    )
    with open(summary_path, "w", newline="", encoding="utf-8") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(
            [
                "pe_id",
                "rank",
                "instance_id",
                "iteration_index",
                "iteration_secs",
                "instance_p50_secs",
                "instance_p95_secs",
                "instance_max_secs",
            ],
        )
        for row in ordered:
            lat = _latency_stats(
                stats_by_instance[(row["pe_id"], row["rank"], row["instance_id"])],
            )
            writer.writerow(
                [
                    row["pe_id"],
                    row["rank"],
                    row["instance_id"],
                    row["iteration_index"],
                    row["iteration_secs"],
                    lat["p50"],
                    lat["p95"],
                    lat["max"],
                ],
            )

    return summary_path


def _write_iteration_latency_summary(args, iteration_rows: list[dict[str, Any]]) -> str | None:
    if not iteration_rows:
        return None

    summary_file = (
        args.iteration_latency_summary_file
        or f"{args.timing_prefix}_iteration_timings_summary_run{args.timing_run_id}.csv"
    )
    summary_path = _resolve_output_path(args.timing_dir, summary_file)

    grouped = defaultdict(list)
    for row in iteration_rows:
        grouped[(row["pe_id"], row["rank"], row["instance_id"])].append(row["iteration_secs"])

    with open(summary_path, "w", newline="", encoding="utf-8") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(
            [
                "pe_id",
                "rank",
                "instance_id",
                "iteration_count",
                "total_secs",
                "avg_secs",
                "min_secs",
                "p50_secs",
                "p95_secs",
                "max_secs",
            ],
        )
        ordered_keys = sorted(grouped, key=lambda item: (item[0], _rank_sort_token(item[1])))
        for pe_id, rank, instance_id in ordered_keys:
            values = grouped[(pe_id, rank, instance_id)]
            total = sum(values)
            count = len(values)
            avg = total / count if count else 0.0
            lat = _latency_stats(values)
            writer.writerow(
                [
                    pe_id,
                    rank,
                    instance_id,
                    count,
                    total,
                    avg,
                    lat["min"],
                    lat["p50"],
                    lat["p95"],
                    lat["max"],
                ],
            )

    return summary_path


def _write_pe_summary(
    args,
    timing_rows: list[dict[str, Any]],
    iteration_rows: list[dict[str, Any]],
) -> str | None:
    if not timing_rows:
        return None

    grouped = defaultdict(lambda: {"ranks": set(), "total_count": 0, "total_secs": 0.0})
    for row in timing_rows:
        pe_id = row["pe_id"]
        grouped[pe_id]["ranks"].add(str(row["rank"]))
        grouped[pe_id]["total_count"] += row["count"]
        grouped[pe_id]["total_secs"] += row["total_secs"]
    iteration_grouped = defaultdict(list)
    for row in iteration_rows:
        iteration_grouped[row["pe_id"]].append(row["iteration_secs"])

    summary_file = args.summary_file or f"{args.timing_prefix}_summary_run{args.timing_run_id}.csv"
    summary_path = _resolve_output_path(args.timing_dir, summary_file)

    with open(summary_path, "w", newline="", encoding="utf-8") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(
            [
                "pe_id",
                "rank_count",
                "ranks",
                "total_count",
                "total_secs",
                "avg_secs",
                "min_secs",
                "p50_secs",
                "p95_secs",
                "max_secs",
            ],
        )
        for pe_id in sorted(grouped):
            entry = grouped[pe_id]
            ordered_ranks = sorted(entry["ranks"], key=_rank_sort_token)
            avg = entry["total_secs"] / entry["total_count"] if entry["total_count"] else 0.0
            lat = _latency_stats(iteration_grouped.get(pe_id, []))
            writer.writerow(
                [
                    pe_id,
                    len(entry["ranks"]),
                    ";".join(ordered_ranks),
                    entry["total_count"],
                    entry["total_secs"],
                    avg,
                    lat["min"],
                    lat["p50"],
                    lat["p95"],
                    lat["max"],
                ],
            )

    return summary_path


def _persist_graph_figure(args, graph_data: dict[str, Any], output_file: str | None, default_name: str, title: str):
    try:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            import matplotlib

            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
    except Exception:
        return None

    graph = nx.DiGraph()
    for node in graph_data["nodes"]:
        node_id = node["instance_id"] if isinstance(node, dict) else str(node)
        graph.add_node(node_id)

    for edge in graph_data["edges"]:
        graph.add_edge(edge["from"], edge["to"])

    if graph.number_of_nodes() == 0:
        return None

    path = _resolve_output_path(args.timing_dir, output_file or default_name)
    width = max(8, min(20, graph.number_of_nodes() * 1.3))
    height = max(5, min(14, graph.number_of_nodes() * 0.9))

    plt.figure(figsize=(width, height))
    position = nx.spring_layout(graph, seed=7)
    nx.draw_networkx(
        graph,
        pos=position,
        with_labels=True,
        arrows=True,
        node_size=1500,
        node_color="#d9edf7",
        edge_color="#4f5b66",
        font_size=9,
    )
    plt.title(title)
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(path, dpi=180)
    plt.close()
    return path


def add_timing_arguments(parser):
    parser.add_argument(
        "--timing-dir",
        default="timings",
        help="directory for timing/shape/figure outputs",
    )
    parser.add_argument(
        "--timing-prefix",
        default="monitor",
        help="prefix for generated output files",
    )
    parser.add_argument(
        "--run-id",
        dest="timing_run_id",
        help="run identifier to include in output file names",
    )
    parser.add_argument(
        "--summary-file",
        help="path for aggregated timing summary per PE",
    )
    parser.add_argument(
        "--instance-summary-file",
        help="path for aggregated timing summary per PE instance",
    )
    parser.add_argument(
        "--iteration-summary-file",
        help="path for merged per-iteration timings across all PE instances",
    )
    parser.add_argument(
        "--iteration-latency-summary-file",
        help="path for latency summary derived from merged per-iteration timings",
    )
    parser.add_argument(
        "--shape-file",
        help="path for abstract workflow shape JSON",
    )
    parser.add_argument(
        "--concrete-shape-file",
        help="path for concrete PE-instance shape JSON",
    )
    parser.add_argument(
        "--abstract-figure-file",
        help="path for abstract workflow graph PNG",
    )
    parser.add_argument(
        "--concrete-figure-file",
        help="path for concrete PE-instance graph PNG",
    )
    parser.add_argument(
        "--no-graph-figures",
        action="store_true",
        help="disable PNG graph generation",
    )
    parser.add_argument(
        "--print-shape",
        action="store_true",
        help="print abstract and concrete graph shape details",
    )
    return parser


def parse_args(args, namespace):  # pragma: no cover
    parser = argparse.ArgumentParser(
        prog="dispel4py",
        description="Submit a dispel4py graph to timed multiprocessing.",
    )
    parser.add_argument(
        "-s",
        "--simple",
        help="force simple processing",
        action="store_true",
    )
    parser.add_argument(
        "-n",
        "--num",
        metavar="num_processes",
        required=True,
        type=int,
        help="number of processes to run",
    )
    add_timing_arguments(parser)

    result, _remaining = parser.parse_known_args(args, namespace)
    return result


def process(workflow, inputs, args):
    args.timing_run_id = _safe_token(args.timing_run_id or _default_run_id())
    os.makedirs(args.timing_dir, exist_ok=True)

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
    print(f"Timing monitor enabled for {wrapped} PEs (run_id={args.timing_run_id}).")

    result = multi_process.process(workflow, inputs, args)

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
