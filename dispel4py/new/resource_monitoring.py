"""Process resource observations during PE.process calls (not PE-owned memory).

Objects with OS handles or threads are created only inside the executing worker.
No psutil.cpu_percent baseline or process-lifetime high-water mark is used.
"""
from __future__ import annotations

import math
import os
import socket
import threading
import time

IDENTITY_FIELDS = ["run_id", "mapping", "hostname", "pid", "resource_enabled"]
ITERATION_RESOURCE_FIELDS = IDENTITY_FIELDS + [
    "cpu_secs", "cpu_percent", "rss_before_bytes", "rss_after_bytes",
    "rss_delta_bytes", "rss_peak_observed_bytes", "rss_sample_count",
    "rss_sample_errors", "memory_sampling_interval_secs", "cpu_scope", "memory_scope",
]
SUMMARY_RESOURCE_FIELDS = [
    "run_id", "mapping", "process_ids", "process_count", "resource_enabled",
    "resource_count", "total_cpu_secs", "avg_cpu_secs", "cpu_percent",
    "rss_min_bytes", "rss_max_bytes", "rss_mean_endpoint_bytes",
    "rss_delta_mean_bytes", "rss_delta_min_bytes", "rss_delta_max_bytes",
    "rss_sample_count", "rss_sample_errors", "memory_sampling_interval_secs",
    "cpu_scope", "memory_scope",
]


def sampling_interval(value):
    """Argparse type: zero disables periodic sampling, not endpoint readings."""
    import argparse

    try:
        value = float(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("sampling interval must be a number") from exc
    if not math.isfinite(value) or value < 0:
        raise argparse.ArgumentTypeError("sampling interval must be finite and >= 0")
    return value


def check_resource_dependency():
    try:
        import psutil
    except ImportError as exc:
        raise RuntimeError(
            "CPU/memory monitoring requires psutil: python -m pip install psutil; "
            "or use --no-resource-monitoring for timing only."
        ) from exc
    return psutil


def identity(run_id, mapping, enabled):
    return dict(run_id=run_id, mapping=mapping, hostname=socket.gethostname(),
                pid=os.getpid(), resource_enabled=int(enabled))


class ResourceProbe:
    """One lazy, reusable sampler per executing PE instance.

    RSS is observed at call boundaries and, optionally, by a background thread.
    The thread sleeps on a condition between calls, and exits at postprocess.
    Sampling is best effort: the GIL/scheduling can delay it. Process CPU includes
    all local threads (including this small monitoring overhead), not children.
    """

    def __init__(self, interval=0.01):
        self.interval = sampling_interval(interval)
        self.process = check_resource_dependency().Process(os.getpid())
        self.condition = threading.Condition()
        self.active = False
        self.closed = False
        self.peak = 0
        self.samples = 0
        self.errors = 0
        self.generation = 0
        self.thread = None
        if self.interval:
            self.thread = threading.Thread(target=self._sample, daemon=True,
                                           name="d4py-rss-monitor")
            self.thread.start()

    def _rss(self):
        return self.process.memory_info().rss

    def _sample(self):
        with self.condition:
            while not self.closed:
                self.condition.wait_for(lambda: self.active or self.closed)
                if self.closed:
                    break
                generation = self.generation
                # A notification means a call ended/started; only sample on timeout.
                notified = self.condition.wait(timeout=self.interval)
                if not notified and self.active and self.generation == generation:
                    try:
                        self.peak = max(self.peak, self._rss())
                        self.samples += 1
                    except (OSError, self._psutil_error):
                        self.errors += 1

    @property
    def _psutil_error(self):
        return check_resource_dependency().Error

    def begin(self):
        rss = self._rss()
        with self.condition:
            if self.active:
                raise RuntimeError("Concurrent/reentrant calls on one PE are unsupported")
            self.generation += 1
            self.peak = rss
            self.samples = 1
            self.errors = 0
            self.active = True
            self.condition.notify_all()
        # Keep endpoint memory reads outside the CPU/wall-clock interval.
        return rss, time.perf_counter_ns(), time.process_time_ns()

    def end(self, start):
        cpu_end = time.process_time_ns()
        wall_end = time.perf_counter_ns()
        before, wall_start, cpu_start = start
        with self.condition:
            self.active = False
            self.condition.notify_all()
            after = self._rss()
            peak = max(self.peak, after)
            samples = self.samples + 1
            errors = self.errors
        wall = (wall_end - wall_start) / 1e9
        cpu = (cpu_end - cpu_start) / 1e9
        return wall, dict(
            cpu_secs=cpu, cpu_percent=100.0 * cpu / wall if wall > 0 else None,
            rss_before_bytes=before, rss_after_bytes=after,
            rss_delta_bytes=after - before, rss_peak_observed_bytes=peak,
            rss_sample_count=samples, rss_sample_errors=errors,
            memory_sampling_interval_secs=self.interval,
            cpu_scope="process_during_call", memory_scope="process_rss_during_call",
        )

    def close(self):
        with self.condition:
            self.active = False
            self.closed = True
            self.condition.notify_all()
        if self.thread is not None:
            self.thread.join()


def _number(row, key):
    value = row.get(key)
    return None if value in (None, "") else float(value)


def _joined(rows, key):
    return ";".join(sorted({str(r[key]) for r in rows if r.get(key) not in (None, "")}))


def resource_summary(rows, contexts=()):
    """Pool raw call observations; never sum RSS or average CPU percentages."""
    observed = [r for r in rows if _number(r, "cpu_secs") is not None]
    context = list(contexts) + list(rows)
    process_ids = sorted({f"{r['hostname']}:{r['pid']}" for r in context
                          if r.get("hostname") and r.get("pid") not in (None, "")})
    result = dict.fromkeys(SUMMARY_RESOURCE_FIELDS)
    result.update(
        run_id=_joined(context, "run_id"), mapping=_joined(context, "mapping"),
        process_ids=";".join(process_ids), process_count=len(process_ids),
        resource_enabled=_joined(context, "resource_enabled"), resource_count=len(observed),
    )
    if not observed:
        # Old timing-only traces and idle instances have no resource observations.
        return result
    cpu = sum(_number(r, "cpu_secs") for r in observed)
    wall = sum(float(r["iteration_secs"]) for r in observed)
    endpoints = [int(r[k]) for r in observed for k in ("rss_before_bytes", "rss_after_bytes")]
    deltas = [int(r["rss_delta_bytes"]) for r in observed]
    result.update(
        total_cpu_secs=cpu, avg_cpu_secs=cpu / len(observed),
        cpu_percent=100.0 * cpu / wall if wall > 0 else None,
        rss_min_bytes=min(endpoints),
        rss_max_bytes=max(int(r["rss_peak_observed_bytes"]) for r in observed),
        rss_mean_endpoint_bytes=sum(endpoints) / len(endpoints),
        rss_delta_mean_bytes=sum(deltas) / len(deltas),
        rss_delta_min_bytes=min(deltas), rss_delta_max_bytes=max(deltas),
        rss_sample_count=sum(int(r["rss_sample_count"]) for r in observed),
        rss_sample_errors=sum(int(r.get("rss_sample_errors") or 0) for r in observed),
        memory_sampling_interval_secs=_joined(observed, "memory_sampling_interval_secs"),
        cpu_scope="process_during_call", memory_scope="process_rss_during_call",
    )
    return result
