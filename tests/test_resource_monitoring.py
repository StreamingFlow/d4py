"""Focused checks for attribution, aggregation, resource semantics and lifecycle."""
import argparse
import copy
import csv
import os
import time
from types import SimpleNamespace

import pytest

from dispel4py.new.resource_monitoring import (
    ResourceProbe, resource_summary, sampling_interval,
)
from dispel4py.new.timed_multi_process import (
    CsvProcessTimingPE, _load_iteration_rows, _load_timing_rows,
    _prepare_monitoring_run, _write_instance_summary, _write_pe_summary,
)


def test_cpu_wait_and_memory_endpoints():
    probe = ResourceProbe(0)
    try:
        start = probe.begin()
        time.sleep(0.07)
        wall, wait = probe.end(start)
        start = probe.begin()
        target = time.process_time() + 0.04
        while time.process_time() < target:
            sum(range(1000))
        _, busy = probe.end(start)
        assert wall >= 0.06
        assert busy['cpu_secs'] >= 0.035
        assert wait['cpu_secs'] < busy['cpu_secs']
        assert wait['rss_sample_count'] == 2
        assert wait['rss_peak_observed_bytes'] >= wait['rss_before_bytes'] > 0
        assert wait['rss_delta_bytes'] == wait['rss_after_bytes'] - wait['rss_before_bytes']
    finally:
        probe.close()


def test_periodic_sampling_observes_transient_peak_and_resets(monkeypatch):
    probe = ResourceProbe(0.002)
    current_rss = [100]
    monkeypatch.setattr(probe, '_rss', lambda: current_rss[0])
    try:
        start = probe.begin()
        current_rss[0] = 1000
        # Wait for a real sampler observation, bounded rather than assuming scheduling.
        deadline = time.monotonic() + 2
        while probe.peak < 1000 and time.monotonic() < deadline:
            time.sleep(0.005)
        current_rss[0] = 120
        _, first = probe.end(start)
        assert first['rss_peak_observed_bytes'] == 1000
        assert first['rss_after_bytes'] == 120
        assert first['rss_sample_count'] > 2
        current_rss[0] = 100
        start = probe.begin()
        _, second = probe.end(start)
        assert second['rss_peak_observed_bytes'] == 100
    finally:
        probe.close()
    assert not probe.thread.is_alive()


def observation(cpu, wall, peak, pid=1):
    return dict(cpu_secs=cpu, iteration_secs=wall, rss_before_bytes=100,
                rss_after_bytes=120, rss_delta_bytes=20, rss_peak_observed_bytes=peak,
                rss_sample_count=3, rss_sample_errors=0, memory_sampling_interval_secs=0.01,
                run_id='test', mapping='timed_multi', hostname='host', pid=pid, resource_enabled=1)


def test_pooled_cpu_is_weighted_and_memory_not_summed():
    rows = [observation(1, 1, 200), observation(1, 3, 300, pid=2)]
    summary = resource_summary(rows)
    assert summary['total_cpu_secs'] == 2
    assert summary['cpu_percent'] == 50  # NOT mean(100%, 33.33%)
    assert summary['rss_max_bytes'] == 300  # NOT 200 + 300
    assert summary['rss_mean_endpoint_bytes'] == 110
    assert summary['process_count'] == 2
    assert summary['resource_count'] == 2


def test_missing_observations_are_not_zero_measurements():
    result = resource_summary([dict(iteration_secs=1)])
    assert result['resource_count'] == 0
    assert result['total_cpu_secs'] is None
    assert result['rss_max_bytes'] is None


@pytest.mark.parametrize('value', ['-1', 'nan', 'inf', '-inf', 'abc'])
def test_invalid_sampling_interval(value):
    with pytest.raises(argparse.ArgumentTypeError):
        sampling_interval(value)


class EchoPE:
    id = 'Echo0'
    rank = 3

    def process(self, data):
        return data

    def postprocess(self):
        pass


class ErrorPE(EchoPE):
    def process(self, data):
        raise ValueError('intentional PE error')


def test_wrapper_is_copyable_before_execution_and_preserves_results(tmp_path):
    wrapper = copy.deepcopy(CsvProcessTimingPE(EchoPE(), str(tmp_path), 'monitor', 'test'))
    assert wrapper._resource_probe is None
    payload = {'value': [1, 2, 3]}
    assert wrapper.process(payload) is payload
    probe = wrapper._resource_probe
    wrapper.postprocess()
    assert not probe.thread.is_alive()
    with next(tmp_path.glob('monitor_iterations_*.csv')).open() as source:
        row = next(csv.DictReader(source))
    assert row['instance_id'] == 'Echo0@3'
    assert int(row['pid']) == os.getpid()
    assert float(row['cpu_secs']) >= 0


def test_sampler_closes_when_pe_raises(tmp_path):
    wrapper = CsvProcessTimingPE(ErrorPE(), str(tmp_path), 'monitor', 'test')
    with pytest.raises(ValueError, match='intentional'):
        wrapper.process({})
    assert wrapper._resource_probe is None
    assert wrapper.times_count == 0


def test_idle_instance_has_zero_calls_and_unknown_resources(tmp_path):
    wrapper = CsvProcessTimingPE(EchoPE(), str(tmp_path), 'monitor', 'idle')
    wrapper.postprocess()
    with next(tmp_path.glob('monitor_Echo*.csv')).open() as source:
        row = next(csv.DictReader(source))
    assert row['count'] == '0'
    assert row['resource_count'] == '0'
    assert row['total_cpu_secs'] == ''
    assert row['rss_max_bytes'] == ''


def test_old_traces_still_aggregate_with_blank_resource_fields(tmp_path):
    (tmp_path / 'monitor_Echo0_rank3_runold.csv').write_text(
        'pe_id,rank,count,total_secs,avg_secs\nEcho0,3,2,4,2\n')
    (tmp_path / 'monitor_iterations_Echo0_rank3_runold.csv').write_text(
        'pe_id,rank,instance_id,iteration_index,iteration_secs\n'
        'Echo0,3,Echo0@3,1,1\nEcho0,3,Echo0@3,2,3\n')
    args = SimpleNamespace(timing_dir=str(tmp_path), timing_prefix='monitor', timing_run_id='old',
                           summary_file=None, instance_summary_file=None)
    totals, iterations = _load_timing_rows(args), _load_iteration_rows(args)
    for exporter in [_write_pe_summary, _write_instance_summary]:
        with open(exporter(args, totals, iterations)) as source:
            row = next(csv.DictReader(source))
        assert float(row['total_secs']) == 4
        assert float(row['p50_secs']) == 2
        assert row['total_cpu_secs'] == ''
        assert row['resource_count'] == '0'


def test_run_id_reuse_is_rejected(tmp_path):
    args = SimpleNamespace(timing_dir=str(tmp_path), timing_prefix='monitor', timing_run_id='test',
                           _monitor_mapping='timed_simple', no_resource_monitoring=True)
    _prepare_monitoring_run(args)
    with pytest.raises(FileExistsError, match='already exists'):
        _prepare_monitoring_run(args)
