"""Execute real mappings. Set D4PY_TEST_MPI=1 on a machine with working MPI."""
import csv
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
MODULE = 'dispel4py.examples.graph_testing.resource_monitoring_demo'


def read_rows(path):
    with path.open(newline='') as source:
        return list(csv.DictReader(source))


def run_cli(command, timeout=45):
    # Clean up workers if an execution fails or gets stuck.
    kwargs = {'start_new_session': True} if os.name == 'posix' else {}
    process = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True, **kwargs)
    try:
        output, _ = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        if os.name == 'posix':
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
        output, _ = process.communicate()
        pytest.fail('Mapping timed out:\n' + output)
    assert process.returncode == 0, output
    assert 'Traceback' not in output, output
    return output


@pytest.mark.parametrize('mapping', ['timed_simple', 'timed_multi', 'timed_mpi'])
def test_mapping_resources_and_aggregation(mapping, tmp_path):
    launcher = []
    if mapping == 'timed_mpi':
        if os.environ.get('D4PY_TEST_MPI') != '1':
            pytest.skip('Set D4PY_TEST_MPI=1 to run on a working MPI installation')
        executable = shutil.which('mpiexec') or shutil.which('mpirun')
        assert executable, 'mpiexec/mpirun is required'
        launcher = [executable, '-n', '4']
    command = launcher + [sys.executable, '-m', 'dispel4py.new.processor', mapping, MODULE,
                          '-i', '4', '--timing-dir', str(tmp_path), '--run-id', 'test',
                          '--no-graph-figures']
    if mapping == 'timed_multi':
        command += ['-n', '4']
    run_cli(command)
    instances = read_rows(tmp_path / 'monitor_instances_runtest.csv')
    pes = read_rows(tmp_path / 'monitor_summary_runtest.csv')
    calls = read_rows(tmp_path / 'monitor_iteration_timings_runtest.csv')
    assert len(instances) == (3 if mapping == 'timed_simple' else 4)
    assert len(pes) == 3
    assert len(calls) == 12
    assert sum(int(r['total_count']) for r in instances) == 12
    pids = {(r['hostname'], r['pid']) for r in calls}
    assert len(pids) == (1 if mapping == 'timed_simple' else 4)
    for row in calls:
        assert row['mapping'] == mapping
        assert float(row['cpu_secs']) >= 0
        assert int(row['rss_peak_observed_bytes']) >= int(row['rss_before_bytes']) > 0
        assert int(row['rss_peak_observed_bytes']) >= int(row['rss_after_bytes'])
        assert int(row['rss_sample_count']) >= 2
        assert int(row['rss_delta_bytes']) == int(row['rss_after_bytes']) - int(row['rss_before_bytes'])
    for abstract in pes:
        workers = [r for r in instances if r['pe_id'] == abstract['pe_id']]
        cpu = sum(float(r['total_cpu_secs']) for r in workers)
        assert float(abstract['total_cpu_secs']) == pytest.approx(cpu)
        wall = sum(float(r['total_secs']) for r in workers)
        assert float(abstract['cpu_percent']) == pytest.approx(100 * cpu / wall)
        assert int(abstract['rss_max_bytes']) == max(int(r['rss_max_bytes']) for r in workers)
        assert int(abstract['resource_count']) == 4
    sleep = next(r for r in pes if r['pe_id'].startswith('SleepPE'))
    busy = next(r for r in pes if r['pe_id'].startswith('BusyMemoryPE'))
    assert float(sleep['total_cpu_secs']) < float(busy['total_cpu_secs'])


@pytest.mark.parametrize('option', ['--no-resource-monitoring', '--memory-sampling-interval=0'])
def test_timing_only_and_endpoint_modes(option, tmp_path):
    run_cli([sys.executable, '-m', 'dispel4py.new.processor', 'timed_simple', MODULE,
             '-i', '2', '--timing-dir', str(tmp_path), '--run-id', 'test',
             '--no-graph-figures', option])
    calls = read_rows(tmp_path / 'monitor_iteration_timings_runtest.csv')
    assert len(calls) == 6
    for row in calls:
        assert float(row['iteration_secs']) >= 0
        if option == '--no-resource-monitoring':
            assert row['cpu_secs'] == ''
            assert row['rss_peak_observed_bytes'] == ''
            assert row['resource_enabled'] == '0'
        else:
            assert row['rss_sample_count'] == '2'
            assert row['memory_sampling_interval_secs'] == '0.0'


def test_unused_multi_instance_is_retained(tmp_path):
    run_cli([sys.executable, '-m', 'dispel4py.new.processor', 'timed_multi', MODULE,
             '-i', '1', '-n', '4', '--timing-dir', str(tmp_path), '--run-id', 'test',
             '--no-graph-figures'])
    instances = read_rows(tmp_path / 'monitor_instances_runtest.csv')
    busy = [r for r in instances if r['pe_id'].startswith('BusyMemoryPE')]
    assert len(busy) == 2
    idle = next(r for r in busy if r['total_count'] == '0')
    assert idle['resource_count'] == '0'
    assert idle['total_cpu_secs'] == ''
    assert idle['rss_max_bytes'] == ''


def test_partitioned_multi_keeps_leaf_pe_identity(tmp_path):
    run_cli([sys.executable, '-m', 'dispel4py.new.processor', 'timed_multi',
             'dispel4py.examples.graph_testing.pipeline_test', '-i', '3', '-n', '2', '-s',
             '--timing-dir', str(tmp_path), '--run-id', 'test', '--no-graph-figures'])
    instances = read_rows(tmp_path / 'monitor_instances_runtest.csv')
    assert len(instances) == 6
    assert {int(r['total_count']) for r in instances} == {3}
    assert len({r['process_ids'] for r in instances}) == 2
    assert all(r['total_cpu_secs'] != '' for r in instances)
