import sys
import os
import time
import warnings

import pytest

from pathlib import Path

from expyre.resources import Resources

def test_system(tmp_path, expyre_config):
    from expyre.config import systems

    for sys_name, system in systems.items():
        if sys_name.startswith('_'):
            continue

        sys.stderr.write(f'Testing system {sys_name}\n')

        do_system(tmp_path, system, 'dummy_job_'+sys_name)


def do_system(tmp_path, system, job_name):
    stage_dir = tmp_path / ('stage_' + job_name)
    stage_dir.mkdir()

    assert not (stage_dir / 'out').exists()

    if system.host is not None:
        # May need to clean up remote stage dir.
        warnings.warn('If remote system still has stage_dir {stage_dir} from a previous run, this test will fail')

    # submit job
    remote_id = system.submit(job_name, stage_dir,
                           resources=Resources(num_nodes=int(os.environ.get('EXPYRE_PYTEST_MAX_NUM_NODES', 2)), max_time='5m'),
                           commands=['pwd', 'echo BOB > out', 'sleep 20'])

    # wait to finish
    print('remote_id', remote_id)
    status = system.scheduler.status(remote_id)
    while status[remote_id] != 'done':
        time.sleep(5)
        status = system.scheduler.status(remote_id)
        sys.stderr.write(f'status {status}\n')

    # make sure it's not failed
    assert status[remote_id] == 'done'

    system.get_remotes(tmp_path)

    # poke filesystem, since sometimes Path.exists on NFS fails even if file
    # exists when doing "ls"
    _ = list(stage_dir.iterdir())

    # make sure output file got staged back in, and has correct content
    assert (stage_dir / 'out').exists()

    with open(stage_dir / 'out') as fin:
        lines = fin.readlines()
    assert ['BOB'] == [l.strip() for l in lines]

