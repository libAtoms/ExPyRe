from pathlib import Path
from expyre.subprocess import FailedSubprocessWarning

import pytest

def test_qsub_failure_atomic(expyre_config, monkeypatch):
    from expyre.config import systems

    for sys_name in systems:
        if sys_name.startswith('_'):
            continue

        do_qsub_failure_atomic(expyre_config, sys_name, monkeypatch)


def do_qsub_failure_atomic(expyre_config, sys_name, monkeypatch):
    # make sure that a failed qsub cleans up the remote running directory

    # WARNING: this test will only work if it is being run on same machine as job
    # was executed on (i.e. not really remote) because it is manually looking in remote_rundir

    from expyre.config import systems
    from expyre.resources import Resources
    from expyre.func import ExPyRe
    from expyre.subprocess import subprocess_run

    # mess up queuing system
    system = systems[sys_name]
    system.queuing_sys_header.append('#SBATCH FAIL')
    system.queuing_sys_header.append('#PBS FAIL')
    system.queuing_sys_header.append('#$ FAIL')

    monkeypatch.setenv('EXPYRE_RETRY', '1 0')

    xpr = ExPyRe('test', function=sum, args=[[1, 2, 3]])
    try:
        with pytest.warns(FailedSubprocessWarning):
            xpr.start(resources=Resources(num_nodes=1, max_time='5m'), system_name=sys_name)
        raise Exception
    except RuntimeError:
        print('ExPyRe.start failed, checking state of remote dir')

    if system.remote_rundir is not None:
        # make sure remote rundir exists
        stdout, stderr = system.run(['ls', '-d', f'{system.remote_rundir}'])
        assert system.remote_rundir in stdout

        # make sure job remote rundir does not exist
        stdout, stderr = system.run(['ls', f'{system.remote_rundir}'])
        assert xpr.id not in stdout
