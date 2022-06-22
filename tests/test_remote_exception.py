import sys
import math

import pytest

from expyre.resources import Resources

def do_remote_exception(sys_name):
    from expyre import ExPyRe

    xpr = ExPyRe('test_exception', function=math.log, args=[-1.0])
    xpr.start(resources=Resources(num_nodes=1, max_time='1m'), system_name=sys_name)

    with pytest.raises(ValueError):
        results = xpr.get_results(check_interval=5)

def test_remote_exception(expyre_config):
    from expyre import config

    for sys_name in config.systems:
        if sys_name.startswith('_'):
            continue

        sys.stderr.write(f'Test remote exception job {sys_name}\n');

        do_remote_exception(sys_name)
