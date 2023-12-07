"""configuration for expyre from ``config.json``. Use env var ``EXPYRE_ROOT`` if set.
Otherwise (or if env var is set to ``@``), search from ``HOME`` (or ``/``, if current
dir is not below ``HOME``) down to current directory for ``.expyre`` or ``_expyre``.
Configuration is parsed in the same order, with deeper directories modifying previous
ones.  ``local_stage_dir`` is set to deepest directory unless it is set explicitly
in one of the found ``config.json`` files.

Global variables
----------------

local_stage_dir: str
    path of directory to stage jobs into
systems: dict
    dict of expyre.system.System that jobs can run on
db: JobsDB
    expyre.jobsdb.JobsDB database of jobs
"""
import sys
import os
import json
from pathlib import Path

from copy import deepcopy

def update_dict_leaves(d, d_loc):
    for k_loc, v_loc in d_loc.items():
        if v_loc == "_DELETE_" and k_loc in d:
            del d[k_loc]
        elif k_loc not in d:
            # create new
            d[k_loc] = deepcopy(v_loc)
        else:
            # override
            if isinstance(v_loc, dict):
                # dict, override leaves inside
                assert isinstance(d[k_loc], dict)
                update_dict_leaves(d[k_loc], v_loc)
            else:
                # not dict, overwrite
                d[k_loc] = deepcopy(v_loc)

# read config.json file from expyre root (~/.expyre or EXPYRE_ROOT),
# and save important parts (root, systems, db) as symbols in this module

def _get_config(root_dir, verbose=False):
    """get configuration from root dir
    """
    if root_dir == "@":
        if verbose: print("Searching directories")
        # search the path
        dirs = []
        cur_dir = Path.cwd()
        while True:
            if (cur_dir / ".expyre").exists():
                if (cur_dir / "_expyre").exists():
                    raise RuntimeError(f"Found both .expyre and _expyre in {cur_dir}")
                dirs.append(cur_dir / ".expyre")
            elif (cur_dir / "_expyre").exists():
                if (cur_dir / ".expyre").exists():
                    raise RuntimeError(f"Found both .expyre and _expyre in {cur_dir}")
                dirs.append(cur_dir / "_expyre")

            if cur_dir.parent == cur_dir or cur_dir.absolute() == Path.home().absolute():
                # reached ~ or /
                break

            cur_dir = cur_dir.parent
        dirs = list(reversed(dirs))
    else:
        if verbose: print("Exact directory", root_dir)
        root_dir = Path(root_dir)
        if not root_dir.is_dir():
            raise ValueError(f"expyre root {root_dir} is not a dir")
        dirs = [root_dir]

    if verbose: print("Using directories", dirs)

    config_data = {}
    for cur_dir in dirs:
        if verbose: print(f"Updating dict checking {cur_dir}")
        if (cur_dir / "config.json").exists():
            with open(cur_dir / "config.json") as fin:
                d_loc = json.load(fin)
                update_dict_leaves(config_data, d_loc)

    if len(config_data) == 0:
        raise FileNotFoundError('Failed to find any config.json file')

    # use explicitly specified local_stage_dir, otherwise deepest config dir
    local_stage_dir = Path(config_data.get("local_stage_dir", dirs[-1]))

    return local_stage_dir, config_data

local_stage_dir = None
systems = None
db = None


def init(root_dir, verbose=False):
    """Initializes ``root``, ``systems``, ``db``"""

    import os

    from .units import time_to_sec, mem_to_kB
    from .system import System
    from .jobsdb import JobsDB

    global local_stage_dir, systems, db

    try:
        local_stage_dir, _config_data = _get_config(root_dir, verbose=verbose)
    except FileNotFoundError:
        local_stage_dir = None
        systems = {}
        db = None
        return

    if local_stage_dir.name == '.expyre' or local_stage_dir.name == '_expyre':
        use_local_stage_dir = local_stage_dir.parent
    else:
        use_local_stage_dir = local_stage_dir
    _rundir_extra = os.environ.get('HOSTNAME', 'unkownhost') + '-' + str(use_local_stage_dir).replace('/', '_')

    systems = {}
    for _sys_name in _config_data['systems']:
        _sys_data = _config_data['systems'][_sys_name]
        if 'queues' in _sys_data:
            if 'partitions' in _sys_data:
                raise ValueError("config systems data contains both partitions and queues")
            _sys_data['partitions'] = _sys_data.pop('queues')
        if _sys_data['partitions'] is not None:
            for _partitions in _sys_data['partitions']:
                _sys_data['partitions'][_partitions]['max_time'] = time_to_sec(_sys_data['partitions'][_partitions]['max_time'])
                _sys_data['partitions'][_partitions]['max_mem'] = mem_to_kB(_sys_data['partitions'][_partitions]['max_mem'])
        systems[_sys_name] = System(rundir_extra=_rundir_extra, **_sys_data)

    db = JobsDB(local_stage_dir / 'jobs.db')
    if verbose:
        sys.stderr.write(f'expyre config got systems {list(systems.keys())}\n')


if 'pytest' not in sys.modules:
    init(os.environ.get("EXPYRE_ROOT", "@"))
