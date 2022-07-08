.. _environment_variables:

################################################################
Environment Variables
################################################################

At expyre runtime
================================================================

- ``EXPYRE_RETRY``: 'n t' number of tries ``n`` and wait time in seconds ``t`` if subprocess run fails, default '3 5'
- ``EXPYRE_ROOT``: override path to root directory for ``config.json``, ``JobsDB``, and stage directories
- ``EXPYRE_RSH``: default remote shell command if not specified in system configuration, overall default ``ssh``
- ``EXPYRE_SYS``: default system to start remote functions on, if not specified in call to ``ExPyRe.start()``
- ``EXPYRE_TIMING_VERBOSE``: print trace (to stderr) with timing info to determine what operation is taking time

Available in submitted job scripts
================================================================

- ``EXPYRE_NUM_NODES``
- ``EXPYRE_NUM_CORES``
- ``EXPYRE_NUM_CORES_PER_NODE``

Only for pytest
================================================================

- ``EXPYRE_PYTEST_SSH``: path to ssh to use (instead of ``/usr/bin/ssh``) in pytest ``test_subprocess.py`` (all higher level tests use ``remsh_cmd`` item for system in ``config.json``
- ``EXPYRE_PYTEST_SYSTEMS``: regexp to use to filter systems from those available in ``$HOME/.expyre/config.json``
- ``EXPYRE_PYTEST_QUEUED_JOB_RESOURCES``: JSON or filename with JSON that defines a dict with system names as keys and arrays as values. Each array has two dicts, each with Resources kwargs so that first one is a small job that runs for a substantial amount of time, and second one is large enough that it's guaranteed to be queued (not running) once the first has been submitted (e.g. using _all_ available nodes).
