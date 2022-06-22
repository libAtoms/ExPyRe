.. _unit_testing:

################################
Unit testing
################################

Unit tests use ``pytest`, in the ``tests`` subdirectory.  The only flag that's essentially required is ``--basetemp``, since many queuing systems won't work if you try to submit jobs from a temporary directory like ``/tmp``, which
`pytest`` uses by default, because those are not shared between the head node and compute nodes.  Pass ``--basetemp``
a directory under your home directory instead.  The ``--clean`` flag is recommended unless you are worried that something 
is so wrong that attempting to clean deleted jobs will delete the wrong thing.  The ``EXPYRE_PYTEST_SYSTEMS`` env var
is recommended unless you want all the remote tests to run on every remote system (useful to ensure that they are working,
but overkill if you changed functionality that's not computer or queuing system dependent). Example use:
```
env EXPYRE_PYTEST_SYSTEMS=tin pytest --clean --basetemp $HOME/pytest
```