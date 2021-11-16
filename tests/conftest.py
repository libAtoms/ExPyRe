import sys
import os
import re

import pytest

from pathlib import Path
import shutil


################################################
# Skip of slow or remote execution tests
# code from Pytest documentation at:
# https://docs.pytest.org/en/latest/example/simple.html#control-skipping-of-tests-according-to-command-line-option
#################################################
def pytest_addoption(parser):
    parser.addoption(
        "--clean", action="store_true", default=False, help="really do tests that clean the stage and remote rundirs"
    )


@pytest.fixture
def clean(request):
    return request.config.getoption("--clean")


orig_remote_rundir = {}


@pytest.fixture()
def expyre_dummy_config(tmp_path):

    expyre_root = Path(tmp_path / '.expyre')
    expyre_root.mkdir()
    shutil.copy(Path(__file__).parent / 'assets' / 'expyre_dummy_config.json', expyre_root / 'config.json')

    import expyre.config
    expyre.config.init(expyre_root, verbose=True)


@pytest.fixture()
def expyre_config(tmp_path):
    if not str(tmp_path).startswith(str(Path.home())):
        pytest.xfail(reason='expyre tests require tmp_path be under $HOME, pass "--basetemp $HOME/pytest"')

    # make a root directory, copy in config.json
    expyre_root = Path(tmp_path / '.expyre')
    expyre_root.mkdir()
    shutil.copy(Path.home() / '.expyre' / 'config.json', expyre_root / 'config.json')

    import expyre.config
    expyre.config.init(Path(tmp_path / '.expyre'), verbose=True)

    # set remote_rundir which now depends on tmp_path, and cannot be hardwired in the
    # config.json
    from expyre.subprocess import subprocess_run

    for sys_name in list(expyre.config.systems.keys()):
        if not re.search(os.environ.get('EXPYRE_PYTEST_SYSTEMS', ''), sys_name):
            sys.stderr.write(f'Not using {sys_name}, does not match regexp in EXPYRE_PYTEST_SYSTEMS\n')
            del expyre.config.systems[sys_name]
            continue

        system = expyre.config.systems[sys_name]

        if sys_name not in orig_remote_rundir:
            # save original so that if this is called more than once, path isn't prepended repeatedly
            orig_remote_rundir[sys_name] = system.remote_rundir

        if system.host is None:
            system.remote_rundir = str(tmp_path / f'pytest_expyre_rundir_{sys_name}' / orig_remote_rundir[sys_name])
        else:
            # NOTE: should make this something unique for each run
            system.remote_rundir = f'pytest_expyre_rundir_{sys_name}/' + str(Path(tmp_path).name) + '/' + orig_remote_rundir[sys_name]

        system.run(['mkdir', '-p', system.remote_rundir])


# from https://stackoverflow.com/questions/62044541/change-pytest-working-directory-to-test-case-directory
@pytest.fixture(scope="function")
def change_test_dir(request):
    os.chdir(request.fspath.dirname)
    yield
    os.chdir(request.config.invocation_dir)
