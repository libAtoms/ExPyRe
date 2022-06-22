import sys
import shutil
import subprocess
from pathlib import Path
import numpy as np
import pandas as pd

import click
from ..jobsdb import JobsDB
from ..func import ExPyRe
from .. import config


@click.group("expyre")
@click.option("--dbfile", help="Database file from here, rather than default")
@click.pass_context
def cli(ctx, dbfile):
    if dbfile is not None:
        config.db = JobsDB(dbfile)


def _get_jobs(**kwargs):
    kwargs = kwargs.copy()
    for k, v in list(kwargs.items()):
        if v is not None:
            kwargs[k] = [item.strip() for item in v.split(',')]

    return list(config.db.jobs(**kwargs, readable=True))


@cli.command("ls")
@click.option("--id", "-i", help="comma separated list of regexps for entire job id")
@click.option("--name", "-n", help="comma separated list of regexps for entire job name")
@click.option("--status", "-s", help="comma separated list of status values to include")
@click.option("--system", "-S", help="comma separated list of regexps for entire system name")
@click.option("--long-output", "-l", is_flag=True, help="long format output")
@click.pass_context
def cli_ls(ctx, id, name, status, system, long_output):
    """List jobs fitting criteria (default all jobs)
    """
    jobs = _get_jobs(id=id, name=name, status=status, system=system)

    if len(jobs) == 0:
        print(f"No matching jobs in JobsDB at {config.db.db_filename}")
    else:
        print("Jobs:")
        # {'id': 'vasp_eval_chunk_0_KEiSzsC7ft8ASaOmurO4yw5PhlCYVLgpAcVxVXvc1e0=_1kxwmf_c',
        # 'name': 'vasp_eval_chunk_0',
        # 'from_dir': '/home/cluster2/bernstei/src/work/Perovskites/ACE/_expyre/run_vasp_eval_chunk_0_KEiSzsC7ft8ASaOmurO4yw5PhlCYVLgpAcVxVXvc1e0=_1kxwmf_c',
        # 'status': 'started',
        # 'system': 'onyx',
        # 'remote_id': '1805770.pbs01',
        # 'remote_status': 'running',
        # 'creation_time': '2021-12-06 10:23:04',
        # 'status_time': '2021-12-06 10:25:42'}

        if long_output:
            headers = ['name', 'id', 'created']
        else:
            headers = ['id']
        headers += ['stat (time)', 'remote_id@sys', 'remote stat']
        if long_output:
            headers += ['from dir']

        rows = {f: [] for f in headers}
        for job in jobs:
            if long_output:
                rows['name'].append(f"{job['name']}")
                rows['id'].append(f"{job['id']}")
                rows['created'].append(f"{job['creation_time']}")
            else:
                rows['id'].append(f"{job['id'][0:len(job['name'])+10]}...")
            rows['stat (time)'].append(f"{job['status']}({job['status_time']})")
            rows['remote_id@sys'].append(f"{job['remote_id']}@{job['system']}")
            rows['remote stat'].append(f"{job['remote_status']}")
            if long_output:
                rows['from dir'].append(f"{job['from_dir']}")

        d = pd.DataFrame.from_dict(rows)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_colwidth', None)
        pd.set_option('display.width', None)
        pd.set_option('display.colheader_justify', 'left')
        print(d)


@cli.command("rm")
@click.option("--id", "-i", help="comma separated list of regexps for entire job id")
@click.option("--name", "-n", help="comma separated list of regexps for entire job name")
@click.option("--status", "-s", help="comma separated list of status values to include")
@click.option("--system", "-S", help="comma separated list of regexps for entire system name")
@click.option("--yes", "-y", is_flag=True, help="assume 'yes' for all confirmations, i.e. delete job and stage dirs without asking user")
@click.option("--clean", "-c", is_flag=True, help="delete local and remote stage directories")
@click.pass_context
def cli_rm(ctx, id, name, status, system, yes, clean):
    """Delete jobs fitting criteria (at least one criterion required)
    """
    if id is None and name is None and status is None and system is None:
        sys.stderr.write('At least one selection criterion is required\n\n')
        sys.stderr.write(ctx.get_help()+'\n')
        sys.exit(1)

    jobs = _get_jobs(id=id, name=name, status=status, system=system)

    for xpr in ExPyRe.from_jobsdb(jobs):
        if not yes:
            # ask user
            answer = None
            while answer not in ['y', 'n']:
                answer = input(f'Deleting "{xpr}"\nEnter n to reject or y to accept: ')
            if answer != 'y':
                sys.stderr.write(f'Not deleting "{xpr.id}"\n')
                sys.stderr.write('\n')
                continue

        if clean:
            xpr.cancel()
        xpr.clean(wipe=True, dry_run=not clean)
        config.db.remove(xpr.id)
        if not clean:
            sys.stderr.write('\n')


@cli.command("sync")
@click.option("--id", "-i", help="comma separated list of regexps for entire job id")
@click.option("--name", "-n", help="comma separated list of regexps for entire job name")
@click.option("--status", "-s", help="comma separated list of status values to include, or '*' for all", default='ongoing')
@click.option("--system", "-S", help="comma separated list of regexps for entire system name")
@click.pass_context
def cli_sync(ctx, id, name, status, system):
    """Sync remote status and results for jobs fitting criteria (at least one criterion required)
    """
    if status == '*':
        status = None

    jobs = _get_jobs(id=id, name=name, status=status, system=system)

    ExPyRe.sync_remote_results_status_ll(jobs, cli=True)


@cli.command("db_unlock")
@click.pass_context
def cli_db_unlock(ctx):
    """Unlock the database if it is stuck in a locked state

    This may happen if a process crashes during database access.
    Note that if another process is active and trying to access database,
    its behavior may be inconsistent.
    """

    config.db.unlock()
