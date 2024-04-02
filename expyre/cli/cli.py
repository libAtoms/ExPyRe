import sys
import shutil
import subprocess
import warnings
from pathlib import Path
import pickle

import numpy as np

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

        # add row numbers
        numbered_rows = {'': list(range(len(rows['id'])))}
        numbered_rows.update(rows)
        rows = numbered_rows

        # create formats
        widths = {col_name: max([len(str(val)) for val in [col_name] + col_vals]) for col_name, col_vals in rows.items()}
        fmt = " ".join([f'{{:>{w}}}' for w in widths.values()])
        fmt_header = " ".join([f'{{:<{w}}}' for w in widths.values()])

        # print
        print("Jobs:")
        print(fmt_header.format(*list(rows.keys())))
        for row_i in range(len(rows['id'])):
            print(fmt.format(*[rows[col_name][row_i] for col_name in rows.keys()]))


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
                answer = input(f'Delete "{xpr}"\nEnter n to reject or y to accept: ')
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
@click.option("--delete", "-d", is_flag=True, help="delete local files that aren't in remote dir, e.g. if remote job got an "
                                                   "error, was synced, but was then manually restarted and finished without an error")
@click.option("--verbose", "-v", is_flag=True, help="verbose output")
@click.pass_context
def cli_sync(ctx, id, name, status, system, delete, verbose):
    """Sync remote status and results for jobs fitting criteria (at least one criterion required)
    """
    if status == '*':
        status = None

    jobs = _get_jobs(id=id, name=name, status=status, system=system)

    if len(jobs) == 0:
        warnings.warn(f"sync found no jobs with status {status} to sync")

    ExPyRe._sync_remote_results_status_ll(jobs, cli=True, delete=delete, verbose=verbose)


@cli.command("db_unlock")
@click.pass_context
def cli_db_unlock(ctx):
    """Unlock the database if it is stuck in a locked state

    This may happen if a process crashes during database access.
    Note that if another process is active and trying to access database,
    its behavior may be inconsistent.
    """

    config.db.unlock()


@cli.command("reset_status")
@click.option("--id", "-i", help="comma separated list of regexps for entire job id")
@click.option("--name", "-n", help="comma separated list of regexps for entire job name")
@click.option("--status", "-s", help="comma separated list of status values to include, or '*' for all", default='*')
@click.option("--system", "-S", help="comma separated list of regexps for entire system name")
@click.argument("new_status", type=click.Choice(JobsDB.possible_status), required=True)
@click.pass_context
def cli_reset_status(ctx, id, name, status, system, new_status):
    """Reset local status of jobs (useful when jobs have been processed but you want to reset their
    status to something like 'started' to force syncing and processing to happen again, e.g. if you
    restarted some of them manually)
    """
    if status == '*':
        status = None

    jobs = _get_jobs(id=id, name=name, status=status, system=system)

    if len(jobs) == 0:
        warnings.warn(f"sync found no jobs with status {status} to reset")

    for job in jobs:
        config.db.update(job['id'], status=new_status)


@cli.command("create_job")
@click.option("--from_dir", "-d", type=click.Path(exists=True, file_okay=False, dir_okay=True,
                                                  path_type=Path),
              help="local stage directory of job to create", required=True)
@click.option("--system", "-S", help="system of job to create", required=True)
@click.option("--id", "-i", help="id of job to create, deduced from from_dir if not specified")
@click.option("--name", "-n", help="name of job to create, deduced from id if not specified")
@click.pass_context
def cli_reset_status(ctx, from_dir, system, id, name):
    """Create a job database entry manually
    """

    if id is None:
        id = from_dir.name[4:]
    if name is None:
        name = id[:-54]

    config.db.add(id=id, name=name, from_dir=str(from_dir.absolute()), status="created", system=system, remote_id="NA", remote_status="unknown")


@cli.command("fail_job")
@click.option("--id", "-i", help="comma separated list of regexps for entire job id")
@click.option("--name", "-n", help="comma separated list of regexps for entire job name")
@click.option("--status", "-s", help="comma separated list of status values to include, or '*' for all")
@click.option("--system", "-S", help="comma separated list of regexps for entire system name")
@click.option("--yes", "-y", is_flag=True, help="assume 'yes' for all confirmations, i.e. delete job and stage dirs without asking user")
@click.pass_context
def cli_fail_job(ctx, id, name, status, system, yes):
    """Mark a job as failed
    """
    if id is None and name is None and status is None and system is None:
        sys.stderr.write('At least one selection criterion is required\n\n')
        sys.stderr.write(ctx.get_help()+'\n')
        sys.exit(1)

    if status == '*':
        status = None

    jobs = _get_jobs(id=id, name=name, status=status, system=system)

    if len(jobs) == 0:
        warnings.warn(f"sync found no jobs with status {status} to reset")

    for job in jobs:
        if not yes:
            # ask user
            answer = None
            while answer not in ['y', 'n']:
                answer = input(f'Fail "{job["id"]}"\nEnter n to reject or y to accept: ')
            if answer != 'y':
                sys.stderr.write(f'Not failing "{job["id"]}"\n')
                sys.stderr.write('\n')
                continue

        from_dir = Path(job['from_dir'])

        with open(from_dir / "_expyre_job_error", "w") as fout:
            fout.write("xpr fail_job\n")
        with open(from_dir / "_expyre_job_exception", "wb") as fout:
            pickle.dump(RuntimeError("xpr fail_job"), fout)
        (from_dir / "_expyre_job_succeeded").unlink(missing_ok=True)
