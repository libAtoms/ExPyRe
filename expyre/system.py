import sys
import os
from pathlib import Path
import time

from .subprocess import subprocess_run, subprocess_copy
from .schedulers import schedulers
from . import util


class System:
    """Interface for a System that can run jobs remotely, including staging files from
    a local directory to a (config-specified) remote directory, submitting it with the correct
    kind of Scheduler, and staging back the results from the remote directory.  Does not report
    any other status directly.

    Parameters
    ----------
    host: str
        [username@]machine.fqdn
    partitions: dict
        dictionary describing partitions types
    scheduler: str, Scheduler
        type of scheduler
    header: list(str), optional
        list of batch system header to use in every job, typically for system-specific things
        like selecting nodes
    no_default_header: bool, default False
        do not automatically add default header fields, namely job name, partition/queue,
        max runtime, and stdout/stderr files
    script_exec: str, default '/bin/bash'
        executable for 1st line of script
    pre_submit_cmds: list(str), optional
        command to run in process that does submission before actual job submission
    commands: list(str), optional
        list of commands to run at start of every job on machine
    rundir: str / None, default 'run_expyre' if host is not None, else None
        path on remote machine to run in.  If absolute, used as is, and if relative, relative
        to (remote) home directory. If host is None, rundir is None means run directly
        in stage directory
    rundir_extra: str, default None
        extra string to add to remote_rundir, e.g. per-project part of path
    remsh_cmd: str, default EXPYRE_RSH or 'ssh'
        remote shell command to use with this system
    """
    def __init__(self, host, partitions, scheduler, header=[], no_default_header=False, script_exec='/bin/bash',
                 pre_submit_cmds=[], commands=[], rundir=None, rundir_extra=None, remsh_cmd=None):
        self.host = host

        self.remote_rundir = rundir
        if self.remote_rundir is None and self.host is not None:
            # set default for remote runs
            self.remote_rundir = 'run_expyre'
        if self.host is None and self.remote_rundir is not None and not Path(self.remote_rundir).is_absolute():
            # make local runs with non-absolute rundir relative to $HOME, mimicking behavior
            # of rsync with remote directory specifications
            self.remote_rundir = str(Path.home() / self.remote_rundir)

        if self.remote_rundir is not None:
            while self.remote_rundir.endswith('/'):
                self.remote_rundir = self.remote_rundir[:-1]
        if rundir_extra is not None and self.remote_rundir is not None:
            self.remote_rundir += '/' + rundir_extra
        self.partitions = partitions.copy() if partitions is not None else partitions
        self.queuing_sys_header = header.copy()
        self.no_default_header = no_default_header
        self.script_exec = script_exec
        self.pre_submit_cmds = pre_submit_cmds
        self.commands = commands.copy()
        self.remsh_cmd = util.remsh_cmd(remsh_cmd)
        self.initialized = False

        if isinstance(scheduler, str):
            self.scheduler = schedulers[scheduler](host, self.remsh_cmd)
        else:
            self.scheduler = scheduler(host)


    def run(self, args, script=None, shell='bash -c', retry=None, in_dir='_HOME_', dry_run=False, verbose=False):
        # like subprocess_run, but filling in host and remsh command from self
        return subprocess_run(self.host, args, script=script, shell=shell, remsh_cmd=self.remsh_cmd,
                              retry=retry, in_dir=in_dir, dry_run=dry_run, verbose=verbose)


    def initialize_remote_rundir(self, verbose=False):
        if self.initialized or self.remote_rundir is None:
            return

        self.run(['mkdir', '-p', str(self.remote_rundir)], verbose=verbose)
        self.initialized = True


    def _job_remote_rundir(self, stage_dir):
        return f'{self.remote_rundir}/{stage_dir.name}'


    def submit(self, id, stage_dir, resources, commands, header_extra=[], exact_fit=True, partial_node=False, verbose=False):
        """Submit a job on a remote machine, including staging out files

        Parameters
        ----------
        id: str
            unique id for job
        stage_dir: str, Path
            directory in which files have been prepared
        resources: Resources
            resources to use for job
        commands: list(str)
            commands to run in job script after per-machine commands
        header_extra: list(str), optional
            list of lines to append to system header for this job
        exact_fit: bool, default True
            only match partitions that have nodes with exact match to number of cores
        partial_node: bool, default False
            allow jobs that take less than an entire node

        Returns
        -------
        id of job on remote machine
        """
        if 'EXPYRE_TIMING_VERBOSE' in os.environ:
            sys.stderr.write(f'system {self.id} submit start {time.time()}\n')
        self.initialize_remote_rundir()

        partition, node_dict = resources.find_nodes(self.partitions, exact_fit=exact_fit,
                                                    partial_node=partial_node)
        # add partition-specific header after per-system header but before header specific
        # to this submission
        header_extra = self.partitions[partition].get("header", []) + header_extra
        # override default partition name from dict key (but after using dict key to look up
        # other things like header above)
        actual_partition = self.partitions[partition].get("partition", partition)

        commands = self.commands + commands

        stage_dir = Path(stage_dir)
        if self.remote_rundir is None:
            # no host, so run in stage dir to avoid needless copying
            job_remote_rundir = str(stage_dir)
        else:
            job_remote_rundir = self._job_remote_rundir(stage_dir)

            # make remote rundir, but fail if job-specific remote dir already exists
            self.run(['bash'],
                     script=f'if [ ! -d "{self.remote_rundir}" ]; then '
                            f'    echo "remote rundir \'{self.remote_rundir}\' does not exist" 1>&2; '
                             '    exit 1; '
                            f'elif [ -e "{job_remote_rundir}" ]; then '
                            f'    echo "remote job rundir \'{job_remote_rundir}\' already exists" 1>&2; '
                             '    exit 2; '
                             'else '
                            f'    mkdir -p "{job_remote_rundir}"; '
                             'fi', verbose=verbose)

            # stage out files
            # strip out final / from source path so that rsync creates stage_dir.name remotely under self.remote_rundir
            stage_dir_src = str(stage_dir)
            while stage_dir_src.endswith('/'):
                stage_dir_src = stage_dir_src[:-1]
            if 'EXPYRE_TIMING_VERBOSE' in os.environ:
                sys.stderr.write(f'system {self.id} submit start stage in {time.time()}\n')
            subprocess_copy(stage_dir_src, self.remote_rundir, to_host=self.host,
                            remsh_cmd=self.remsh_cmd, verbose=verbose)

        # submit job
        if 'EXPYRE_TIMING_VERBOSE' in os.environ:
            sys.stderr.write(f'system {self.id} submit start scheduler submit {time.time()}\n')
        try:
            r = self.scheduler.submit(id, str(job_remote_rundir), actual_partition,
                                      commands, resources.max_time, self.queuing_sys_header + header_extra,
                                      node_dict, no_default_header=self.no_default_header, script_exec=self.script_exec,
                                      pre_submit_cmds=self.pre_submit_cmds, verbose=verbose)
        except Exception:
            if self.remote_rundir is not None:
                sys.stderr.write(f'System.submit call to Scheduler.submit failed for job id {id}, '
                                 f'cleaning up remote dir {str(self.remote_rundir)}\n')
                self.run(['rm', '-r', str(job_remote_rundir)], verbose=verbose)
            raise

        if 'EXPYRE_TIMING_VERBOSE' in os.environ:
            sys.stderr.write(f'system {self.id} submit end {time.time()}\n')
        return r


    def get_remotes(self, local_dir, subdir_glob=None, delete=False, verbose=False):
        """get data from directories of remotely running jobs

        Parameters
        ----------
        local_dir: str
            local directory to stage to
        subdir_glob: str, list(str), default None
            only get subdirectories that much one or more globs
        delete: bool, default False
            delete local files that aren't in remote dir
        verbose: bool, default False
            verbose output
        """
        if self.remote_rundir is None:
            # nothing to "get" since this ran in stage dir
            return

        if subdir_glob is None:
            subdir_glob = '/*'
        elif isinstance(subdir_glob, str):
            subdir_glob = '/' + subdir_glob
        elif len(subdir_glob) == 1:
            subdir_glob = '/' + subdir_glob[0]
        else:
            subdir_glob = '/{' + ','.join(subdir_glob) + '}'

        subprocess_copy(self.remote_rundir + subdir_glob, local_dir, from_host=self.host,
                        remsh_cmd=self.remsh_cmd, delete=delete, verbose=verbose)



    def clean_rundir(self, stage_dir, filenames, dry_run=False, verbose=False):
        """clean a remote stage directory

        Parameters
        ----------
        stage_dir: str | Path
            local stage directory path
        files: list(str) or None
            list of files to replaced with 'CLEANED', or wipe entire directory if None
        verbose: bool, default False
            verbose output
        """
        if self.remote_rundir is None:
            # the job remote rundir _is_ the stage dir, so do not delete here
            return

        job_remote_rundir = self._job_remote_rundir(Path(stage_dir))

        if filenames is not None:
            filenames = ['"' + filename + '"' for filename in filenames]
            self.run(['bash'],
                     script=(f'for f in {" ".join(filenames)}; do\n'
                             f'    ff={job_remote_rundir}/$f\n'
                             f'    if [ -f $ff ]; then\n'
                             f'        echo "CLEANED" > $ff\n'
                             f'    fi\n'
                             f'done\n'), dry_run=dry_run, verbose=verbose)
        else:
            self.run(['bash'],
                     script="find " + str(job_remote_rundir) + " -type d -exec chmod u+rwx {} \\; ; rm -rf " + str(job_remote_rundir),
                     dry_run=dry_run, verbose=verbose)


    def __str__(self):
        s = f'System: host {self.host} rundir {self.remote_rundir} scheduler {type(self.scheduler).__name__}\n'
        if self.partitions is not None:
            s += ' ' + ' '.join(self.partitions.keys())
        return s
