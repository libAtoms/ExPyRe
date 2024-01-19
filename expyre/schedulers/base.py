import os

from ..subprocess import subprocess_run
from .. import util


class Scheduler:
    def __init__(self, host, remsh_cmd=None):
        """Create Scheduler object.  [NEED MORE INFO ABOUT HOW SCRIPTS WILL BE SET UP, THEIR ENVIRONMENT, ETC]

        Parameters
        ----------
        host: str
            username and host for ssh/rsync username@machine.fqdn
        """
        self.host = host
        self.hold_command = None
        self.release_command = None
        self.cancel_command = None
        self.remsh_cmd = util.remsh_cmd(remsh_cmd)


    def submit(self, id, remote_dir, partition, commands, max_time, header, node_dict, no_default_header=False,
               script_exec="/bin/bash", pre_submit_cmds=[], verbose=False):
        raise RuntimeError('Not implemented')


    def status(self, remote_ids, verbose=False):
        raise RuntimeError('Not implemented')


    def hold(self, remote_ids, verbose=False):
        """hold remote job

        Parameters
        ----------
        remote_ids: str, list(str)
            remote ids of jobs to hold
        """
        if isinstance(remote_ids, str):
            remote_ids = [remote_ids]

        subprocess_run(self.host, args=self.hold_command + remote_ids, remsh_cmd=self.remsh_cmd, verbose=verbose)


    def release(self, remote_ids, verbose=False):
        """release remote job

        Parameters
        ----------
        remote_ids: str, list(str)
            remote ids of jobs to hold
        """
        if isinstance(remote_ids, str):
            remote_ids = [remote_ids]

        subprocess_run(self.host, args=self.release_command + remote_ids, remsh_cmd=self.remsh_cmd, verbose=verbose)


    def cancel(self, remote_ids, verbose=False):
        """cancel remote job

        Parameters
        ----------
        remote_ids: str, list(str)
            remote ids of jobs to hold
        """
        if isinstance(remote_ids, str):
            remote_ids = [remote_ids]

        subprocess_run(self.host, args=self.cancel_command + remote_ids, remsh_cmd=self.remsh_cmd, verbose=verbose)


    @staticmethod
    def unset_scheduler_env_vars(prefix):
        unset_cmds = []
        if 'WFL_SCHEDULER_IGNORE_ENV' in os.environ:
            for v in os.environ:
                if v.startswith(prefix + '_'):
                    unset_cmds += ['unset', f'{v}', '&&']
        return unset_cmds


    @staticmethod
    def node_dict_env_var_commands(node_dict):
        # set env vars for node_dict, with max flexibility in case only some are known at submit time

        # EXPYRE_NUM_CORES_PER_NODE is defined by scheduler before these commands are run

        pre_commands = []

        # either num_nodes or num_cores must be known at submit time, so compute each in terms of the other
        if node_dict.get('num_nodes', None) is None:
            pre_commands.append('export EXPYRE_NUM_NODES=$(( {num_cores} / $EXPYRE_NUM_CORES_PER_NODE ))')
        else:
            pre_commands.append('export EXPYRE_NUM_NODES={num_nodes}')

        if node_dict.get('num_cores', None) is None:
            pre_commands.append('export EXPYRE_NUM_CORES=$(( {num_nodes} * $EXPYRE_NUM_CORES_PER_NODE ))')
        else:
            pre_commands.append('export EXPYRE_NUM_CORES={num_cores}')

        return pre_commands
