import os


def remsh_cmd(cmd):
    """Get remote command
    
    Parameters
    ----------
    cmd: str

    Returns
    -------
    cmd: str
        Command set as ``EXPYRE_RSH`` environment variable, ``"ssh"`` otherwise. 
    """
    if cmd is None:
        return os.environ.get('EXPYRE_RSH', 'ssh')
    else:
        return cmd
