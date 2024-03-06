.. _config:

#################################
Scheduler Configuration file
#################################

The description of the available systems and associated resources is in a ``config.json`` file.

**********************************
File format
**********************************


The top level structure is a single dict with one ``"systems"`` key with a dict value, containing
one key-value pair for each system. The key is the name of the system (used to select
the system when evaluating the function).  See ``expyre.System`` docstring for full list. The
value is a dict with the following keys:

- ``"host"``: string hostname, with optional ``username@`, or ``null`` for localhost without ssh.
- ``"scheduler"``: string indicating type of scheduler, currently ``"slurm"``, ``"pbs"`` or ``"sge"``
- ``"commands"``: optional list(str) with commands to run at beginning of every script on system, usually for things that set up the runtime such as environment modules (e.g. ``module load vasp``)
- ``"header"``: list(str) queuing system header lines.  Actual header is created by applying string formatting, i.e.  ``str.format(**node_dict)``, replacing substrings such as ``{num_nodes}``. Normally added to default header which sets job name, max time, node number, and stdout/stderr files.
- ``rundir``: str, optional, default ``"run_expyre"``. Place for all remote files and remote execution. 
- ``no_default_header``: bool, optional, default ``False``. Do not put default lines (such as ``#$ -cwd`` or automatically setting the job name) to the job submission script. 
- ``"remsh_cmd"``: optional string remote shell command, default ``"ssh"``
- ``"rundir"``: string for a path where the remote jobs should be run
- ``"partitions"`` or ``"queues"``: dict with partitions/queues/node-type names as keys and dict of node properties as values.

===========================
Node property dict includes
===========================

- ``"num_cores"``: int number of cores per node
- ``"max_time"``: max time, int for seconds, str for ``"<N>[smhd]"`` (case insensitive) or ``"<dd>-<hh>:<mm>:<ss>"``. Leading parts are optional, so N1:N2 is N1 minutes + N2 seconds.
- ``"max_mem"``: max mem per node, int for kB, str for ``"<N>[kmgt]b?"`` (case insensitive).

==========================
format() keys used in header
==========================

Available keys in ``node_dict`` which are normally used in this template:

- ``"num_nodes"``: int total number of nodes
- ``"num_cores"``: int total number of cores
- ``"num_cores_per_node"``: int number of cores per node
- ``"partition"``: name of partition (or queue), from ``partitions`` or ``queues`` dict keys,
  of specified explicitly in corresponding value dict

Note that if ``partial_node=True`` is passed to ``find_nodes`` and the total number of cores is less
than the number per node, ``num_cores`` and ``num_cores_per_node`` are *not* rounded up to an entire node.

Additional keys that are generally only used by the internally generated parts of the header:

- ``"id"``: str (supposed to be guaranteed to be unique among current jobs within project) job id
- ``"max_time_HMS"``: str max runtime in ``hours:minutes:seconds`` format


**********************************************
Configuration file location and project scope
**********************************************

The ``expyre`` root dir is the location of the jobs database and staging directories for each function call.
By setting this appropriately the namespace associated with different projects can be separated.

If the ``EXPYRE_ROOT`` env var is set, it is used as location for ``config.json`` as well as the ``expyre`` root.
If it is not set, then ``$HOME/.expyre`` is first read, then starting from ``$CWD/_expyre`` and going up
one directory at a time.  The expyre root is the directory closest to ``$CWD`` which contains a ``_expyre``
subdirectory.  The configuration consists of the content of ``$HOME/.expyre/config.json``, modified by any
entries that are present in ``<dir>/_expyre/config.json``
(with ``home_systems[sys_name].dict.update(local_systems[sys_name])`` on the dict associated with each
system).  New systems are added, and a system dict value of ``null`` removes the system.  Currently within
each system each key-value pair is overwritten, so you cannot directly disable one partition, only redefine
the entire ``"partitions"`` dict.


**********************************
Default header lines
**********************************

Below are the default headder lines added to every submission script. 
Values between curly brackets get filled in from `Resources` dictionary.

==================================
Slurm
==================================

.. code-block:: bash

    #SBATCH --job-name={id}
    #SBATCH --partition={partition}
    #SBATCH --time={max_time}
    #SBATCH --output=job.{id}.stdout
    #SBATCH --error=job.{id}.stderr


==================================
Sun Grid Engine
==================================

.. code-block:: bash

    #$ -N N_{id}
    #$ -q {partition}
    #$ -l h_rt={max_time}
    #$ -o job.{id}.stdout
    #$ -e job.{id}.stderr
    #$ -S /bin/bash
    #$ -r n
    #$ -cwd


==================================
PBS
==================================

.. code-block:: bash

    #PBS -N N_{id}
    #PBS -q {partition}
    #PBS -l walltime={max_time}
    #PBS -o job.{id}.stdout
    #PBS -e job.{id}.stderr
    #PBS -S /bin/bash
    #PBS -r n



***************************
config.json example
***************************

.. code-block:: json

    { "systems": {
        "local": { "host": "localhost",
            "remsh_cmd": "/usr/bin/ssh",
            "scheduler": "slurm",
            "commands": [ "module purge", "module load python/3 compilers/gnu lapack ase quip vasp" ],
            "header": ["#SBATCH --nodes={num_nodes}",
                       "#SBATCH --ntasks={num_cores}",
                       "#SBATCH --ntasks-per-node={num_cores_per_node}"],
            "partitions": { "node16_old,node16_new": { "num_cores" : 16, "max_time" : null, "max_mem" : "60GB" },
                            "node36":                { "num_cores" : 36, "max_time" : null, "max_mem" : "180GB" },
                            "node32":                { "num_cores" : 32, "max_time" : null, "max_mem" : "180GB" },
                            "node56_bigmem":         { "num_cores" : 56, "max_time" : "48:00:00", "max_mem" : "1500GB" },
                            "node72_bigmem":         { "num_cores" : 72, "max_time" : "48h", "max_mem" : "1500GB" }
            }
        }
    }

For this system:

- Connect with ``/usr/bin/ssh`` to localhost
- use slurm commands to submit jobs
- do some env mod stuff in each job before running task
- use built-in header for job name, partition, time, stdout/stderr, and + specified 3 lines to select number of nodes
- define 5 partitions (names in slurm ``--partition`` format), with varying numbers of cores, memory, and time limit
    on the two ``_bigmem`` ones (same time, specified in different formats as an example).
