################################################################
Basic use
################################################################

An example of where ExPyRe might be useful is a resourse-heavy step in a Python script, which would otherwhise be run on a ("local") laptop. To run that step on a ("remote") high-performance computer, one would need to: 

1. Write a script for the expensive step and potentially prepare inputs,
2. Copy script and inputs to the "remote" location, 
3. Write a submission script and specify there the resources to use (nodes, cores, time, etc.),
4. Submit the job, 
5. Monitor the job progress, 
6. Copy results to the "local" computer, 
7. Unpack results and run the rest of the Python script, 
8. Remove no longer useful files from "local" and "remote" locations. 

For ExPyRe to automatically manage these steps, one needs to

1. Specify hpc's queueing system in ``config.json`` (once ever), 
2. Wrap the resources-heavy part in ExPyRe functions (once per script), 
3. Clean the files with ``xpr`` command line utility function (once after running the script). 


Below is a minimal example of running a simple Numpy function remotely, with a breif explanation of options and behaviour. For more information, see the dedicated page.  


Original Python script:

.. code-block:: python 

    import numpy as np
    array_to_sum = np.ones((100, 50))
    array_sum = np.sum(array_to_sum, axis=1)
    assert np.max(np.abs(array_sum - [50.0] * 100)) == 0.0


And ExPyRe-modified script:

.. code-block:: python 

    import numpy as np
    from expyre.func import ExPyRe

    # input for function
    array_to_sum = np.ones((100, 50))

    # create a task to sum array over axis=1
    xpr = ExPyRe('test_task', function=np.sum, args=[array_to_sum], kwargs={'axis': 1})

    # resources: run for max 1 hour on 1 node, on a regular partition node
    res = {'max_time': '1h', 'num_nodes': 1, 'partitions': 'regular'}

    # submit job
    xpr.start(resources=res, system_name='remote')

    # gather results
    try:
        array_sum, stdout, stderr = xpr.get_results()
        assert np.max(np.abs(array_sum - [50.0] * 100)) == 0.0
    except TimeoutError:
        print('job did not run in alloted time')

    # mark as processed in jobs db in case of restarts
    xpr.mark_processed()

In addition, the following (modified appropriately) should be placed at ``~/.expyre/config.json``

.. code-block:: 

    { "systems": {
        "remtote": { "host": 'my-cluster',
                    "scheduler": "sge",
                    "header": ["#$ -pe smp {num_cores_per_node}"],
                    "partitions": {"regular" : {"num_cores": 16, "max_time" : "168h", "max_mem": "50GB"},
                                "large-mem" : {"num_cores": 32, "max_time": "168h", "max_mem": "200GB"},
            }
        }
    }

First, we create an ExPyRe object (``xpr``). Instead of calling a function directly, we need to specify the function, its arguments and keyword arguments separately for them to be pickled and stored in the local stage directory (``~/.expyre/...`` by default). Any files used by the function and some utility files are also stored there. This step also makes an entry in the jobs database (also at ``~/.expyre/jobs.db``), in preparation to tracking the remote jobs, so that the parent script can be restarted if interrupted. 

``xpr.start()`` starts the job on the remote machine. It copies files from the local stage directory to remote (``~/run_expyre/``, by default) via a passwordless ssh command to ``my-cluster`` (i.e. ``ssh my-cluster`` from "local" computer should work and login to the "remote" machine). The partition (or queue) is picked from the ones defined in ``~/.expyre/config.json``, based on the resources specified in ``res``. Scheduler-specific submission script is prepared and submitted on the "remote" computer and the status and details about the remote job are updated in the "local" ``jobs.db``. 

Based on all above, the Sun Grid Engine submission script has the following header: 

.. code-block:: bash

    #!/bin/bash -l
    #$ -pe smp 16
    #$ -N N_test_task_tkrT4KjgpTu6r-4NiIWq-CuXMmAuUbEdP6_s1uSAGiQ=_e0p0ywcz
    #$ -q regular
    #$ -l h_rt=1:00:00
    #$ -o job.test_task_tkrT4KjgpTu6r-4NiIWq-CuXMmAuUbEdP6_s1uSAGiQ=_e0p0ywcz.stdout
    #$ -e job.test_task_tkrT4KjgpTu6r-4NiIWq-CuXMmAuUbEdP6_s1uSAGiQ=_e0p0ywcz.stderr
    #$ -S /bin/bash
    #$ -r n
    #$ -cwd


``xpr.get_results()`` periodically checks the queueing system for job's status and finally gathers the results, along with standard output and error. If the script is interrupted before the remote job has finished running, the whole Python script can be restarted: instead of creating a second identical instance, ``ExPyRe()`` will recognise the job already present in the jobs database and ``xpr.get_results()`` will resume waiting for the results or return if the remote job has meanwhile finished running. 

The final ``xpr.mark_processed()`` modifies the jobs.db entry. All remote and local files may be deleted with ``xpr rm -c -s processed``. 


