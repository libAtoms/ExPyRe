.. _restarting:

############################################
Restarting an interrupted process
############################################


The simplest approach is to rely on the automated pickle/hash-based
identification of repeated jobs.  Unless the default
``try_restart_from_prev=True`` is overridden when calling ``ExPyRe(...)``
constructor, it will try to recreate the job if it seems identical to
a previous one.  

A job is identical if the hash of the pickles are identical
for all of the following:

- function
- input arguments (except those listed in ``hash_ignore``, for example any output-only arguments)
- input file names
- input file contents

The job will be recreated if the hash matches a job that
exists in the JobsDB and has status compatible with returning results
(i.e. not failed or cleaned.) "Processed" status means that the function
responsible for creating these jobs have already used the results, marked
jobs as "processed" and returned. These jobs will not be recreated and
files associated with them may be safely cleaned up. With this mechanism the
sequence of calling the restart is identical to that of the original calls.
The constructor will recreate the ``ExPyRe`` object, the ``start`` call will
submit it if necessary, and the ``get_results`` will sync remote files if needed and
return the unpickled results.

Another approach, applicable to jobs that have been started, is to
manually recreate them from the JobsDB database.  Syntax is

.. code-block:: python 

    db = expyre.config.db
    xprs = ExPyRe.from_jobsdb(db.jobs(name='task_1'))
    for xpr in xprs:
        res = xpr.get_results()
        # do something with res

The ``db.jobs(...)`` call can filter with regexps on ``status``, ``name``,
``id``, or ``system``.
