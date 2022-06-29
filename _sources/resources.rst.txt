.. _resources: 

##############
Resources
##############

Argument for ``ExPyRe.run()``. Specifies the resources to be allocated for each remotely submitted job. The fomrat is: 

- ``"max_time"``: maximum runtime. ``int`` for seconds, ``str`` for "<N>[smhd]" (case insensitive) or ``<dd>-<hh>:<mm>:<ss>`` (with leading parts optional, so N1:N2 is N1 minutes + N2 seconds)
- ``"n"``: ``tuple(int, str)``. ``int`` number of ``str`` ``"nodes"`` or ``"tasks"``. 
- ``"partitions"``: ``str`` or ``list(str)`` with regexps that match entire partition names (see section on  :ref:`Configuration <config>`)

Additional possible keys

- ``"max_mem_per_task"``: max memory per task. ``int`` for kB, ``str`` for "<N>[kmgt]b?" (case insensitive).
- ``"ncores_per_task"``: ``int`` cores per task


For clusters that do not operate in node-exclusive way (multiple jobs with a handful of cores each may run on the same node) it might be useful to set ``partial_node=True`` in ``Expyre.run()``. This allows to select partitions that wouldn't have all their cores filled by the submitted job. 