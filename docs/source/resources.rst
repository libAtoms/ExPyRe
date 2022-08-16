.. _resources: 

##############
Resources
##############

Argument for ``ExPyRe.start()``. Specifies the resources to be allocated for each remotely submitted job. The fomrat is: 

- ``"max_time"``: maximum runtime. ``int`` for seconds, ``str`` for "<N>[smhd]" (case insensitive) or ``<dd>-<hh>:<mm>:<ss>`` (with leading parts optional, so N1:N2 is N1 minutes + N2 seconds)
- ``"num_nodes"``: ``int`` number of nodes (mutually exclusive with ``num_cores``)
- ``"num_cores"``: ``int`` number of cores (mutually exclusive with ``num_nodes``)
- ``"partitions"``: ``str`` or ``list(str)`` with regexps that match entire partition names (see section on  :ref:`Configuration <config>`)

Additional possible keys

- ``"max_mem_tot"``: max memory for entire job. ``int`` for kB, ``str`` for "<N>[kmgt]b?" (case insensitive) (mutually exclusive with ``max_mem_per_core``)
- ``"max_mem_per_core"``: max memory per core. ``int`` for kB, ``str`` for "<N>[kmgt]b?" (case insensitive). (mutually exclusive with ``max_mem_tot``)


For clusters that do not operate in node-exclusive way (multiple jobs with a handful of cores each may run on the same node) it might be useful to set ``partial_node=True`` in ``Expyre.start()``. This allows to select partitions that wouldn't have all their cores filled by the submitted job. 
