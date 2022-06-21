import re

from .units import time_to_sec, mem_to_kB


class Resources:
    """Resources required for a task, including time, memory, cores/nodes, and particular
    partitions.  Mainly consists of code that selects appropriate partition from the list
    associated with each System.
    """

    def __init__(self, max_time, n, max_mem=None, partitions=None):
        """Create Resources object
        Parameters
        ----------
        max_time: int / str
            max time for job in sec (int) or time spec (str)
        n: (int, str)
            int number of cores or nodes to use and str 'cores' or 'nodes'
        max_mem_per_core: (int/str, str), default None
            max mem in kB (int) or memory spec (str) per 'per_core' or 'tot' (str)
        partitions: list(str), default None
            regexps for types of node that can be used
        """
        assert n[1] in ['nodes', 'cores']
        if max_mem is not None:
            assert max_mem[1] in ['per_core', 'tot']

        self.max_time = time_to_sec(max_time)
        self.n = n
        if max_mem is None:
            self.max_mem = None
        else:
            self.max_mem = (mem_to_kB(max_mem[0]), max_mem[1])
        self.partitions = partitions
        if isinstance(self.partitions, str):
            self.partitions = [self.partitions]


    def find_nodes(self, partitions, exact_fit=True, partial_node=False):
        """find a node type that accomodates requested resources
        Parameters
        ----------
        partitions: dict
            properties of available partitions
        exact_fit: bool, default True
            only return nodes that exactly satisfy the number of cores
        partial_node: bool, default False
            allow jobs that take less than one entire node, overrides exact_fit

        Returns
        -------
        partition: str
            name of partition selected
        dict: various quantities of node
            nnodes: int, total number of nodes needed
            ncores: int, total number of cores needed
            ncores_per_node: int, number of cores per node for selected nodes
        """
        selected_partitions = []

        if partial_node:
            exact_fit=False

        for partition, node_spec in partitions.items():
            nnodes, ncores = self._get_nnodes_ncores(node_spec) 

            if self.partitions is not None and all([re.search('^'+nt_re+'$', partition) is None for nt_re in self.partitions]):
                # wrong node type
                continue

            if node_spec['max_time'] is not None and self.max_time > node_spec['max_time']:
                # too much time
                continue

            if exact_fit and self.n[1] == 'cores' and self.n[0] % node_spec['ncores'] != 0:
                # wrong number of cores
                continue

            if self.max_mem is not None and node_spec['max_mem'] is not None:
                if ((self.max_mem[1] == 'per_core' and (self.max_mem[0] > node_spec['max_mem'] / node_spec['ncores'])) or
                    (self.max_mem[1] == 'tot'  and (self.max_mem[0] > node_spec['max_mem'] * nnodes))):
                    # too much memory
                    continue

            selected_partitions.append(partition)

        if len(selected_partitions) == 0:
            raise RuntimeError(f'Failed to find acceptable node type '
                               f'for {self} with exact_fit={exact_fit}')

        if len(selected_partitions) > 1:
            excess_cores = []
            for nt in selected_partitions:
                node_spec = partitions[nt]
                _, ncores = self._get_nnodes_ncores(node_spec) 
                excess_cores.append((node_spec['ncores'] - ncores % node_spec['ncores']) % node_spec['ncores'])

            try:
                # look for first one that matches exactly
                partition_i = excess_cores.index(0)
            except ValueError:
                # pick best among remaining
                max_extra = min(excess_cores)
                partition_i = excess_cores.index(max_extra)
            selected_partitions = [selected_partitions[partition_i]]

        partition = selected_partitions[0]

        nnodes, ncores = self._get_nnodes_ncores(partitions[partition])

        if partial_node:
            if ncores <= partitions[partition]['ncores']:
                # partial node
                ncores_per_node = ncores
            else:
                raise ValueError('partial_node only supported when it can be satisfied by 1 node')
        else:
            # entire nodes
            ncores_per_node = partitions[partition]['ncores']
            ncores = nnodes * ncores_per_node

        return partition, {'nnodes': nnodes, 'ncores': ncores, 'ncores_per_node': ncores_per_node}


    def _get_nnodes_ncores(self, node_spec):
        """ get totals numbers of nodes and cores for this task
        Parameters
        ----------
        node_spec: dict 
            node type, from partitions dict

        Returns
        -------
        nnodes, ncores: total number of sufficient nodes and cores 
        """
        if self.n[1] == 'nodes':
            # fill up requested # of nodes
            nnodes = self.n[0]
            ncores = nnodes * node_spec['ncores']
        elif self.n[1] == 'cores':
            # determine how many nodes are necessary
            ncores = self.n[0]
            nnodes = ncores // node_spec['ncores']
            if nnodes * node_spec['ncores'] < ncores:
                nnodes += 1
        else:
            raise ValueError(f'number of unknown quantity {self.n[1]}, not "nodes" or "cores"')

        return nnodes, ncores


    def __repr__(self):
        return (f'time={self.max_time} n={self.n} mem={self.max_mem} partitions={self.partitions}')
