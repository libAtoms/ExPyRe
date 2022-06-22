import pytest

from expyre.resources import Resources


def test_cores(expyre_dummy_config):
    import expyre.config

    nodes = expyre.config.systems['_sys_default'].partitions

    assert Resources(max_time='1h', num_cores=16).find_nodes(nodes) == ('node16_1,node16_2', {'nnodes': 1, 'ncores': 16,
                                                                                                 'ncores_per_node': 16 })
    assert Resources(max_time='1h', num_cores=32).find_nodes(nodes) == ('node16_1,node16_2', {'nnodes': 2, 'ncores': 32,
                                                                                                 'ncores_per_node': 16 })
    assert Resources(max_time='1h', num_cores=36).find_nodes(nodes) == ('node36', {'nnodes': 1, 'ncores': 36,
                                                                                      'ncores_per_node': 36 })
    assert Resources(max_time='1h', num_nodes=1).find_nodes(nodes) == ('node16_1,node16_2', {'nnodes': 1, 'ncores': 16,
                                                                                                'ncores_per_node': 16 })
    assert Resources(max_time='1h', num_nodes=1, partitions='node_bigmem').find_nodes(nodes) == ('node_bigmem', {'nnodes': 1, 'ncores': 56,
                                                                                                                    'ncores_per_node': 56 })
    assert Resources(max_time='1h', num_nodes=1, partitions='.*bigmem').find_nodes(nodes) == ('node_bigmem', {'nnodes': 1, 'ncores': 56,
                                                                                                                 'ncores_per_node': 56 })

    try:
        r = Resources(max_time='1h', num_cores=17).find_nodes(nodes)
    except RuntimeError:
        pass

    assert Resources(max_time='1h', num_cores=17).find_nodes(nodes, exact_fit=False) == ('node16_1,node16_2', {'nnodes': 2, 'ncores': 32,
                                                                                                                  'ncores_per_node': 16 })
    assert Resources(max_time='1h', num_cores=36).find_nodes(nodes, exact_fit=False) == ('node36', {'nnodes': 1, 'ncores': 36,
                                                                                                       'ncores_per_node': 36 })
    assert Resources(max_time='1h', num_cores=71).find_nodes(nodes, exact_fit=False) == ('node36', {'nnodes': 2, 'ncores': 72,
                                                                                                       'ncores_per_node': 36 })

def test_ncores_and_nnodes(expyre_dummy_config):
    import expyre.config

    nodes = expyre.config.systems['_sys_default'].partitions

    try:
        _ = Resources(max_time='1h', num_nodes=1, num_cores=16).find_nodes(nodes)
    except ValueError:
        # mutually exclusive
        pass

    try:
        _ = Resources(max_time='1h').find_nodes(nodes)
    except ValueError:
        # one is required
        pass


def test_max_mem(expyre_dummy_config):
    import expyre.config

    nodes = expyre.config.systems['_sys_default'].partitions

    assert Resources(max_time='1h', max_mem_per_core='17gb', num_nodes=1).find_nodes(nodes) == ('node_bigmem', {'nnodes': 1, 'ncores': 56,
                                                                                                                       'ncores_per_node': 56 })

    assert Resources(max_time='1h', max_mem_tot='1tb', num_nodes=1).find_nodes(nodes) == ('node_bigmem', {'nnodes': 1, 'ncores': 56,
                                                                                                                    'ncores_per_node': 56 })

    try:
        _ = Resources(max_time='1h', num_nodes=1, max_mem_tot=1, max_mem_per_core=2).find_nodes(nodes)
    except ValueError:
        # mutually exclusive
        pass

def test_time(expyre_dummy_config):
    import expyre.config
    nodes = expyre.config.systems['_sys_timelimited'].partitions

    assert Resources(max_time='30m', num_nodes=1).find_nodes(nodes) == ('debug', {'nnodes': 1, 'ncores': 40,
                                                                                     'ncores_per_node': 40 })
    assert Resources(max_time='00:30', num_nodes=1).find_nodes(nodes) == ('debug', {'nnodes': 1, 'ncores': 40,
                                                                                       'ncores_per_node': 40 })
    assert Resources(max_time='1h', num_nodes=1).find_nodes(nodes) == ('debug', {'nnodes': 1, 'ncores': 40,
                                                                                    'ncores_per_node': 40 })
    assert Resources(max_time='1:05:00', num_nodes=2).find_nodes(nodes) == ('standard', {'nnodes': 2, 'ncores': 80,
                                                                                            'ncores_per_node': 40 })
    assert Resources(max_time='1:05:00', num_cores=80).find_nodes(nodes) == ('standard', {'nnodes': 2, 'ncores': 80,
                                                                                             'ncores_per_node': 40 })
    assert Resources(max_time='2-1:10:05', num_nodes=1).find_nodes(nodes) == ('standard', {'nnodes': 1, 'ncores': 40,
                                                                                              'ncores_per_node': 40 })

def test_partitions_queues(expyre_dummy_config):
    import expyre.config

    for sys_name in '_sys_timelimited', '_sys_queues':
        nodes = expyre.config.systems[sys_name].partitions

        # partitions exact str
        assert Resources(max_time='30m', num_nodes=1, partitions='debug').find_nodes(nodes) == ('debug', {'nnodes': 1, 'ncores': 40,
                                                                                                'ncores_per_node': 40 })
        # partitions exact str 1 item list
        assert Resources(max_time='30m', num_nodes=1, partitions=['debug']).find_nodes(nodes) == ('debug', {'nnodes': 1, 'ncores': 40,
                                                                                                  'ncores_per_node': 40 })
        # partitions exact str 2 item list
        assert Resources(max_time='30m', num_nodes=1, partitions=['standard', 'debug']).find_nodes(nodes) == ('debug', {'nnodes': 1, 'ncores': 40,
                                                                                                              'ncores_per_node': 40 })
        # partitions regexp list
        assert Resources(max_time='30m', num_nodes=1, partitions=['deb.*']).find_nodes(nodes) == ('debug', {'nnodes': 1, 'ncores': 40,
                                                                                                  'ncores_per_node': 40 })
        # queues
        assert Resources(max_time='30m', num_nodes=1, queues=['deb.*']).find_nodes(nodes) == ('debug', {'nnodes': 1, 'ncores': 40,
                                                                                              'ncores_per_node': 40 })
        # partitions failed str
        try:
            assert Resources(max_time='30m', num_nodes=1, partitions=['deb']).find_nodes(nodes) == ('debug', {'nnodes': 1, 'ncores': 40,
                                                                                                      'ncores_per_node': 40 })
        except RuntimeError:
            # no partition named just "deb"
            pass
