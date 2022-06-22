import pytest

from expyre.resources import Resources


def test_cores(expyre_dummy_config):
    import expyre.config

    nodes = expyre.config.systems['_sys_default'].partitions

    assert Resources(max_time='1h', n=(16, 'cores')).find_nodes(nodes) == ('node16_1,node16_2', {'nnodes': 1, 'ncores': 16,
                                                                                                 'ncores_per_node': 16 })
    assert Resources(max_time='1h', n=(32, 'cores')).find_nodes(nodes) == ('node16_1,node16_2', {'nnodes': 2, 'ncores': 32,
                                                                                                 'ncores_per_node': 16 })
    assert Resources(max_time='1h', n=(36, 'cores')).find_nodes(nodes) == ('node36', {'nnodes': 1, 'ncores': 36,
                                                                                      'ncores_per_node': 36 })
    assert Resources(max_time='1h', n=(1, 'nodes')).find_nodes(nodes) == ('node16_1,node16_2', {'nnodes': 1, 'ncores': 16,
                                                                                                'ncores_per_node': 16 })
    assert Resources(max_time='1h', n=(1, 'nodes'), partitions='node_bigmem').find_nodes(nodes) == ('node_bigmem', {'nnodes': 1, 'ncores': 56,
                                                                                                                    'ncores_per_node': 56 })
    assert Resources(max_time='1h', n=(1, 'nodes'), partitions='.*bigmem').find_nodes(nodes) == ('node_bigmem', {'nnodes': 1, 'ncores': 56,
                                                                                                                 'ncores_per_node': 56 })

    try:
        r = Resources(max_time='1h', n=(17, 'cores')).find_nodes(nodes)
    except RuntimeError:
        pass

    assert Resources(max_time='1h', n=(17, 'cores')).find_nodes(nodes, exact_fit=False) == ('node16_1,node16_2', {'nnodes': 2, 'ncores': 32,
                                                                                                                  'ncores_per_node': 16 })
    assert Resources(max_time='1h', n=(36, 'cores')).find_nodes(nodes, exact_fit=False) == ('node36', {'nnodes': 1, 'ncores': 36,
                                                                                                       'ncores_per_node': 36 })
    assert Resources(max_time='1h', n=(71, 'cores')).find_nodes(nodes, exact_fit=False) == ('node36', {'nnodes': 2, 'ncores': 72,
                                                                                                       'ncores_per_node': 36 })

def test_cores_tmp(expyre_dummy_config):
    import expyre.config

    nodes = expyre.config.systems['_sys_default'].partitions

    assert Resources(max_time='1h', n=(17, 'cores')).find_nodes(nodes, exact_fit=False) == ('node16_1,node16_2', {'nnodes': 2, 'ncores': 32,
                                                                                                                  'ncores_per_node': 16 })

def test_mem(expyre_dummy_config):
    import expyre.config

    nodes = expyre.config.systems['_sys_default'].partitions

    assert Resources(max_time='1h', max_mem=('17gb', 'per_core'), n=(1, 'nodes')).find_nodes(nodes) == ('node_bigmem', {'nnodes': 1, 'ncores': 56,
                                                                                                                       'ncores_per_node': 56 })

    assert Resources(max_time='1h', max_mem=('1tb', 'tot'), n=(1, 'nodes')).find_nodes(nodes) == ('node_bigmem', {'nnodes': 1, 'ncores': 56,
                                                                                                                    'ncores_per_node': 56 })


def test_time(expyre_dummy_config):
    import expyre.config
    nodes = expyre.config.systems['_sys_timelimited'].partitions

    assert Resources(max_time='30m', n=(1, 'nodes')).find_nodes(nodes) == ('debug', {'nnodes': 1, 'ncores': 40,
                                                                                     'ncores_per_node': 40 })
    assert Resources(max_time='00:30', n=(1, 'nodes')).find_nodes(nodes) == ('debug', {'nnodes': 1, 'ncores': 40,
                                                                                       'ncores_per_node': 40 })
    assert Resources(max_time='1h', n=(1, 'nodes')).find_nodes(nodes) == ('debug', {'nnodes': 1, 'ncores': 40,
                                                                                    'ncores_per_node': 40 })
    assert Resources(max_time='1:05:00', n=(2, 'nodes')).find_nodes(nodes) == ('standard', {'nnodes': 2, 'ncores': 80,
                                                                                            'ncores_per_node': 40 })
    assert Resources(max_time='1:05:00', n=(80, 'cores')).find_nodes(nodes) == ('standard', {'nnodes': 2, 'ncores': 80,
                                                                                             'ncores_per_node': 40 })
    assert Resources(max_time='2-1:10:05', n=(1, 'nodes')).find_nodes(nodes) == ('standard', {'nnodes': 1, 'ncores': 40,
                                                                                              'ncores_per_node': 40 })
