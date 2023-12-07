import pytest

from pathlib import Path
from copy import deepcopy
import json
import shutil


def test_update_dict_leaves(expyre_dummy_config):
    from expyre.config import update_dict_leaves

    d = {1: 2, 3: 4, 5: ["a", "b"], "d": {1: 2, 3: 4}}
    d_ref = deepcopy(d)
    print("d", d)
    assert d == d_ref

    update_dict_leaves(d, {3: "new"})
    d_ref[3] = "new"
    print("updated d", d)
    assert d == d_ref

    update_dict_leaves(d, {"d": {1: "new"}})
    d_ref["d"][1] = "new"
    print("updated d", d)
    assert d == d_ref

    update_dict_leaves(d, {5: "_DELETE_"})
    del d_ref[5]
    print("updated d", d)
    assert d == d_ref

    with pytest.raises(AssertionError):
        update_dict_leaves(d, {3: {1: 2}})


def test_config_nested(expyre_dummy_config, monkeypatch, tmp_path):
    from expyre import config
    sys_name = list(config.systems)[0]
    print(config.systems[sys_name].partitions)

    # start at tmp_path
    monkeypatch.chdir(tmp_path)

    # make a subdirectory with .expyre inside it
    (Path() / "sub1" / ".expyre").mkdir(parents=True)
    monkeypatch.chdir("sub1")
    print("CWD", Path.cwd())
    config.init("@", verbose=True)
    print("config.local_stage_dir", config.local_stage_dir)
    # make sure stage dir is in cwd
    assert Path(config.local_stage_dir) == Path.cwd() / ".expyre"

    # make a subdirectory with _expyre inside it
    (Path() / "sub2" / "_expyre").mkdir(parents=True)
    monkeypatch.chdir("sub2")
    print("CWD", Path.cwd())
    config.init("@", verbose=True)
    print("config.local_stage_dir", config.local_stage_dir)
    # make sure stage dir is in cwd
    assert Path(config.local_stage_dir) == Path.cwd() / "_expyre"

    # make a subdirectory without _expyre inside it
    (Path() / "sub3").mkdir(parents=True)
    monkeypatch.chdir("sub3")
    print("CWD", Path.cwd())
    config.init("@", verbose=True)
    print("config.local_stage_dir", config.local_stage_dir)
    # make sure stage dir is _one above_ cwd
    assert Path(config.local_stage_dir) == Path.cwd().parent / "_expyre"


def test_config_specific_dir_as_str(expyre_dummy_config, monkeypatch, tmp_path):
    from expyre import config
    sys_name = list(config.systems)[0]
    print(config.systems[sys_name].partitions)

    # start at tmp_path
    monkeypatch.chdir(tmp_path)

    # make a subdirectory with .expyre inside it
    (Path() / "sub1" / ".expyre").mkdir(parents=True)
    monkeypatch.chdir("sub1")
    print("CWD", Path.cwd())
    shutil.copy(config.local_stage_dir / "config.json", Path.cwd() / ".expyre")
    config.init(str(Path.cwd() / ".expyre"), verbose=True)
    print("config.local_stage_dir", config.local_stage_dir)
    # make sure stage dir is in cwd
    assert Path(config.local_stage_dir) == Path.cwd() / ".expyre"


def test_config_override(expyre_dummy_config, monkeypatch, tmp_path):
    from expyre import config
    sys_name = list(config.systems)[0]
    print(config.systems[sys_name].partitions)
    assert config.systems[sys_name].partitions["node32"]["max_time"] == None

    orig_partitions = deepcopy(config.systems[sys_name].partitions)

    # start at tmp_path
    monkeypatch.chdir(tmp_path)

    # make a subdirectory with .expyre inside it
    (Path() / "sub1" / ".expyre").mkdir(parents=True)
    monkeypatch.chdir("sub1")

    with open(".expyre/config.json", "w") as fout:
        fout.write(json.dumps({"systems": {sys_name: {"partitions": {"node32": {"max_time": 1}}}}}))
    print("CWD", Path.cwd())
    config.init("@", verbose=True)
    print(config.systems[sys_name].partitions)
    
    orig_partitions["node32"]["max_time"] = 1
    assert orig_partitions == config.systems[sys_name].partitions
