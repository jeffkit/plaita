"""plaita#209：supervisor 数据集路径按**路径归属**判越界，不是字符串前缀。

`str(candidate).startswith(str(root))` 会放行同前缀兄弟目录
（`/srv/supervisor-datasets-evil/`）；修复改为
`candidate.is_relative_to(root)`（resolve 后按路径段比较）。
兄弟前缀 / 绝对路径 / `..` 逃逸均 400，正常子目录与 .json 保持可读。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import supervisor as supervisor_api  # noqa: E402


@pytest.fixture()
def dataset_env(tmp_path, monkeypatch):
    root = tmp_path / "supervisor-datasets"
    root.mkdir()
    (root / "case.json").write_text('{"cases": []}', encoding="utf-8")
    (root / "sub").mkdir()
    (root / "sub" / "case.json").write_text('{"cases": []}', encoding="utf-8")

    sibling = tmp_path / "supervisor-datasets-evil"
    sibling.mkdir()
    (sibling / "x.json").write_text('{"evil": true}', encoding="utf-8")

    outside = tmp_path / "other"
    outside.mkdir()
    (outside / "x.json").write_text('{"outside": true}', encoding="utf-8")

    monkeypatch.setenv("PLAITA_SUPERVISOR_DATASET_DIR", str(root))
    return root, sibling, outside


def _resolve(name: str) -> Path:
    return supervisor_api._resolve_dataset_path(name)


def test_normal_file_and_subdir_allowed(dataset_env):
    root, _sibling, _outside = dataset_env
    assert _resolve("case.json") == (root / "case.json").resolve()
    assert _resolve("sub/case.json") == (root / "sub" / "case.json").resolve()


def test_sibling_prefix_is_rejected(dataset_env):
    """核心回归：同前缀兄弟目录不是 root 的子路径。"""
    with pytest.raises(HTTPException) as excinfo:
        _resolve("../supervisor-datasets-evil/x.json")
    assert excinfo.value.status_code == 400
    assert "dataset 路径越界" in str(excinfo.value.detail)


def test_absolute_path_is_rejected(dataset_env):
    _root, _sibling, outside = dataset_env
    with pytest.raises(HTTPException) as excinfo:
        _resolve(str(outside / "x.json"))
    assert excinfo.value.status_code == 400
    assert "dataset 路径越界" in str(excinfo.value.detail)


def test_dotdot_escape_is_rejected(dataset_env):
    with pytest.raises(HTTPException) as excinfo:
        _resolve("a/../../x.json")
    assert excinfo.value.status_code == 400
    assert "dataset 路径越界" in str(excinfo.value.detail)


def test_missing_normal_path_is_404_not_400(dataset_env):
    """正常名字但文件不存在 ⇒ 404（越界判据没有误伤正常路径）。"""
    with pytest.raises(HTTPException) as excinfo:
        _resolve("missing.json")
    assert excinfo.value.status_code == 404


def test_list_datasets_filtering_unchanged(dataset_env):
    root, _sibling, _outside = dataset_env
    (root / "_hidden").mkdir()
    (root / ".hidden").mkdir()
    (root / "demo.json").write_text("{}", encoding="utf-8")

    listed = supervisor_api.list_datasets("demo-flow")
    names = [item["name"] for item in listed["datasets"]]
    assert names == ["case.json", "demo.json", "sub"]
    assert "_hidden" not in names
    assert ".hidden" not in names
    assert listed["root"] == str(root.resolve())
