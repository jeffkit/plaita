"""
PlaitaClient 默认 URL 行为单测。

默认 URL 指向本仓库 plaita-console 控制台的 /api/flowVersion/semver/detail 契约接口；
显式传入 url 仍可覆盖。
"""
from plaita.client import DEFAULT_CONSOLE_URL, PlaitaClient


def test_default_url_points_to_console():
    client = PlaitaClient(secret_id="x", secret_key="y")
    assert client.url == DEFAULT_CONSOLE_URL
    assert client.url.endswith("/api/flowVersion/semver/detail")


def test_explicit_url_overrides_default():
    client = PlaitaClient(secret_id="x", secret_key="y", url="https://my-plaita.example.com/api/flowVersion/semver/detail")
    assert client.url == "https://my-plaita.example.com/api/flowVersion/semver/detail"
    assert client.url != DEFAULT_CONSOLE_URL
