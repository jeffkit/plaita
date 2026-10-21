"""Runtime configuration: single resolver + config generations (A2R31).

This module is the foundation-layer home of the three r31 contracts:

* **B2R31-01** — three-dimensional config identity. ``config_family`` is the
  hard compatibility key (can a process consume this carrier at all),
  ``config_generation`` is the fleet-observability hash, and
  ``budget_generation`` isolates delivery/lease/TTL budgets so a budget change
  is not mistaken for a message-incompatibility change.
* **B2R31-02** — one typed resolver for the env names that used to be read at
  several places with different empty-string/alias semantics
  (``PLAITA_QUEUE_NAME`` / ``QUEUE_NAME`` / ``PLAITA_CONSOLE_TASK_QUEUE``,
  ``PLAITA_REDIS_URL`` / ``REDIS_URL``, ``PLAITA_DLQ_KEY``).
* **B2R31-03** — one invalid-value policy per semantic class. Identity and
  security values fail fast; resource caps reject negatives instead of
  silently clamping; optional tuning values fall back with a WARNING.

The module deliberately imports nothing from ``plaita.server`` or
``plaita-console`` (foundation layering). Process entry points call
:func:`load_runtime_config` once and pass the frozen result down.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple

from plaita.core.retention import task_max_age_seconds as _task_max_age_seconds

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults kept identical to the historical read points.
# ---------------------------------------------------------------------------
DEFAULT_QUEUE_NAME = "plaita:flow:queue"
DEFAULT_REDIS_URL = "redis://localhost:6379/0"
DEFAULT_CONSUMER_GROUP = "plaita-workers"
DEFAULT_DLQ_SUFFIX = ":dlq"
DEFAULT_DLQ_MAX_LEN = 1000
DEFAULT_MAX_DELIVERIES = 5
#: 与 ``execution_lease.DEFAULT_LEASE_TTL_SECONDS``（worker 实际 acquire/renew
#: 的默认值）及 ops-runbook 文档的 120 对齐（b94-02）。此模块不得 import
#: ``plaita.server``（分层约束），故以常量单源对齐；改这里必须同步
#: ``plaita/server/execution_lease.py``，否则预算代际哈希会再次与实际生效值脱钩。
DEFAULT_LEASE_TTL_SECONDS = 120
DEFAULT_TASK_MAX_AGE_SECONDS = 30 * 24 * 3600
DEFAULT_TENANT_STATUS_RECONCILE_SECONDS = 60.0
DEFAULT_TENANT_NAMESPACE_VERSION = "v1"

LEGACY_GENERATION = "legacy"
FAMILY_PREFIX = "cf1-"
GENERATION_PREFIX = "cfg1-"
BUDGET_PREFIX = "bg1-"
_GENERATION_ID_RE = re.compile(r"^(cf1|cfg1|bg1)-[0-9a-f]{12}$")

TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
FALSE_VALUES = frozenset({"0", "false", "no", "off"})

# Semantic classes drive the invalid-value policy (B2R31-03).
CLASS_IDENTITY = "identity"
CLASS_SECURITY = "security"
CLASS_RESOURCE = "resource"
CLASS_TTL = "ttl"
CLASS_OPTIONAL = "optional"
CLASS_FEATURE = "feature"

ROLES = frozenset(
    {"worker", "console", "event_filter", "schedule", "delay", "services", "library"}
)


class ConfigError(ValueError):
    """One or more invalid configuration values (aggregated, fail-fast)."""

    def __init__(self, problems: Sequence[str]):
        self.problems = tuple(problems)
        super().__init__("; ".join(self.problems))


#: WARNING de-duplication: every distinct (env, raw, outcome) is reported once
#: per process (B2R31-03 "每进程每 (env,raw,effective) 一次").
_WARNED: set[Tuple[str, str, str]] = set()


def _warn_once(name: str, raw: Any, outcome: str, message: str, *args: Any) -> None:
    key = (name, "" if raw is None else str(raw), outcome)
    if key in _WARNED:
        return
    _WARNED.add(key)
    logger.warning(message, *args)


def _reset_warnings_for_tests() -> None:
    _WARNED.clear()


# ---------------------------------------------------------------------------
# Typed value parsing (shared by the resolver and the legacy read points).
# ---------------------------------------------------------------------------
def parse_bool(raw: Any, name: str) -> bool:
    """Strict boolean: 1/true/yes/on vs 0/false/no/off (case-insensitive)."""
    if isinstance(raw, bool):
        return raw
    text = "" if raw is None else str(raw).strip().lower()
    if text in TRUE_VALUES:
        return True
    if text in FALSE_VALUES:
        return False
    raise ConfigError(
        [
            f"{name}: invalid boolean (got {_redact(raw)!r}); "
            "expected one of 1/true/yes/on/0/false/no/off"
        ]
    )


def parse_int(raw: Any, name: str, *, minimum: Optional[int] = None) -> int:
    """Strict integer: rejects ``1.5``/``1e3``/``0x10``/blank."""
    text = "" if raw is None else str(raw).strip()
    if not text:
        raise ConfigError([f"{name}: empty value; expected an integer"])
    if not re.fullmatch(r"[+-]?\d+", text):
        raise ConfigError(
            [f"{name}: invalid integer (got {_redact(raw)!r}); expected digits"]
        )
    value = int(text)
    if minimum is not None and value < minimum:
        raise ConfigError(
            [f"{name}: {value} < minimum {minimum} (got {_redact(raw)!r})"]
        )
    return value


def parse_float(raw: Any, name: str, *, minimum: Optional[float] = None) -> float:
    """Strict finite float: rejects ``nan``/``inf`` and non-numeric text."""
    text = "" if raw is None else str(raw).strip()
    if not text:
        raise ConfigError([f"{name}: empty value; expected a number"])
    try:
        value = float(text)
    except (TypeError, ValueError) as exc:
        raise ConfigError(
            [f"{name}: invalid number (got {_redact(raw)!r}); expected a finite number"]
        ) from exc
    if not math.isfinite(value):
        raise ConfigError(
            [f"{name}: non-finite value {text!r}; expected a finite number"]
        )
    if minimum is not None and value < minimum:
        raise ConfigError(
            [f"{name}: {value} < minimum {minimum} (got {_redact(raw)!r})"]
        )
    return value


def parse_enum(raw: Any, name: str, choices: Iterable[str]) -> str:
    text = "" if raw is None else str(raw).strip().lower()
    allowed = tuple(choices)
    if text in allowed:
        return text
    raise ConfigError(
        [
            f"{name}: invalid value (got {_redact(raw)!r}); "
            f"expected one of {', '.join(allowed)}"
        ]
    )


def _redact(value: Any) -> str:
    """Redact credential-looking values for error messages."""
    text = "" if value is None else str(value)
    if "://" in text and "@" in text:
        return re.sub(r"://[^/@\s]+@", "://***@", text)
    return text


# ---------------------------------------------------------------------------
# FieldSpec / ResolvedValue: the typed resolver surface.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class FieldSpec:
    """One env field and its policy (B2R31-02 §架构)."""

    name: str
    category: str
    kind: str = "str"  # str|int|float|bool|url|path|enum
    default: Any = None
    aliases: Tuple[str, ...] = ()
    empty_policy: str = "reject"  # reject|default|unset
    invalid_policy: str = "fail_fast"  # fail_fast|warn_default|warn_off
    minimum: Optional[float] = None
    choices: Tuple[str, ...] = ()
    secret: bool = False
    owner: str = ""


@dataclass(frozen=True)
class ResolvedValue:
    field: str
    raw: Optional[str]
    effective: Any
    source: str  # env name or "default"
    warnings: Tuple[str, ...] = ()
    error: Optional[str] = None


def resolve_field(
    spec: FieldSpec,
    environ: Optional[Mapping[str, str]] = None,
) -> ResolvedValue:
    """Resolve one :class:`FieldSpec` under its declared policy."""
    env = os.environ if environ is None else environ
    names = (spec.name,) + tuple(spec.aliases)
    source = "default"
    raw: Optional[str] = None
    for name in names:
        if name in env:
            source, raw = name, env.get(name)
            break
    if source == "default":
        return ResolvedValue(spec.name, None, spec.default, "default")

    text = "" if raw is None else str(raw).strip()
    if not text:
        if spec.empty_policy == "reject":
            return ResolvedValue(
                spec.name, raw, None, source,
                error=f"{source}: empty value is not allowed (identity/routing)",
            )
        if spec.empty_policy == "unset":
            return ResolvedValue(
                spec.name, raw, spec.default, "default",
                warnings=(f"{source} is empty, treated as unset",),
            )

    try:
        effective = _parse_typed(spec, text, source)
    except ConfigError as exc:
        message = str(exc)
        if spec.invalid_policy == "fail_fast":
            return ResolvedValue(spec.name, raw, None, source, error=message)
        fallback = spec.default
        if spec.invalid_policy == "warn_off" and spec.kind == "bool":
            fallback = False
        return ResolvedValue(
            spec.name, raw, fallback, "default",
            warnings=(f"{message}; using safe default {_redact(fallback)!r}",),
        )
    return ResolvedValue(spec.name, raw, effective, source)


def _parse_typed(spec: FieldSpec, text: str, source: str) -> Any:
    if spec.kind == "str":
        return text
    if spec.kind in ("url", "path"):
        return text
    if spec.kind == "bool":
        return parse_bool(text, source)
    if spec.kind == "int":
        return parse_int(text, source, minimum=None if spec.minimum is None else int(spec.minimum))
    if spec.kind == "float":
        return parse_float(text, source, minimum=spec.minimum)
    if spec.kind == "enum":
        return parse_enum(text, source, spec.choices)
    raise ConfigError([f"{spec.name}: unsupported kind {spec.kind!r}"])


# ---------------------------------------------------------------------------
# Canonical generation IDs.
# ---------------------------------------------------------------------------
def _canonical_value(value: Any) -> Any:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return None
    return value


def canonical_json(fields: Mapping[str, Any]) -> str:
    """Stable canonical JSON: sorted keys, no whitespace, ``None`` dropped."""
    payload = {
        key: _canonical_value(value)
        for key, value in fields.items()
        if value is not None
    }
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )


def _hash(prefix: str, fields: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(canonical_json(fields).encode("utf-8")).hexdigest()
    return f"{prefix}{digest[:12]}"


@dataclass(frozen=True)
class ConfigGeneration:
    """Three independent generations plus their effective timestamp."""

    family: str
    generation: str
    budget: str
    effective_at: str
    details: Mapping[str, Any] = field(default_factory=dict)

    def envelope_fields(self) -> Dict[str, str]:
        return {
            "config_family": self.family,
            "config_generation": self.generation,
            "budget_generation": self.budget,
            "config_effective_at": self.effective_at,
        }


def _family_fields(
    *,
    queue_name: str,
    consumer_group: str,
    dlq_key: str,
    schema_version: int,
    max_schema_version: Optional[int],
    tenant_namespace_version: str,
) -> Dict[str, Any]:
    return {
        "queue_name": queue_name,
        "consumer_group": consumer_group,
        "dlq_key": dlq_key,
        "schema_version": int(schema_version),
        "max_schema_version": (
            None if max_schema_version is None else int(max_schema_version)
        ),
        "tenant_namespace_version": tenant_namespace_version,
    }


def config_family(
    *,
    queue_name: str,
    consumer_group: str,
    dlq_key: str,
    schema_version: int = 1,
    max_schema_version: Optional[int] = None,
    tenant_namespace_version: str = DEFAULT_TENANT_NAMESPACE_VERSION,
) -> str:
    return _hash(
        FAMILY_PREFIX,
        _family_fields(
            queue_name=queue_name,
            consumer_group=consumer_group,
            dlq_key=dlq_key,
            schema_version=schema_version,
            max_schema_version=max_schema_version,
            tenant_namespace_version=tenant_namespace_version,
        ),
    )


def budget_generation(
    *,
    max_deliveries: int,
    lease_ttl_seconds: int,
    task_max_age_seconds: int,
    dlq_max_len: int,
) -> str:
    return _hash(
        BUDGET_PREFIX,
        {
            "max_deliveries": int(max_deliveries),
            "lease_ttl_seconds": int(lease_ttl_seconds),
            "task_max_age_seconds": int(task_max_age_seconds),
            "dlq_max_len": int(dlq_max_len),
        },
    )


def build_generation(
    *,
    queue_name: str,
    consumer_group: str = DEFAULT_CONSUMER_GROUP,
    dlq_key: Optional[str] = None,
    schema_version: int = 1,
    max_schema_version: Optional[int] = None,
    tenant_namespace_version: str = DEFAULT_TENANT_NAMESPACE_VERSION,
    deployment_epoch: str = LEGACY_GENERATION,
    tenant_status_epoch: str = LEGACY_GENERATION,
    lease_ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS,
    max_deliveries: int = DEFAULT_MAX_DELIVERIES,
    task_max_age_seconds: int = DEFAULT_TASK_MAX_AGE_SECONDS,
    dlq_max_len: int = DEFAULT_DLQ_MAX_LEN,
    fencing: bool = True,
    tenant_quota: bool = True,
    dispatch_target: bool = True,
    affinity: bool = True,
    tenant_status_gate: bool = True,
    effective_at: Optional[str] = None,
) -> ConfigGeneration:
    """Build the three generation IDs from the effective configuration.

    ``config_generation`` intentionally excludes pure observability knobs
    (log level, metrics port); changing them must not change the generation.
    """
    dlq = dlq_key or f"{queue_name}{DEFAULT_DLQ_SUFFIX}"
    family_fields = _family_fields(
        queue_name=queue_name,
        consumer_group=consumer_group,
        dlq_key=dlq,
        schema_version=schema_version,
        max_schema_version=max_schema_version,
        tenant_namespace_version=tenant_namespace_version,
    )
    budget = budget_generation(
        max_deliveries=max_deliveries,
        lease_ttl_seconds=lease_ttl_seconds,
        task_max_age_seconds=task_max_age_seconds,
        dlq_max_len=dlq_max_len,
    )
    all_fields = dict(family_fields)
    all_fields.update(
        {
            "deployment_epoch": deployment_epoch,
            "tenant_status_epoch": tenant_status_epoch,
            "lease_ttl_seconds": int(lease_ttl_seconds),
            "max_deliveries": int(max_deliveries),
            "task_max_age_seconds": int(task_max_age_seconds),
            "dlq_max_len": int(dlq_max_len),
            "fencing": fencing,
            "tenant_quota": tenant_quota,
            "dispatch_target": dispatch_target,
            "affinity": affinity,
            "tenant_status_gate": tenant_status_gate,
        }
    )
    stamp = effective_at or datetime.now(timezone.utc).isoformat()
    return ConfigGeneration(
        family=_hash(FAMILY_PREFIX, family_fields),
        generation=_hash(GENERATION_PREFIX, all_fields),
        budget=budget,
        effective_at=stamp,
        details=all_fields,
    )


def is_valid_generation(value: Any, prefix: Optional[str] = None) -> bool:
    if not isinstance(value, str):
        return False
    if prefix is None:
        return bool(_GENERATION_ID_RE.match(value))
    return bool(re.fullmatch(re.escape(prefix) + r"[0-9a-f]{12}", value))


def normalize_generation(value: Any, *, kind: str) -> str:
    """Malformed/missing envelope generation → ``legacy`` + WARNING.

    Never replaces a malformed value with the *current* generation: a carrier
    with a broken envelope must keep its unknown provenance (B2R31-01).
    """
    prefix = {"family": FAMILY_PREFIX, "generation": GENERATION_PREFIX,
              "budget": BUDGET_PREFIX}[kind]
    if value is None:
        return LEGACY_GENERATION
    if isinstance(value, bytes):
        value = value.decode(errors="replace")
    text = str(value).strip()
    if not text:
        return LEGACY_GENERATION
    if is_valid_generation(text, prefix):
        return text
    _warn_once(
        f"config_{kind}",
        text,
        LEGACY_GENERATION,
        f"消息信封 config_{kind}={_redact(text)!r} 畸形，按 legacy 处理"
        "（不猜测代际，B2R31-01）",
    )
    return LEGACY_GENERATION


# ---------------------------------------------------------------------------
# Queue / Redis / DLQ resolvers (B2R31-02).
# ---------------------------------------------------------------------------
_CONSOLE_ROLES = frozenset({"console", "schedule"})
_WORKER_ROLES = frozenset({"worker", "event_filter", "services", "delay", "library"})


def _env_text(env: Mapping[str, str], name: str) -> Tuple[bool, str]:
    if name not in env:
        return False, ""
    raw = env.get(name)
    return True, "" if raw is None else str(raw).strip()


def _first_nonempty(
    env: Mapping[str, str], names: Sequence[str], *, label: str
) -> Tuple[str, Optional[str]]:
    """Return (value, source); an explicitly empty identity value is an error."""
    for name in names:
        present, value = _env_text(env, name)
        if not present:
            continue
        if not value:
            raise ConfigError(
                [f"{name}: empty value is not allowed for {label} (identity/routing)"]
            )
        return value, name
    return "", None


def resolve_queue_name(
    role: str,
    explicit: Optional[str] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> str:
    """Effective task-stream name for a role.

    ``worker``/``event_filter``/``services`` use ``PLAITA_QUEUE_NAME`` then
    ``QUEUE_NAME``; ``console``/``schedule`` use ``PLAITA_CONSOLE_TASK_QUEUE``.
    Empty strings are rejected for every role: identity values must not
    silently degrade to an empty Stream key (A2R31-03).
    """
    if role not in ROLES:
        raise ConfigError([f"unknown config role {role!r}"])
    env = os.environ if environ is None else environ
    if explicit is not None:
        value = str(explicit).strip()
        if not value:
            raise ConfigError(
                ["queue_name: empty value is not allowed (identity/routing)"]
            )
        return value
    if role in _CONSOLE_ROLES:
        names = ("PLAITA_CONSOLE_TASK_QUEUE",)
    else:
        names = ("PLAITA_QUEUE_NAME", "QUEUE_NAME")
    value, _source = _first_nonempty(env, names, label="queue name")
    return value or DEFAULT_QUEUE_NAME


def resolve_dispatch_queue_name(
    role: str = "schedule",
    explicit: Optional[str] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> str:
    """Schedule/dispatch queue: explicit > console env > worker env > default.

    Kept separate from :func:`resolve_queue_name` because the schedule service
    historically accepted either side's env. The A56-02 cross-source WARNING
    moves here so console/capacity/schedule all share one implementation.
    """
    env = os.environ if environ is None else environ
    if explicit is not None:
        value = str(explicit).strip()
        if not value:
            raise ConfigError(
                ["queue_name: empty value is not allowed (identity/routing)"]
            )
        return value
    console, console_src = _first_nonempty(
        env, ("PLAITA_CONSOLE_TASK_QUEUE",), label="console queue name"
    )
    worker, worker_src = _first_nonempty(
        env, ("PLAITA_QUEUE_NAME", "QUEUE_NAME"), label="worker queue name"
    )
    if console and worker and console != worker:
        _warn_once(
            "queue_name",
            f"{console}|{worker}",
            "split",
            "调度派发队列与 worker 消费队列不同源：console/调度=%s，worker=%s"
            "（PLAITA_CONSOLE_TASK_QUEUE vs PLAITA_QUEUE_NAME）——消息可能落入"
            "无人消费的队列，迁移时两个 env 必须一起改",
            console,
            worker,
        )
    return console or worker or DEFAULT_QUEUE_NAME


def resolve_redis_url(
    role: str,
    explicit: Optional[str] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> str:
    """Effective Redis URL for a role.

    Every entry point accepts the ``PLAITA_REDIS_URL`` > ``REDIS_URL`` alias
    chain (A2R31-04). The console additionally prefers its own
    ``PLAITA_CONSOLE_REDIS_URL`` (historical contract), then falls back to the
    generic chain so a manual deployment that only sets ``REDIS_URL`` does not
    silently connect to localhost.
    """
    if role not in ROLES:
        raise ConfigError([f"unknown config role {role!r}"])
    env = os.environ if environ is None else environ
    if explicit is not None:
        value = str(explicit).strip()
        if not value:
            raise ConfigError(["redis_url: empty value is not allowed"])
        return value
    names: Sequence[str]
    if role == "console":
        names = ("PLAITA_CONSOLE_REDIS_URL", "PLAITA_REDIS_URL", "REDIS_URL")
    else:
        names = ("PLAITA_REDIS_URL", "REDIS_URL")
    value, _source = _first_nonempty(env, names, label="Redis URL")
    return value or DEFAULT_REDIS_URL


def resolve_consumer_group(
    explicit: Optional[str] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> str:
    env = os.environ if environ is None else environ
    if explicit is not None:
        value = str(explicit).strip()
        if not value:
            raise ConfigError(
                ["consumer_group: empty value is not allowed (identity/routing)"]
            )
        return value
    value, _source = _first_nonempty(
        env, ("PLAITA_CONSUMER_GROUP",), label="consumer group"
    )
    return value or DEFAULT_CONSUMER_GROUP


def resolve_dlq_key(
    queue_name: str,
    explicit: Optional[str] = None,
    environ: Optional[Mapping[str, str]] = None,
    *,
    empty_policy: str = "reject",
) -> str:
    env = os.environ if environ is None else environ
    if explicit is not None:
        value = str(explicit).strip()
        if value:
            return value
        if empty_policy == "reject":
            raise ConfigError(
                ["PLAITA_DLQ_KEY: empty value is not allowed (identity/routing)"]
            )
        return f"{queue_name}{DEFAULT_DLQ_SUFFIX}"
    present, value = _env_text(env, "PLAITA_DLQ_KEY")
    if present:
        if value:
            return value
        if empty_policy == "reject":
            raise ConfigError(
                ["PLAITA_DLQ_KEY: empty value is not allowed (identity/routing)"]
            )
    return f"{queue_name}{DEFAULT_DLQ_SUFFIX}"


def resolve_instance_id(
    explicit: Optional[str] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> Optional[str]:
    """``PLAITA_INSTANCE_ID`` (empty = identity error, unset = None)."""
    env = os.environ if environ is None else environ
    if explicit is not None:
        value = str(explicit).strip()
        if not value:
            raise ConfigError(
                ["PLAITA_INSTANCE_ID: empty value is not allowed (identity)"]
            )
        return value
    present, value = _env_text(env, "PLAITA_INSTANCE_ID")
    if not present:
        return None
    if not value:
        raise ConfigError(
            ["PLAITA_INSTANCE_ID: empty value is not allowed (identity)"]
        )
    return value


def resolve_deployment_epoch(environ: Optional[Mapping[str, str]] = None) -> str:
    env = os.environ if environ is None else environ
    present, value = _env_text(env, "PLAITA_DEPLOYMENT_EPOCH")
    if not present:
        return LEGACY_GENERATION
    if not value:
        raise ConfigError(
            ["PLAITA_DEPLOYMENT_EPOCH: empty value is not allowed (identity)"]
        )
    return value


# ---------------------------------------------------------------------------
# RuntimeConfig.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class RuntimeConfig:
    role: str
    queue_name: str
    redis_url: str
    consumer_group: str
    dlq_key: str
    max_deliveries: int
    lease_ttl_seconds: int
    task_max_age_seconds: int
    dlq_max_len: int
    tenant_namespace_version: str
    deployment_epoch: str
    tenant_status_epoch: str
    tenant_status_reconcile_seconds: float
    tenant_status_epoch_strict: bool
    fencing: bool
    tenant_quota: bool
    dispatch_target: bool
    affinity: bool
    tenant_status_gate: bool
    instance_id: Optional[str] = None
    generation: Optional[ConfigGeneration] = None
    warnings: Tuple[str, ...] = ()
    errors: Tuple[str, ...] = ()

    def require_valid(self) -> "RuntimeConfig":
        if self.errors:
            raise ConfigError(self.errors)
        return self


def _switch(
    env: Mapping[str, str],
    name: str,
    *,
    default: bool,
    errors: list,
    warnings: list,
) -> bool:
    """Strict boolean switch (new switches only)."""
    if name not in env:
        return default
    raw = env.get(name)
    try:
        return parse_bool(raw, name)
    except ConfigError as exc:
        errors.append(str(exc))
        return default


def _legacy_disable_switch(
    env: Mapping[str, str],
    name: str,
    *,
    errors: list,
    warnings: list,
) -> bool:
    """Legacy ``DISABLE_*``: only the exact text ``1`` disables.

    B2R31-03 keeps one release of the historical dialect: ``true``/``yes``/
    ``on`` are **not** treated as disable (matching ``_env_switch`` at runtime)
    but emit a WARNING recommending ``1``. Unknown values keep the safety
    mechanism enabled (``False``) with a WARNING, never fail-open.
    """
    if name not in env:
        return False
    raw = env.get(name)
    text = "" if raw is None else str(raw).strip()
    if text == "1":
        return True
    if text in FALSE_VALUES or not text:
        return False
    if text.lower() in TRUE_VALUES:
        warnings.append(
            f"{name}={_redact(raw)!r}: legacy DISABLE_* 只认精确 '1'，"
            "该值不会关闭机制（下一版统一布尔方言；当前请用 1）"
        )
        return False
    warnings.append(
        f"{name}={_redact(raw)!r}: 未知值，保持安全机制开启（DISABLE=0 语义）"
    )
    return False


def _int_value(
    env: Mapping[str, str],
    name: str,
    default: int,
    *,
    minimum: int,
    category: str,
    errors: list,
    warnings: list,
) -> int:
    if name not in env:
        return default
    raw = env.get(name)
    try:
        return parse_int(raw, name, minimum=minimum)
    except ConfigError as exc:
        if category in (CLASS_IDENTITY, CLASS_SECURITY, CLASS_RESOURCE, CLASS_TTL):
            errors.append(str(exc))
        else:
            warnings.append(f"{exc}; using default {default}")
        return default


def _budget_int(
    overrides: Mapping[str, Any],
    key: str,
    env: Mapping[str, str],
    env_name: str,
    default: int,
    *,
    minimum: int,
    category: str,
    errors: list,
    warnings: list,
) -> int:
    """Resolve one budget knob: explicit override wins over env (b94-02).

    Workers pass their constructor/CLI-effective values as ``overrides``
    (``max_deliveries`` / ``lease_ttl_seconds`` / ``task_max_age_seconds`` /
    ``dlq_max_len``). The budget generation hash must be computed from those
    actual values; reading only env silently hashed the env-or-300 defaults
    while the worker enforced the constructor values (default lease 120).
    ``None`` means "not supplied" and falls back to env, like identity keys.
    """
    if overrides.get(key) is not None:
        raw = overrides[key]
        try:
            return parse_int(raw, key, minimum=minimum)
        except ConfigError as exc:
            if category in (CLASS_IDENTITY, CLASS_SECURITY, CLASS_RESOURCE, CLASS_TTL):
                errors.append(str(exc))
            else:
                warnings.append(f"{exc}; using default {default}")
            return default
    return _int_value(
        env,
        env_name,
        default,
        minimum=minimum,
        category=category,
        errors=errors,
        warnings=warnings,
    )


def _float_value(
    env: Mapping[str, str],
    name: str,
    default: float,
    *,
    minimum: float,
    category: str,
    errors: list,
    warnings: list,
) -> float:
    if name not in env:
        return default
    raw = env.get(name)
    try:
        return parse_float(raw, name, minimum=minimum)
    except ConfigError as exc:
        if category in (CLASS_IDENTITY, CLASS_SECURITY, CLASS_RESOURCE, CLASS_TTL):
            errors.append(str(exc))
        else:
            warnings.append(f"{exc}; using default {default}")
        return default


def load_runtime_config(
    role: str,
    *,
    environ: Optional[Mapping[str, str]] = None,
    strict: bool = False,
    overrides: Optional[Mapping[str, Any]] = None,
) -> RuntimeConfig:
    """Resolve all runtime fields for a role.

    ``strict=True`` aggregates every identity/security/resource/TTL error and
    raises :class:`ConfigError` once (process entry points). ``strict=False``
    is the library/test surface: invalid values fall back to safe defaults
    with WARNING-level observability, never silently.
    """
    if role not in ROLES:
        raise ConfigError([f"unknown config role {role!r}"])
    env: Mapping[str, str] = os.environ if environ is None else environ
    overrides = overrides or {}
    errors: list = []
    warnings: list = []

    def _resolve(fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ConfigError as exc:
            errors.extend(exc.problems)
            return None

    queue_name = overrides.get("queue_name")
    if queue_name is None:
        queue_name = _resolve(resolve_queue_name, role, None, env)
    else:
        queue_name = str(queue_name).strip()
        if not queue_name:
            errors.append("queue_name: empty value is not allowed (identity/routing)")
    queue_name = queue_name or DEFAULT_QUEUE_NAME

    redis_url = overrides.get("redis_url")
    if redis_url is None:
        redis_url = _resolve(resolve_redis_url, role, None, env)
    redis_url = redis_url or DEFAULT_REDIS_URL

    consumer_group = overrides.get("consumer_group")
    if consumer_group is None:
        consumer_group = _resolve(resolve_consumer_group, None, env)
    consumer_group = consumer_group or DEFAULT_CONSUMER_GROUP

    dlq_key = overrides.get("dlq_key")
    if dlq_key is None:
        dlq_key = _resolve(resolve_dlq_key, queue_name, None, env)
    dlq_key = dlq_key or f"{queue_name}{DEFAULT_DLQ_SUFFIX}"

    instance_id = overrides.get("instance_id")
    if instance_id is None:
        instance_id = _resolve(resolve_instance_id, None, env)

    deployment_epoch = overrides.get("deployment_epoch")
    if deployment_epoch is None:
        deployment_epoch = _resolve(resolve_deployment_epoch, env)
    deployment_epoch = deployment_epoch or LEGACY_GENERATION

    tenant_status_epoch = overrides.get(
        "tenant_status_epoch", deployment_epoch
    )

    max_deliveries = _budget_int(
        overrides, "max_deliveries", env, "PLAITA_MAX_DELIVERIES",
        DEFAULT_MAX_DELIVERIES, minimum=1, category=CLASS_RESOURCE,
        errors=errors, warnings=warnings,
    )
    lease_ttl = _budget_int(
        overrides, "lease_ttl_seconds", env, "PLAITA_LEASE_TTL_SECONDS",
        DEFAULT_LEASE_TTL_SECONDS, minimum=1, category=CLASS_TTL,
        errors=errors, warnings=warnings,
    )
    task_max_age = _budget_int(
        overrides, "task_max_age_seconds", env, "PLAITA_TASK_MAX_AGE_SECONDS",
        DEFAULT_TASK_MAX_AGE_SECONDS, minimum=0, category=CLASS_TTL,
        errors=errors, warnings=warnings,
    )
    dlq_max_len = _budget_int(
        overrides, "dlq_max_len", env, "PLAITA_DLQ_MAX_LEN",
        DEFAULT_DLQ_MAX_LEN, minimum=1, category=CLASS_RESOURCE,
        errors=errors, warnings=warnings,
    )
    reconcile = _float_value(
        env, "PLAITA_TENANT_STATUS_RECONCILE_SECONDS",
        DEFAULT_TENANT_STATUS_RECONCILE_SECONDS, minimum=0,
        category=CLASS_TTL, errors=errors, warnings=warnings,
    )
    if reconcile == 0 and "PLAITA_TENANT_STATUS_RECONCILE_SECONDS" in env:
        warnings.append(
            "PLAITA_TENANT_STATUS_RECONCILE_SECONDS=0：周期对账已关闭；"
            "Redis-only 恢复后的运行中漂移不会被自动纠正"
        )

    fencing = not _legacy_disable_switch(
        env, "PLAITA_DISABLE_FENCING", errors=errors, warnings=warnings,
    )
    tenant_quota = not _legacy_disable_switch(
        env, "PLAITA_DISABLE_TENANT_QUOTA", errors=errors, warnings=warnings,
    )
    dispatch_target = not _legacy_disable_switch(
        env, "PLAITA_DISABLE_DISPATCH_TARGET", errors=errors, warnings=warnings,
    )
    affinity = not _legacy_disable_switch(
        env, "PLAITA_DISABLE_AFFINITY", errors=errors, warnings=warnings,
    )
    tenant_status_gate = not _legacy_disable_switch(
        env, "PLAITA_DISABLE_TENANT_STATUS_GATE", errors=errors, warnings=warnings,
    )
    epoch_strict = _switch(
        env, "PLAITA_TENANT_STATUS_EPOCH_STRICT", default=False,
        errors=errors, warnings=warnings,
    )
    # A2R31-02 灰度开关：显式 ``PLAITA_TENANT_STATUS_EPOCH_STRICT=0`` 是回滚阀，
    # 让 worker 回到 #237 的旧语义；未设 = 部署纪元存在时启用。
    if (
        "PLAITA_TENANT_STATUS_EPOCH_STRICT" in env
        and not epoch_strict
        and tenant_status_epoch != LEGACY_GENERATION
    ):
        warnings.append(
            "PLAITA_TENANT_STATUS_EPOCH_STRICT=0：租户状态纪元闸已关闭"
            "（回滚到 #237 fail-open；运行中 Redis-only 恢复可能复活已删租户）"
        )
        tenant_status_epoch = LEGACY_GENERATION

    namespace = overrides.get("tenant_namespace_version")
    if namespace is None:
        namespace = (
            env.get("PLAITA_TENANT_NAMESPACE_VERSION", DEFAULT_TENANT_NAMESPACE_VERSION)
            or DEFAULT_TENANT_NAMESPACE_VERSION
        ).strip()
    if not namespace:
        errors.append(
            "PLAITA_TENANT_NAMESPACE_VERSION: empty value is not allowed (identity)"
        )
        namespace = DEFAULT_TENANT_NAMESPACE_VERSION

    generation = build_generation(
        queue_name=queue_name,
        consumer_group=consumer_group,
        dlq_key=dlq_key,
        max_schema_version=overrides.get("max_schema_version"),
        tenant_namespace_version=namespace,
        deployment_epoch=deployment_epoch,
        tenant_status_epoch=tenant_status_epoch,
        lease_ttl_seconds=lease_ttl,
        max_deliveries=max_deliveries,
        task_max_age_seconds=task_max_age,
        dlq_max_len=dlq_max_len,
        fencing=fencing,
        tenant_quota=tenant_quota,
        dispatch_target=dispatch_target,
        affinity=affinity,
        tenant_status_gate=tenant_status_gate,
    )

    config = RuntimeConfig(
        role=role,
        queue_name=queue_name,
        redis_url=redis_url,
        consumer_group=consumer_group,
        dlq_key=dlq_key,
        max_deliveries=max_deliveries,
        lease_ttl_seconds=lease_ttl,
        task_max_age_seconds=task_max_age,
        dlq_max_len=dlq_max_len,
        tenant_namespace_version=namespace,
        deployment_epoch=deployment_epoch,
        tenant_status_epoch=tenant_status_epoch,
        tenant_status_reconcile_seconds=reconcile,
        tenant_status_epoch_strict=epoch_strict,
        fencing=fencing,
        tenant_quota=tenant_quota,
        dispatch_target=dispatch_target,
        affinity=affinity,
        tenant_status_gate=tenant_status_gate,
        instance_id=instance_id,
        generation=generation,
        warnings=tuple(warnings),
        errors=tuple(errors),
    )
    for warning in warnings:
        _warn_once("runtime_config", warning, warning, "配置降级：%s", warning)
    if strict and config.errors:
        raise ConfigError(config.errors)
    return config


def log_config_effective(logger_: logging.Logger, config: RuntimeConfig) -> None:
    """Emit the ``event=config.effective`` startup line (secrets masked)."""
    generation = config.generation
    logger_.info(
        "event=config.effective role=%s queue=%s redis=%s group=%s dlq=%s "
        "max_deliveries=%s lease_ttl=%s task_max_age=%s dlq_max_len=%s "
        "deployment_epoch=%s tenant_status_epoch=%s "
        "config_family=%s config_generation=%s budget_generation=%s "
        "fencing=%s tenant_quota=%s dispatch_target=%s affinity=%s "
        "tenant_status_gate=%s warnings=%s",
        config.role,
        config.queue_name,
        _redact(config.redis_url),
        config.consumer_group,
        config.dlq_key,
        config.max_deliveries,
        config.lease_ttl_seconds,
        config.task_max_age_seconds,
        config.dlq_max_len,
        config.deployment_epoch,
        config.tenant_status_epoch,
        generation.family if generation else LEGACY_GENERATION,
        generation.generation if generation else LEGACY_GENERATION,
        generation.budget if generation else LEGACY_GENERATION,
        config.fencing,
        config.tenant_quota,
        config.dispatch_target,
        config.affinity,
        config.tenant_status_gate,
        list(config.warnings),
    )


def optional_bool_env(
    name: str, default: bool = False, environ: Optional[Mapping[str, str]] = None
) -> bool:
    """Optional/feature boolean: invalid → WARNING + safe default (never silent)."""
    env = os.environ if environ is None else environ
    if name not in env:
        return default
    raw = env.get(name)
    try:
        return parse_bool(raw, name)
    except ConfigError:
        _warn_once(
            name, raw, "default",
            "%s=%r 非法，使用安全默认 %s（B2R31-03）",
            name, _redact(raw), default,
        )
        return default


def security_bool_env(
    name: str,
    *,
    default: bool = False,
    fail_closed: bool = True,
    environ: Optional[Mapping[str, str]] = None,
) -> bool:
    """Security gate boolean: invalid → fail-closed + WARNING (never open)."""
    env = os.environ if environ is None else environ
    if name not in env:
        return default
    raw = env.get(name)
    try:
        return parse_bool(raw, name)
    except ConfigError:
        effective = True if fail_closed else default
        _warn_once(
            name, raw, "fail_closed",
            "%s=%r 非法，安全门按 fail-closed=%s 处理（B2R31-03；"
            "启动预检会拒绝该配置）",
            name, _redact(raw), effective,
        )
        return effective


def optional_int_env(
    name: str,
    default: int,
    *,
    minimum: int = 1,
    environ: Optional[Mapping[str, str]] = None,
) -> int:
    """Optional numeric tuning: invalid → WARNING + default (never silent)."""
    env = os.environ if environ is None else environ
    if name not in env:
        return default
    raw = env.get(name)
    try:
        return parse_int(raw, name, minimum=minimum)
    except ConfigError:
        _warn_once(
            name, raw, "default",
            "%s=%r 非法，使用安全默认 %s（B2R31-03）",
            name, _redact(raw), default,
        )
        return default


def optional_float_env(
    name: str,
    default: float,
    *,
    minimum: float = 0.0,
    environ: Optional[Mapping[str, str]] = None,
) -> float:
    """Optional float tuning: invalid/nan/inf → WARNING + default."""
    env = os.environ if environ is None else environ
    if name not in env:
        return default
    raw = env.get(name)
    try:
        return parse_float(raw, name, minimum=minimum)
    except ConfigError:
        _warn_once(
            name, raw, "default",
            "%s=%r 非法，使用安全默认 %s（B2R31-03）",
            name, _redact(raw), default,
        )
        return default


def resolve_langfuse_mode(environ: Optional[Mapping[str, str]] = None) -> str:
    """``PLAITA_CONSOLE_LANGFUSE`` strict enum: ``auto|true|false``.

    Unknown values are **false** + WARNING (never "unknown means on"); the
    console startup preflight turns the same value into a hard error.
    """
    env = os.environ if environ is None else environ
    raw = env.get("PLAITA_CONSOLE_LANGFUSE", "auto")
    try:
        return parse_enum(raw, "PLAITA_CONSOLE_LANGFUSE", ("auto", "true", "false"))
    except ConfigError:
        _warn_once(
            "PLAITA_CONSOLE_LANGFUSE", raw, "false",
            "PLAITA_CONSOLE_LANGFUSE=%r 非法（期望 auto/true/false），按 false 处理"
            "（B2R31-03：未知值不得当开）",
            _redact(raw),
        )
        return "false"


# ---------------------------------------------------------------------------
# Preflight for values read by modules that historically owned their parser.
# ---------------------------------------------------------------------------
def preflight_security_and_limits(environ: Optional[Mapping[str, str]] = None) -> list:
    """Validate the six-form matrix; return warnings, raise on hard errors.

    This is called from the worker/console process entry points. Per-call
    readers still fail closed (security) or WARN (optional) so a library
    embedder never loses observability, but a real deployment refuses to start
    on an invalid identity/security/resource value.
    """
    env: Mapping[str, str] = os.environ if environ is None else environ
    errors: list = []
    warnings: list = []

    # Security gates: fail-fast on unknown boolean/enumeration.
    if "PLAITA_HTTP_BLOCK_PRIVATE" in env:
        raw = env.get("PLAITA_HTTP_BLOCK_PRIVATE", "")
        if str(raw).strip():
            try:
                parse_bool(raw, "PLAITA_HTTP_BLOCK_PRIVATE")
            except ConfigError as exc:
                errors.append(str(exc))
        else:
            warnings.append(
                "PLAITA_HTTP_BLOCK_PRIVATE is empty; treated as unset (gate off)"
            )
    if "PLAITA_SANDBOX_MEMORY_MB" in env:
        raw = env.get("PLAITA_SANDBOX_MEMORY_MB", "")
        if str(raw).strip():
            try:
                parse_int(raw, "PLAITA_SANDBOX_MEMORY_MB", minimum=0)
            except ConfigError as exc:
                errors.append(str(exc))
    if "PLAITA_SANDBOX_DOCKER_CPUS" in env:
        raw = env.get("PLAITA_SANDBOX_DOCKER_CPUS", "")
        if str(raw).strip():
            try:
                parse_float(raw, "PLAITA_SANDBOX_DOCKER_CPUS", minimum=1e-9)
            except ConfigError as exc:
                errors.append(str(exc))
    if "PLAITA_CONSOLE_LANGFUSE" in env:
        raw = env.get("PLAITA_CONSOLE_LANGFUSE", "")
        if str(raw).strip():
            try:
                parse_enum(raw, "PLAITA_CONSOLE_LANGFUSE", ("auto", "true", "false"))
            except ConfigError as exc:
                errors.append(str(exc))
    if "PLAITA_TENANT_MAX_RUNNING" in env:
        raw = env.get("PLAITA_TENANT_MAX_RUNNING", "")
        if str(raw).strip():
            try:
                parse_int(raw, "PLAITA_TENANT_MAX_RUNNING", minimum=0)
            except ConfigError as exc:
                errors.append(str(exc))
    if "PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN" in env:
        raw = env.get("PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN", "")
        if str(raw).strip():
            try:
                parse_bool(raw, "PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN")
            except ConfigError as exc:
                errors.append(str(exc))

    # Optional tuning: WARNING + safe default, never fail the process.
    for name, minimum, default in (
        ("PLAITA_HTTP_MAX_RESPONSE_BYTES", 1, 10 * 1024 * 1024),
        ("PLAITA_HTTP_CONNECTOR_LIMIT", 1, 100),
    ):
        if name in env and str(env.get(name, "")).strip():
            try:
                parse_int(env.get(name), name, minimum=minimum)
            except ConfigError as exc:
                warnings.append(f"{exc}; using safe default {default}")
    for name, minimum, default in (
        ("PLAITA_HTTP_DNS_TTL", 0, 30.0),
        ("PLAITA_EVENT_RECONCILE_INTERVAL", 1e-9, None),
        ("PLAITA_CHECK_INTERVAL", 1e-9, None),
    ):
        if name in env and str(env.get(name, "")).strip():
            try:
                parse_float(env.get(name), name, minimum=minimum)
            except ConfigError as exc:
                if default is None:
                    errors.append(str(exc))
                else:
                    warnings.append(f"{exc}; using safe default {default}")
    for name in ("PLAITA_HTTP_COOKIES", "PLAITA_CONSOLE_DEBUG"):
        if name in env and str(env.get(name, "")).strip():
            try:
                parse_bool(env.get(name), name)
            except ConfigError as exc:
                warnings.append(f"{exc}; using safe default False")
    if "PLAITA_LOG_LEVEL" in env and str(env.get("PLAITA_LOG_LEVEL", "")).strip():
        level = str(env.get("PLAITA_LOG_LEVEL")).strip().upper()
        if level not in logging._nameToLevel:
            errors.append(
                f"PLAITA_LOG_LEVEL: invalid level (got {_redact(env.get('PLAITA_LOG_LEVEL'))!r}); "
                f"expected one of {', '.join(sorted(logging._nameToLevel))}"
            )
    for name in (
        "PLAITA_DELAY_QUEUE",        "PLAITA_INSTANCE_ID",
        "PLAITA_CONSUMER_GROUP",
        "PLAITA_QUEUE_NAME",
        "QUEUE_NAME",
        "PLAITA_CONSOLE_TASK_QUEUE",
        "PLAITA_REDIS_URL",
        "REDIS_URL",
        "PLAITA_CONSOLE_REDIS_URL",
        "PLAITA_DLQ_KEY",
    ):
        if name in env and not str(env.get(name, "")).strip():
            errors.append(
                f"{name}: empty value is not allowed (identity/routing)"
            )

    for warning in warnings:
        _warn_once("preflight", warning, warning, "配置预检：%s", warning)
    if errors:
        raise ConfigError(errors)
    return warnings


def envelope_budget_values(
    environ: Optional[Mapping[str, str]] = None,
) -> Dict[str, int]:
    """The four budget inputs producers hash into ``budget_generation``.

    Single source for ``envelope_for_stream`` and the worker CLI preflight
    (plaita#272). Producers only read env; a worker CLI budget override that
    differs makes the two ends hash different budgets, so reclaim rejects the
    new task's PEL forever. Callers compare their actual worker values with
    this tuple and fail fast on mismatch.
    """
    env: Mapping[str, str] = os.environ if environ is None else environ
    return {
        "max_deliveries": _safe_int(
            env.get("PLAITA_MAX_DELIVERIES"), DEFAULT_MAX_DELIVERIES
        ),
        "lease_ttl_seconds": _safe_int(
            env.get("PLAITA_LEASE_TTL_SECONDS"), DEFAULT_LEASE_TTL_SECONDS
        ),
        # plaita#271：0 是文档定义的「关闭年龄闸」合法值。生产者必须与 worker
        # 共用 core.retention.task_max_age_seconds（非法→30d+WARNING，<=0→0）。
        "task_max_age_seconds": _task_max_age_seconds(env),
        "dlq_max_len": _safe_int(
            env.get("PLAITA_DLQ_MAX_LEN"), DEFAULT_DLQ_MAX_LEN
        ),
    }


def envelope_for_stream(
    stream_key: str,
    *,
    role: str = "console",
    consumer_group: Optional[str] = None,
    dlq_key: Optional[str] = None,
    schema_version: int = 1,
    max_schema_version: Optional[int] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> Dict[str, str]:
    """Current-generation envelope fields for a producer writing to a stream.

    Producers compute the family from the *actual* destination stream plus the
    effective group/DLQ, not from the producer's own queue env (console and
    worker use different env names). Invalid values degrade to ``legacy`` with
    a WARNING rather than failing the enqueue path.
    """
    env: Mapping[str, str] = os.environ if environ is None else environ
    group = consumer_group
    if group is None:
        try:
            group = resolve_consumer_group(None, env)
        except ConfigError:
            group = DEFAULT_CONSUMER_GROUP
    dlq = dlq_key
    if dlq is None:
        try:
            dlq = resolve_dlq_key(stream_key, None, env, empty_policy="default")
        except ConfigError:
            dlq = f"{stream_key}{DEFAULT_DLQ_SUFFIX}"
    deployment_epoch = env.get("PLAITA_DEPLOYMENT_EPOCH", LEGACY_GENERATION).strip()
    deployment_epoch = deployment_epoch or LEGACY_GENERATION
    # Producers do not own the delivery budget of the consumer; the four
    # values come from env only and are the same tuple the worker CLI
    # preflight compares against (#272).
    budget = envelope_budget_values(env)
    max_deliveries = budget["max_deliveries"]
    lease_ttl = budget["lease_ttl_seconds"]
    task_max_age = budget["task_max_age_seconds"]
    dlq_max_len = budget["dlq_max_len"]
    namespace = (
        env.get("PLAITA_TENANT_NAMESPACE_VERSION", DEFAULT_TENANT_NAMESPACE_VERSION)
        or DEFAULT_TENANT_NAMESPACE_VERSION
    ).strip() or DEFAULT_TENANT_NAMESPACE_VERSION
    generation = build_generation(
        queue_name=stream_key,
        consumer_group=group,
        dlq_key=dlq,
        schema_version=schema_version,
        max_schema_version=max_schema_version,
        tenant_namespace_version=namespace,
        deployment_epoch=deployment_epoch,
        tenant_status_epoch=deployment_epoch,
        lease_ttl_seconds=lease_ttl,
        max_deliveries=max_deliveries,
        task_max_age_seconds=task_max_age,
        dlq_max_len=dlq_max_len,
    )
    return generation.envelope_fields()


def _safe_int(raw: Any, default: int) -> int:
    try:
        return parse_int(raw, "value", minimum=1)
    except ConfigError:
        return default
