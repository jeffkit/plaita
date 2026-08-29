"""轻量编排 Agent（BrainRunner 默认实现，M2）。

专为编排 Copilot 定制的窄域 agent：理解意图 → 修改 flow IR → 用本地工具
（引擎强校验 / 真实试跑）自检 → 输出 ```plaita-flow 代码块供前端应用。

与 recursive/claude CLI 大脑的分工：
- flow_agent（本模块）：日常编辑/生成——快、token 省、行为可控、无需 CLI；
- recursive / claude-code：大型重构等需要自主多步探索的任务（BrainRunner
  抽象下可经 PLAITA_CONSOLE_COPILOT_BRAIN 切换）。

模型：anthropic 协议端点（默认 GLM-5.2），凭证从 flowcast providers.json
读取（与 recursive/claude 同源），后端不落密钥。
"""
import json
import logging
import os
from typing import Optional

from pydantic_ai import Agent, RunContext
from pydantic_ai.models.anthropic import AnthropicModel
from pydantic_ai.providers.anthropic import AnthropicProvider

try:
    from . import ai_flow, dryrun as dryrun_svc
except ImportError:  # 运行态 services 为顶层包
    import ai_flow
    import dryrun as dryrun_svc

logger = logging.getLogger(__name__)

COPILOT_SYSTEM_PROMPT = """\
你是 Plaita 流程编排 Copilot，帮助用户在可视化画布上创建和修改工作流。

## 工作方式
- 用户消息末尾附有「当前画布状态」（完整 flow JSON），你的一切修改都基于它。
- 需要输出修改时，把**完整的**新 flow JSON 放在 ```plaita-flow 代码块中，
  前端会自动应用并刷新画布。只输出有变化后完整结果，不要输出片段或 diff。
- 输出前先用 validate_flow 工具校验、用 dry_run_flow 工具试跑（流程入参
  不明时可用 {} 试通结构），失败则修正后再输出。
- flow JSON 结构：{"nodes":[{type,id,name,next,else_next,branches,...}],...}；
  线性连接用 next，if 分支用 next（真）+ else_next（假），switch/case 分支在
  branches[].next；子流程放 childFlow（完整子 Flow，含自身 start/end）。

## 可用节点类型
{nodes_digest}

## 回复要求
- 除 plaita-flow 代码块外，用简洁中文说明你做了什么、为什么。
- 用户需求含糊时先提问，不要臆测节点参数。
"""

# 惰性单例（构造需读 providers.json，避免 import 时 IO）
_agent = None


def _provider_config(provider: str = "glm-52") -> tuple:
    """从 flowcast providers.json 读取 (api_key, api_base, model)。"""
    path = os.path.expanduser("~/.flowcast/providers.json")
    with open(path) as f:
        cfg = json.load(f).get(provider) or {}
    return cfg.get("apiKey") or "", cfg.get("apiBase") or "", cfg.get("model") or "GLM-5.2"


def _build_model():
    provider_name = os.getenv("PLAITA_CONSOLE_COPILOT_PROVIDER", "glm-52")
    api_key, api_base, model_name = _provider_config(provider_name)
    if os.getenv("PLAITA_CONSOLE_COPILOT_MODEL"):
        model_name = os.getenv("PLAITA_CONSOLE_COPILOT_MODEL")
    if not api_key:
        raise RuntimeError(
            f"flowcast providers.json 缺少 provider '{provider_name}' 的 apiKey"
        )
    provider = AnthropicProvider(api_key=api_key, base_url=api_base or None)
    return AnthropicModel(model_name, provider=provider)


def _system_prompt() -> str:
    try:
        digest = _available_nodes_digest()
    except Exception as exc:  # noqa: BLE001
        logger.warning("节点摘要生成失败: %s", exc)
        digest = ""
    return COPILOT_SYSTEM_PROMPT.replace("{nodes_digest}", digest)


def get_flow_agent():
    """构造（首次）并返回编排 agent。构造失败抛异常，由路由层转为 503。"""
    global _agent
    if _agent is None:
        agent: Agent[None, str] = Agent(
            _build_model(),
            instructions=_system_prompt(),
            retries=2,
        )

        @agent.tool_plain
        def validate_flow(flow_json: str) -> str:
            """用引擎强校验一份 flow IR 是否合法。传完整 IR 的 JSON 字符串。"""
            from plaita.core.flow import Flow

            try:
                Flow.model_validate(json.loads(flow_json))
                return "ok：flow IR 合法"
            except Exception as exc:  # noqa: BLE001
                return f"invalid：{exc}"

        @agent.tool_plain
        def dry_run_flow(flow_json: str, flow_input: str = "{}") -> str:
            """真实试跑 flow（引擎执行），返回每个节点状态与错误。
            flow_input 为流程入参 JSON（$INPUT），不确定时传 {}。"""
            try:
                out = dryrun_svc.dry_run(flow_json, json.loads(flow_input or "{}"))
            except json.JSONDecodeError as exc:
                return f"flow_input 不是合法 JSON: {exc}"
            nodes = "\n".join(
                f"[{n['status']}] {n.get('id')}"
                + (f"（{str(n.get('error'))[:120]}）" if n.get("error") else "")
                for n in out["nodes"]
            )
            return f"flowError: {out['error'] or '无'}\n节点执行:\n{nodes or '（无）'}"

        @agent.tool_plain
        def list_node_types() -> str:
            """列出当前环境可用的全部节点类型。"""
            return _available_nodes_digest()

        _agent = agent
    return _agent


def flow_agent_available() -> bool:
    """依赖与凭证就绪即可用（构造失败延迟到请求时报告）。"""
    try:
        _, _, _ = _provider_config(
            os.getenv("PLAITA_CONSOLE_COPILOT_PROVIDER", "glm-52")
        )
        return bool(_provider_config(os.getenv("PLAITA_CONSOLE_COPILOT_PROVIDER", "glm-52"))[0])
    except Exception:  # noqa: BLE001
        return False
