"""Agent 编排示例：自定义 LLM 相关节点 + 真实可跑案例。

子模块：

- :mod:`nodes`   —— ``LLMNode`` / ``ToolNode`` / ``RetrieverNode`` 与 ``FakeLLM``
- :mod:`tools`   —— 注册示例工具函数
- :mod:`corpus`  —— 注册示例检索语料
- :mod:`demo`    —— 三个案例的可运行入口

运行::

    python -m examples.agent.demo
"""
