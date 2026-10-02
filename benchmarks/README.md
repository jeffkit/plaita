# benchmarks —— 性能基准与复现环境

BFF 热路径专项（2026-10，四波）的全部数字出自本目录，均可复跑。脚本开头
都会打印并自证被测对象（`plaita.__file__` 必须指向你当前的工作副本）。

## 环境搭建（一次性，~3 分钟）

不要用系统 Python，也不要复用其它项目的 venv（root checkout 的 `.venv`
可能是其它常驻进程的共享环境）。任意位置起一个干净环境即可：

```bash
git clone git@github.com:jeffkit/plaita.git && cd plaita
# 或者：git worktree add .worktrees/bench main
python3.13 -m venv .venv
PATH="$PWD/.venv/bin:$PATH" pip install -e ".[dev,lint,all]"
```

要点：

- `all` extras 含 `fast`（orjson）——`bench_json_payload.py` 的 orjson 对照
  列与 HTTP 节点的快速路径需要它；不装则自动回退标准库（对照列消失）。
- 从 worktree/子目录跑测试与基准时统一带前缀：
  `PATH="$PWD/.venv/bin:$PATH" PYTHONPATH="$PWD"`，并先验证落点：
  `python -c "import plaita; print(plaita.__file__)"`。
- 基准目录不进 wheel、不进 coverage 门（pyproject 只收 `plaita*` 包）。

## 脚本清单与口径

| 脚本 | 回答什么问题 | 关键口径 |
|---|---|---|
| `bench_expression.py` | 表达式编译缓存收益 | 固定配比语料；hit=缓存命中、miss=真冷解析、>4KB 长模板单独列示（独立二级 LRU）；加权总账不含长模板 |
| `bench_http_overhead.py` | HTTP 连接复用收益 | 层 1 loopback 是 CPU 侧下限；层 2 用 `BENCH_HTTP_DELAY=0.05` 注入 RTT 看墙钟；DNS-TTL 收益 loopback 不可测 |
| `bench_flow_overhead.py` | 每节点引擎税 + observer 派发交叉点 | 流 A 用 5/50 节点回归剥离 per-run 固定成本；流 C 是**标定型合成口径**（stub 自旋模拟 handler 成本），GIL 交叉点以此为准 |
| `bench_json_payload.py` | JSON 后端按 payload 分层 | 大 payload 档（≥128KB）orjson 是唯一有效杠杆；验收线必须按 payload 分层报 |
| `bench_checkpoint_step.py` | 每步 checkpoint 成本构成 | 引擎侧 to_dict 为浅拷贝（O(键数)）；随状态增长的是消费侧 json.dumps（engine 外） |

历史基线数字见各专项 commit message（`ec1a0de` / `0d79d63` / `70fb50e` /
`245bccf`）。机器有噪声，脚本已内置 warmup + min-of-N，跨机对比时同机
A/B 才算数。

## 变异测试（改 core 模块后）

单模块配方与纪律见 `docs/mutation-testing.md` §7——要点：临时把
pyproject `[tool.mutmut]` 的 `only_mutate` 收窄到目标模块、test selection
收窄到对应测试文件，跑完 `git checkout` 恢复且临时改动**不进 commit**；
`survived` 结果必须经 `scripts/recheck_mutants.sh` 独立进程复核（历史上
连续多轮 85 个 survived 全为 worker 假阳性）。async 模块（runner /
async_utils 等）不走并行 mutmut，按 docs §6/§8 逐点或记录延后。
