#!/usr/bin/env python3
"""plaita#68 差分 oracle：用"同一份差分电池"判定变异体是否真的等价。

用法::

    python scripts/mutation68/oracle.py <repo>/mutants scripts/mutation68/battery.py ids.txt [workers]

对每个变异体，在**两组环境配置**（默认 / ``floor=factor=1, hard=7``）下各跑一遍
``battery.py``，与不带 ``MUTANT_UNDER_TEST`` 的基线逐 key 比较：

* key 形如 ``priv:*`` → 私有 helper 的内部返回值；
* 其它 key → 公开路径（算子返回值 repr 或异常类型+文案）；
* 公开路径有差异 → ``DIFFERS``（**非等价**，必须补测试杀掉或说明为什么不该杀）；
* 只有私有 helper 有差异 → ``PRIV-ONLY``（同样算非等价，但影响面小）；
* 全同 → ``NO-DIFF``。

**口径**：``NO-DIFF`` 只是"在这份电池的覆盖范围内观察不到差异"，是等价性的
**证据**而不是数学证明；报告里必须带上电池的规模与判定 key 的口径。
"""
import argparse
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

ENVS = [
    {},
    {"PLAITA_EXPR_MAX_RESULT_SIZE": "1", "PLAITA_EXPR_GROWTH_FACTOR": "1",
     "PLAITA_EXPR_MAX_RESULT_HARD": "7"},
]
TIMEOUT_SECONDS = 90.0


def run_battery(python: str, tree: Path, battery: str, env_extra: dict, mutant: Optional[str]):
    env = {"PYTHONPATH": str(tree), "PATH": "/usr/bin:/bin:/usr/local/bin",
           "HOME": str(Path.home())}
    env.update(env_extra)
    if mutant:
        env["MUTANT_UNDER_TEST"] = mutant
    try:
        proc = subprocess.run([python, battery], env=env, capture_output=True,
                              text=True, cwd=str(Path(battery).resolve().parent),
                              timeout=TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        return {"_crash": f"TIMEOUT({TIMEOUT_SECONDS:.0f}s)"}
    if proc.returncode != 0 or not proc.stdout.strip():
        return {"_crash": (proc.stderr or proc.stdout)[-300:]}
    return json.loads(proc.stdout)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tree", help="mutants/ 目录（作为 PYTHONPATH）")
    parser.add_argument("battery", help="差分电池脚本（本目录 battery.py）")
    parser.add_argument("ids", help="每行一个完整 module.mutant_name")
    parser.add_argument("workers", nargs="?", type=int, default=6)
    parser.add_argument("--python", default=sys.executable)
    args = parser.parse_args()

    tree = Path(args.tree).resolve()
    ids = [line.strip() for line in Path(args.ids).read_text().splitlines() if line.strip()]
    baselines = [run_battery(args.python, tree, args.battery, env, None) for env in ENVS]

    def judge(mutant_id: str) -> str:
        public, private = [], []
        for index, env_extra in enumerate(ENVS):
            got = run_battery(args.python, tree, args.battery, env_extra, mutant_id)
            base = baselines[index]
            for key in sorted(set(base) | set(got)):
                if base.get(key) != got.get(key):
                    item = (f"env{index}:{key}: orig={str(base.get(key))[:70]} "
                            f"mut={str(got.get(key))[:70]}")
                    (private if key.startswith("priv:") else public).append(item)
        if not public and not private:
            status = "NO-DIFF"
        elif public:
            status = "DIFFERS"
        else:
            status = "PRIV-ONLY"
        return f"{mutant_id}\t{status}\t{(public or private or [''])[0]}"

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        for line in pool.map(judge, ids):
            print(line, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
