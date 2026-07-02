.PHONY: coverage mutation mutation-recheck clean-mutants

## 单元测试覆盖率（gate 范围，排除 integration/e2e 与可选后端）
coverage:
	python -m pytest tests/ -m "not integration and not e2e" \
		--cov=plaita --cov-report=term --cov-report=html \
		--cov-fail-under=79

## 变异测试：并行初筛（~1.5min）。结果用 mutmut results 查看；
## timeout 类为假阳性，必须再跑 make mutation-recheck。
mutation:
	rm -rf mutants .mutmut-cache
	mutmut run
	@echo "→ 用 'mutmut results' 查看；'mutmut show <id>' 看具体变异点"
	@echo "→ 务必再跑：make mutation-recheck"

## 复核 mutmut 标记为 timeout 的变异点真实状态（写入 mutation-recheck.txt）
mutation-recheck:
	bash scripts/recheck_mutants.sh timeout

clean-mutants:
	rm -rf mutants .mutmut-cache
