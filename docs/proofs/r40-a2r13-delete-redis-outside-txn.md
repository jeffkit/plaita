# A2R13 证明：租户删除的 Redis 清理移出 SQLite 写事务

- 来源：r38（A50-01/02/05）回归审计 A2R13（P2）
- 边界：只动 `services/tenants_svc.py` 的 `delete_tenant` /
  `_purge_orphan_tenant_runtime`；`BEGIN IMMEDIATE` 串行化点与世代令牌
  （A50-01/02）保持不变。

## 根因

r38 为关闭「删除 × 并发写」窗口，先 `BEGIN IMMEDIATE`，随后在**同一事务
内**执行 Redis 全量清理（SCAN 租户命名空间、平台键逐条收窄、共享队列
遍历）。清理是秒级操作，SQLite 写锁因此被持有数秒：

- 并发写他租户等满 5s timeout → `database is locked`；
- 清理/提交失败时连接未正常归还，连接池被 PENDING 状态拖住，后续请求
  全站 500 直到重启。

## 修法

1. DB 删除（凭据旁文件按 #257 仍先于 commit 清）与**墓碑**（廉价 HSET）
   先提交——写锁毫秒级释放，墓碑仍在清理窗口内拦下在途消息；
2. Redis/队列清理移到事务**之外**（`_purge_runtime_after_commit`）；
3. 两步各自尝试后统一 loud 上抛：租户行已删，重试 `delete_tenant` 走
   404 补清扫继续清理（#257「可重试、不伪装成功」）；404 路径同样
   **墓碑先落**，再补偿清理。

## 负控（见 `data/r40-negative-controls.txt`）

把两步清理挪回 `session.commit()` 之前：

- `test_slow_redis_purge_does_not_block_other_tenant_write` 红：
  他租户写被阻塞 **1.24s**（清理时长 1.2s）；
- `test_redis_failure_after_commit_leaves_pool_healthy` 与 #255 补偿用例红：
  失败后租户行仍在（旧回滚语义），与「DB 已提交 + 404 补偿」预期不符。

恢复后 A2R13 新文件 **2 passed**；#255/#257/A50 定向 **15 passed**。
