# duo-a 第三十六轮 ② 证明：集群注册表 create/delete 多资源写盘补偿

- 单号：duo-a-ledger 第三十六轮 ②（审计 P2，无 GitHub issue）
- 基线：`origin/main` = `786de97`
- 边界：只动 `plaita-console/backend/services/cluster_registry.py` 的
  `create_cluster` / `delete_cluster`；#244 的路径归属闸、#249 的原子写
  `_save_registry` 与载入隔离均不改。

## 根因（亲读）

两个方法各自要协调两处资源，旧实现没有失败补偿：

```python
# create_cluster
cluster_dir.mkdir(...)                 # 1. 建目录
self._create_default_config(config_path)  # 2. 写配置
self._clusters[cluster_id] = info
self._save_registry()                  # 3. 写 registry；抛错则 1/2 残留

# delete_cluster
shutil.rmtree(cluster_dir)             # 1. 先毁目录（不可逆）
del self._clusters[cluster_id]
self._save_registry()                  # 2. 抛错 → disk registry 仍登记，
                                       #    目录已毁 = ghost 条目
```

- **create 失败残留目录**：`_save_registry` 失败（ENOSPC 等）时目录和配置
  已落盘，内存登记已回滚；重试 `create_cluster` 不会被「已存在」拦住（登记
  为空），但会覆盖/遗留无主目录，且列表里看不到它。
- **delete save 失败 ghost**：`rmtree` 先执行，保存失败时 `registry.yaml`
  （#249 原子写保证是旧内容）仍登记该集群，但目录已没了；内存里也删了，
  调用方无法重试清理——列表看得到、点开配置全空。

## 修法

### create：补偿式事务

- 记录本次**新建**的目录 / 配置文件（目录或文件原本存在则不动）；
- 配置写盘失败、registry 保存失败两条路径都调用
  `_rollback_created_cluster`：删本次新建的配置文件和目录，并回滚内存登记；
- 显式传入的 `config_path` 视为调用方资源，只回滚登记，不删文件。

### delete：暂存改名 + 落盘后真删

1. `os.replace(cluster_dir, <base>/.<id>.deleting.<ts>)`——同目录同文件系统
   原子改名，此时数据仍完整；
2. 删内存登记 + `_save_registry()`；
3. 保存失败：复原内存登记，`os.replace` 把目录改回原路径后重新抛错——
   登记与目录保持一致，调用方可安全重试；
4. 保存成功：`shutil.rmtree` 清理暂存目录；清理失败只记 WARNING（登记已
   一致，残骸可人工回收，不再是 ghost）。

## 测试（5 条，`test_registry_crud_compensation.py`）

- create save 失败 ⇒ 无 `<base>/<id>/`、无内存登记、registry 文件逐字节
  不变；恢复后重试 create 成功；
- create 配置写盘失败 ⇒ 刚建的目录被回收；
- create save 失败 + 显式 `config_path` ⇒ 调用方文件保留；
- delete save 失败 ⇒ 目录与配置仍在、内存登记复原、磁盘 registry 不变、
  无 `.deleting.*` 暂存残骸；重启后登记/配置一致；恢复后重试删除成功；
- delete 成功路径 ⇒ 目录与登记一起消失，无暂存残骸。

## 负控

仅回退 `services/cluster_registry.py`（保留新测试）：
**4 failed / 1 passed**（成功路径应绿）。精确红文案见
`docs/proofs/data/duo36-registry-compensation-negative-control.txt`。

## 验收实跑

- 新文件 **5 passed**；
- 相邻集群/凭据共 8 文件（新 2 + #213 两份 + #244/#249/cluster_switch/
  cluster_tenant/service_manager_reset）**65 passed**；
- console 全套 **590 passed / 13 skipped**；
- ruff 仅既有 BLE001（改动行零新增）。
