# 采集审计台 (Collection Audit Console)

从 **批次 → 来源 → 原始文件 → 解析记录 → 下游实体** 的全链路采集审计台。
对错误批次发起纠错任务：纠错只生成新版本、原结论完整保留，依赖旧版本的下游任务按影响范围重新排队。
页面展示依赖图、纠错状态、冲突原因与重跑结果，并带并发纠错与任务取消的自动化验证。

零依赖：Python 3.8+ 标准库（`http.server` + `sqlite3` + `threading`）+ 原生前端，无需 pip/npm install。

## 运行

```bash
python3 run.py --port 8000          # 首次自动装载演示数据
python3 run.py --port 8000 --reseed # 重置并重新装载演示数据
# 打开 http://127.0.0.1:8000
```

## 验证（12 个用例）

```bash
python3 -m unittest discover -s tests -v
```

覆盖：

| 验证点 | 用例 |
|---|---|
| 血缘追溯 | 批次→来源→文件→记录→实体每一跳可 JOIN 通；错误记录带错误信息；聚合实体落库 |
| 纠错语义 | 生成 v+1；v1 的错误记录/实体原样保留（`superseded`）；fix/drop/add 三种纠错项生效 |
| 影响范围重排队 | 旧版本下游任务被取代/取消，新版本 extract→aggregate 重新入队并跑出正确实体数 |
| **并发纠错** | 两个基于同一版本的纠错同时应用（`threading.Barrier` 对齐起跑）：恰好一个 `done`，另一个 `conflicted` 且带冲突原因；版本号只前进一次 |
| 过期基线 | 基于 v1 的纠错在批次已到 v2 时提交 → `conflicted`，原因含 v1/v2 |
| **任务取消** | 排队任务取消并级联取消下游；运行中任务协作式取消且部分写入回滚；待执行纠错取消不产生新版本；运行中纠错取消整体回滚（版本号不变、无 v2 行）；已结束任务拒绝取消 |

## 架构

```
run.py               入口
app/db.py            SQLite schema(WAL) + 连接/事件日志
app/pipeline.py      任务引擎: 队列/Worker/取消/纠错应用/影响范围重排队
app/server.py        HTTP API + 静态文件
app/seed.py          演示数据(2 来源 / 4 批次 / 含错误记录)
static/              SPA: 依赖图(SVG DAG) + 5 个数据页签
tests/               上述验证套件
```

### 关键设计

**纠错 = 只增不改。** `apply_correction` 复制基线版本的文件与记录并应用纠错项
（fix 合并 patch / drop 跳过 / add 补录），生成 `batch_versions` 新行；
旧版本及其记录、实体全部保留并标记 `superseded` —— 页面上"已被取代（原结论保留）"。

**并发安全 = 两阶段。**
阶段一（不持写锁）：协作式取消窗口，取消标志此时可被其他连接提交并被检查点看到；
阶段二（`BEGIN IMMEDIATE`，无睡眠）：冲突检查 + `UPDATE batches SET current_version=? WHERE current_version=?`
CAS + 全部写入 + 提交。写锁使并发应用串行化，恰好一个成功，其余落为
`conflicted` 并记录人读冲突原因（被哪个纠错推进到了哪个版本）。

**影响范围重排队。** 纠错提交时计算受影响记录键集合，旧版本上 queued 的下游任务标记
`superseded`、running 的发协作式取消，然后为新版本入队 extract→aggregate 链
（`supersedes` 字段串起替换关系）。纠错详情页展示影响记录、被取代任务与重跑任务的结果。

**取消 = 协作式。** 排队任务立即 `cancelled` 并级联取消依赖者；运行中任务置
`cancel_requested`，Worker 在步骤间用**新连接**检查（事务内连接的 WAL 快照固定，
看不到取消），触发后回滚部分写入。纠错任务在提交前同样设有检查点。

## API 摘要

```
GET  /api/overview                     总览指标
GET  /api/sources | /api/batches       来源 / 批次(含纠错计数)
GET  /api/batches/{id}                 批次详情(版本+纠错)
GET  /api/batches/{id}/lineage         依赖图数据(源→批次→版本→文件→记录→实体+任务)
GET  /api/records/{id}                 记录详情+溯源链+下游实体
GET  /api/entities/{id}                实体详情+溯源链
POST /api/batches/{id}/corrections     发起纠错 {reason, base_version?, items:[{action,record_key,patch}]}
GET  /api/corrections?batch_id=        纠错列表(状态/冲突原因/影响范围)
GET  /api/corrections/{id}             纠错详情(纠错项+影响+重跑结果)
POST /api/corrections/{id}/cancel      取消纠错
GET  /api/tasks?batch_id=              任务队列
POST /api/tasks/{id}/cancel            取消任务
GET  /api/events                       审计事件流
POST /api/seed                         重置演示数据
```

## 页面

- **依赖图**：来源→批次→版本→文件→记录→实体六层 DAG。节点类型用形状+文字区分，
  状态用校验过的状态色+图标双编码（✓ 正常 / ⚠ 警告 / ✕ 错误 / ▶ 流转 / ↺ 已取代），
  被取代版本虚线展示；悬停看摘要，点击看溯源链。
- **解析记录 / 下游实体**：按版本与状态过滤；实体页可勾选"显示被取代的原结论"。
- **纠错任务**：发起表单（修正/剔除/补录），列表展示状态芯片，详情展示冲突原因（红框）、
  影响范围（黄框）与下游重跑结果。
- **任务队列 / 审计事件**：任务状态、依赖、取代链、取消按钮；事件流记录全部状态变迁。
