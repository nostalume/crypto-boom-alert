# OKX 候选运行与恢复

适用：当前 `okx-live.yml` / `okx_live` 候选。读者是仓库维护者。
它只支持手动触发，默认 **preview**，没有定时任务；不代表旧 Binance 任务已切换。
GitHub 特定行为以对应 run 的实际结果为准。代码或运行方式改变时，应一起复核本说明。

## 先看运行摘要

打开目标 run 的 Summary；成功不等于全部产品有可判断的信号，更不等于邮件到达。

| 字段 | 如何解释 |
|---|---|
| `mode` / `publication` | preview 为非通知；`not_sent` 未发告警，`commit_observed` 只证明 Git 提交被观察到 |
| `blocked` | 非空表示诊断模式，本轮不持久化，也不能据此开启通知 |
| `at` | OKX 已收盘数据的截断时间，UTC 毫秒；不是开跑时间或收件时间 |
| `pool` / `complete` | 当前准入产品数 / 完成本轮必需数据计算的产品数；现货和永续分别计数 |
| `partial` / `deferred` | 数据不全 / 预算等原因未完成，不能解释为“没有异常” |
| `warmup` / `already_evaluated` | 已评估但历史不足 / 该截断点已经计算过，不等于新的完整计算 |
| `coverage_capped` | 标的池是否被限制；首次人工基线要求完整准入池的进度已齐 |
| `http_attempts` / `http_retries` / `response_bytes` | 本轮行情请求次数、重试和读取字节；不包含所有 GitHub/安装流量 |
| `observed_at` / `scan_elapsed_seconds` / `cutoff_age_ms` | prepare 验证扫描后观察时间、子进程扫描与等待耗时、该时刻距数据截止的毫秒；不包含调度、安装或邮件投递 |
| `market_coverage` | 按 SPOT / SWAP 分别统计各扫描状态；不能用整体比例掩盖单个市场缺失 |
| `transition_counts` / `transition_age_ms` | 阶段变化数和变化时间的年龄范围；包括 ENDED 与过期记录，不是可发邮件数；无变化时范围为 null |
| `generation` / `final_key` | 提交代次 / 当前精确持久化对象；保存失败不能用较早成功掩盖 |
| `manifest_sha256` / `checkpoint_sha256` | 供独立前序证明交叉核对的摘要，不是送达回执 |

有发送时，consumer 摘要主要给出提交、final 状态与 `email` 边界；扫描覆盖在 prepare 摘要中。
延迟应分别看调度/排队、scan step、数据 `at` 和真实收件时间，不把 1m 输入当作 1m 送达保证。

## 读取有限观察记录

以下命令只读日志。先将 `RUN_ID`、`PREPARE_JOB_ID` 替换为实际运行及 prepare job ID：

```text
gh run view RUN_ID --repo nostalume/crypto-boom-alert --job PREPARE_JOB_ID --log
```

- `OKX_RUN_V1` 后是一行 JSON，与该 job 的 UI Summary 相同；准备阶段失败时可能只有失败状态，不能当成零事件。
- `OKX_TRANSITION_V1` 是已验证扫描的逐条变化，保留 `run_id`、`source_sha`、事件 `identity`、`stage`、`at`、`reason`。重复核对用事件 identity **加 stage**，不是只按币名去重；首次 OBSERVED 和后续 PERSISTENT 不算重复通知。现货、永续也不是同一个产品。
- 这些日志不替代最终 cache / 独立 Jobs proof。只把成功、无 blocked、有完整 final 证明的固定源码 prepare 纳入连续观察；其余单列为失败、诊断或缺口，不补成正常样本。
- 实际窗口长度、相邻数据截止间隔、排队和步骤耗时从真实 run / job 时间与日志核对，不把“10 分钟 cron”当成实际扫描间隔。preview 不发邮件；只能评估事件负担，不能声称收到多少邮件或邮件送达时延。

观察期间冻结规则及源码。当前仍未启用定时；约 24 小时有限观察、到期停止及限定清理须另获授权。此前验收 cache 已清理，新的观察须先检查专用对象确实为空，再显式 `preview rebuild=true`；这不是重新验收 Stage 3 或发送 TEST 邮件。

## 只读核对与手动 preview

前提：仓库仍公开，使用标准 GitHub-hosted runner；已登录 `gh`。
不要新增收费额度或付款方式作为故障兜底；保留账户已有 $0 停用预算。
脚本会拒绝未知或超上限的 repo cache usage，不承诺平台未来永远免费。

以下在任意本地 shell 执行，只读取远程状态（不需要把 token 写入文件）：

```text
gh run list --repo nostalume/crypto-boom-alert --workflow okx-live.yml --limit 5
gh cache list --repo nostalume/crypto-boom-alert --limit 100
```

**下列命令会启动远程任务并保存/精确清理专用 cache，需要明确运行授权。**
preview 不生成行情提交，也不会修改旧生产 workflow；GitHub 自带 Actions 通知仍可能出现。

```text
gh workflow run okx-live.yml --repo nostalume/crypto-boom-alert --ref main -f mode=preview -f rebuild=false
```

期望：prepare 完成，consume 跳过；摘要无 `blocked`，存在 `final_key`。
下一轮必须采用紧邻的成功 attempt1，验证独立 Jobs final 证明、精确 cache 和全部 hash；
不可自行选择“最近成功”绕过失败。缓存可被淘汰，默认最多保留一轮 final，保存/交接时有严格临时上界。

## 故障与人工恢复

| 现象 | 安全处理 |
|---|---|
| cache miss、损坏、过期、代码不兼容、proof 缺失 | 保持非通知诊断；不要将 cache-hit 或成功退出单独当作恢复证据 |
| 前序失败、pending/unknown、多余或身份漂移的对象 | 停止通知；保留对象、run 和提交证据，先判断是否存在未知发布，不自动重发或清账本 |
| final 保存或清理失败 | run 不能成为可信前序；精确核对本次对象，不批量删整个仓库缓存 |
| 免费额度读不到或不满足 | 不保存、不扩容；恢复可读的额度证据后再评估 |

没有可采用状态时，显式 `rebuild=true` **仅支持 preview 且专用完整清单为空**。
它重新建立非通知进度，不会删除未决对象。只有授权并核对精确对象身份后才可人工清理；
不要用删除 cache 来假装不明邮件从未发出。此工具不提供通用 unknown 解锁或 exactly-once 保证。

## 将来切换与回滚（不是已执行操作）

1. 先完成限定 preview 的跨 run 验收与前向非通知观察，确认覆盖、时延和缓存边界。
2. 获切换授权后停旧 hourly writer，确认五种活跃/等待状态清单为空，再设置 `OKX_LIVE_ENABLED=true`。
3. 首次手动 live 提供已审阅前序 final 的 `arm_manifest`；它还须通过完整池进度、checkpoint 与未决账本检查。
   已 armed、兼容且无未决的 final 可以续接；preview 会再次关闭通知，重新启用需再批准。
4. 定时 10 分钟配置尚未启用，应作为单独切换变更验证；不要并行运行两个生产 writer。
5. 回滚先停新 writer 并核对未决/已确认提交，再恢复旧 writer；不能用旧成功状态群发积压告警。

## 本地离线检查

从仓库根目录，在已有 Python 环境运行（PowerShell；其他 shell 同样设置 `PYTHONPATH=src`）：

```text
$env:PYTHONPATH = 'src'
$env:PYTHONDONTWRITEBYTECODE = '1'
uv pip install --python python --no-cache -r requirements.txt -r requirements-dev.txt
python -m pytest -q -p no:cacheprovider
python -m ruff check --no-cache --select F,I,RUF100 src tests
```

安装会访问依赖源；测试和 lint 不启动市场扫描、发布或远程 cache。
如果 uv 是失效的 shim，请改用已安装的 uv.exe 真实路径，不要为此改变机器配置。
只读 CI 候选仅在 PR 或手动事件执行这些检查，不含行情 job、写权限、secret、cache 或 artifact 上传。
CI workflow 未获线上运行验收时，本地语法/测试通过不能代替其实际 GitHub 结果。
