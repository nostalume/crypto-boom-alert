# Crypto Boom Alert

只读 OKX 公共行情，监测 USDT 现货与 USDT 永续的价量异常；不交易、不需要交易所密钥。
本地支持单次/前台定时预览；GitHub Actions 生产目标每 10 分钟运行一次，但可能延迟或漏触发。

## 何时提醒（GitHub 生产模式）

生产使用 A0：最近 5 根已完成 1 分钟 K 线的 USDT 成交额至少 100,000，
相对前 7 天已完成 5 分钟成交额均值至少 3 倍，且 5 分钟价格变动绝对值至少 0.5%。
小时 EMA20 只作背景，不是硬门槛；这里比较的是成交额与成交额，不是成交量与 VWAP。

新事件进入 OBSERVED 或首次进入 PERSISTENT 时，符合时效且未发送的变化合并为一条
`OKX ALTCOIN ALERT ...` 提交，沿用仓库已有 GitHub push 邮件通知。无新变化不提交。
超过 24 小时的事件不发；30 分钟内标为 TIMELY，其余为 BACKFILL。
PERSISTENT 是事件跟踪阶段：中间不足 10 次的条件不满足不一定重置计时，不能理解为严格连续达标。
ENDED 不发邮件，UP/DOWN 是观测方向，不是买卖建议或收益概率。

## 如何读邮件

打开邮件关联的**那次提交**查看新增告警表格，而不是只看 main 的最新 README。
标题时间是批次生成 UTC 时间；表中“首次条件 / 本次条件 / 条件年龄”说明行情发生时间。
及时项优先，同类 PERSISTENT 优先，再按币种和产品身份稳定排序；不是量化收益排名。
现货与永续保留独立记录，同批出现不同方向可能对应不同时间的事件。
工程提交、Actions 结果通知和带 TEST ONLY 的测试邮件都不是真实行情告警。
提交确认不等于邮件送达，页面不更新也不等于扫描失败；覆盖与失败看 Actions summary。

## 本地运行：一个入口

只需安装 [uv](https://docs.astral.sh/uv/getting-started/installation/)，并在项目根目录运行以下命令。
Windows、Linux、macOS 使用相同入口，不需要手动设置 PYTHONPATH、激活虚拟环境，
也不需要填写本机 Python/uv 的绝对路径。`uv run --locked` 按 `pyproject.toml + uv.lock`
准备环境；首次可能需要联网下载运行时和依赖，之后复用 `.venv`。
[uv 项目执行说明](https://docs.astral.sh/uv/guides/scripts/)。

**本地仍是预览：不发邮件、不调用 GitHub、不交易，也不导入云端账本。**

### 执行一次

```sh
uv run --locked --no-dev alert.py once
```

默认最多 1000 标的、4 个 worker；只执行一轮并退出。小池试跑：

```sh
uv run --locked --no-dev alert.py once --limit 8
```

打开相对路径 `.local-preview/preview.txt` 查看结果，`.local-preview/scan-report.json`
保留逐标的覆盖、缺口与请求诊断。终端 JSON 包含耗时、generation 和账本保留结果。
`--workers 8` 可调整并发，但仍受原速率、2400 请求、480 秒扫描进程预算约束。
冷启动可能无法一轮下载全池；再次使用同一目录即可增量续接。
`coverage_capped=true` 或 `partial/warmup/deferred` 不代表全池健康，更不代表没有异常。

### 定时执行

```sh
uv run --locked --no-dev alert.py watch --interval 10
```

启动后立即扫描一次，再按分钟周期执行。**这是前台循环，不是安装到操作系统的定时任务**：
保持该终端运行；不会自动开机启动，电脑休眠/关机时不会扫描。每轮结束后才启动下一轮，
超时错过的轮次跳过，不堆积、不并发抢写。网络失败会显示错误并等到下一周期重试；
损坏检查点、状态身份错误或无法确认子进程退出时停止，不自动清空账本。

查看本地控制记录（不扫描、不联网）：

```sh
uv run --locked --no-dev --offline alert.py status
```

### 取消定时执行或中断扫描

在运行终端按 **Ctrl+C**，或者在另一个终端进入同一项目目录执行：

```sh
uv run --locked --no-dev --offline alert.py stop
```

`stop` 是本地取消请求，不依赖 OKX 或 GitHub，所以断网也能取消。
它会停止定时循环，正在扫描时由原进程监督器终止其自己创建的扫描子进程，
确认退出后释放锁；不会结束其他程序。最多等待 10 秒确认，超时会明确显示
`stop_requested_not_confirmed`，不能将“已经请求”当作“已经停止”。
单次 `once` 没有定时实例，直接在其终端按 Ctrl+C 取消。
首次扫描前会先保存非通知检查点，避免冷启动中断后只剩行情缓存无法续接。
取消不删除行情缓存、检查点或账本；检查点可能是中断前或中断后完整版本，续接以磁盘为准。

### 突然停机后恢复

前台进程不会在重启后自行恢复。若 `status` 显示残留记录/锁，先确认旧终端及扫描进程
已经结束（机器刚重启也满足这一前提），再执行：

```sh
uv run --locked --no-dev --offline alert.py recover --confirm-stopped
uv run --locked --no-dev alert.py watch --interval 10
```

`recover` **只清除残留定时记录、取消请求和 writer 锁**，保留检查点、缓存、账本。
它不通过 PID 自动判断或杀进程；**旧进程还活着时禁止使用**，否则可能解除它的写锁。
检查点损坏或缓存/报告存在但检查点缺失时，清锁也不会绕过检查；不要删账本“修复”。

### 数据与其他选项

默认数据目录 `.local-preview/` 已被 Git 忽略；同目录中的两个命令才会操作同一实例。
需要隔离试跑时，可使用项目内相对目录（`--workspace` 放在子命令前）：

```sh
uv run --locked --no-dev alert.py --workspace .local-preview/demo once --limit 8
uv run --locked --no-dev --offline alert.py --workspace .local-preview/demo status
uv run --locked --no-dev alert.py --help
```

行情缓存总上限 512 MiB，检查点 8 MiB，最新 JSON/文本报告各 16 MiB；报告不按轮次累积。
预览只推进本地事件检查点，既有未启用通知的账本条目原样保留，不增加/清理/确认发送记录。
已启用通知的生产状态、链接路径、损坏检查点、残留锁均拒绝使用。
预览按变化时间排列，含结束等诊断事件，不是通知队列或收益排名。
失败后旧报告可能仍在，须结合命令退出结果、数据截止时间与 generation 阅读。
若检查点已保存而报告写入失败，下一轮可能不再展示该轮变化；本地预览不承诺历史通知可靠投递。

## 运行与检查

[运维说明](docs/okx-operations.md) 包含暂停、状态恢复和免费边界。
当前使用标准 GitHub-hosted runner、严格限额的 native cache，不上传 artifact。
缺失、不兼容或未知发布状态停止通知，不自动清空账本重发。

```sh
uv run --locked pytest -q -p no:cacheprovider
uv run --locked ruff check --no-cache --select F,I,RUF100 alert.py src tests
```

依赖以 `pyproject.toml + uv.lock` 为唯一来源；Python 3.12/3.13，非打包项目。
生产只安装运行依赖，CI 安装 dev 组。更新依赖须显式更新并检查 lock，不在运行时自动升级。
上述命令自动准备 dev 组，测试导入路径由项目配置管理。无需单独运行 Python。
普通源码更新按现有格式/规则版本续用兼容缓存，不要求源码摘要相同；摘要仅用于追溯。
只有持久化格式、告警规则或去重身份不兼容时才需要显式迁移，不能清空账本重发。

<!-- OKX_ALERT_START -->
## 最近 OKX 告警 / Latest alerts

源数据截止：2026-10-12 03:21:00 UTC。本批生成：2026-10-12 03:23:57 UTC / 上海 2026-10-12 11:23:57。
仅记录价量异常；UP/DOWN 是观测方向，不是买卖建议。PERSISTENT 表示持续条件，不代表收益确认。
本页不是实时扫描健康状态；无新告警不更新，扫描覆盖/失败请查看 Actions summary。

展示顺序：及时项优先、同类持续项优先，再按币种分组；现货/永续保留独立事实，不是收益概率排名。

| 币种分组 | 标的 | 市场 | 方向 | 规则 | 阶段 | 首次条件 | 本次条件 | 条件年龄 |
|---|---|---|---|---|---|---|---|---|
| ARX | ARX-USDT-SWAP | SWAP | DOWN | A0 | PERSISTENT | 2026-10-12 03:13:00 UTC | 2026-10-12 03:18:00 UTC | 5m · 及时 / TIMELY |
| MUBARAK | MUBARAK-USDT-SWAP | SWAP | DOWN | A0 | PERSISTENT | 2026-10-12 03:12:00 UTC | 2026-10-12 03:17:00 UTC | 6m · 及时 / TIMELY |
| GRASS | GRASS-USDT-SWAP | SWAP | DOWN | A0 | OBSERVED | 2026-10-12 03:18:00 UTC | 2026-10-12 03:18:00 UTC | 5m · 及时 / TIMELY |
| PLUME | PLUME-USDT-SWAP | SWAP | UP | A0 | OBSERVED | 2026-10-12 03:19:00 UTC | 2026-10-12 03:19:00 UTC | 4m · 及时 / TIMELY |
| RVN | RVN-USDT-SWAP | SWAP | DOWN | A0 | OBSERVED | 2026-10-12 03:19:00 UTC | 2026-10-12 03:19:00 UTC | 4m · 及时 / TIMELY |
| W | W-USDT | SPOT | UP | A0 | OBSERVED | 2026-10-12 03:21:00 UTC | 2026-10-12 03:21:00 UTC | 2m · 及时 / TIMELY |
| W | W-USDT | SPOT | DOWN | A0 | OBSERVED | 2026-10-12 03:19:00 UTC | 2026-10-12 03:19:00 UTC | 4m · 及时 / TIMELY |
| W | W-USDT-SWAP | SWAP | UP | A0 | OBSERVED | 2026-10-12 03:21:00 UTC | 2026-10-12 03:21:00 UTC | 2m · 及时 / TIMELY |
| W | W-USDT-SWAP | SWAP | DOWN | A0 | OBSERVED | 2026-10-12 03:19:00 UTC | 2026-10-12 03:19:00 UTC | 4m · 及时 / TIMELY |
| ZK | ZK-USDT-SWAP | SWAP | UP | A0 | OBSERVED | 2026-10-12 03:20:00 UTC | 2026-10-12 03:20:00 UTC | 3m · 及时 / TIMELY |

<!-- OKX_ALERT_RECEIPT_V1:eyJhdHRlbXB0IjoxLCJiYXRjaCI6ImVkNmFlMmFmZjVhZjJjOWY2ZTc5ZjcyYzU1MGI5ODI3ZTllNDRlMGE0OWY2N2Y1ZjI2MWY1NDFiYmZjNDYxOWYiLCJnZW5lcmF0aW9uIjoxMiwiaWRzIjpbIjM3Njk1YzhiOTUwNTU3MTA0NGU3YzViZWRhYjc2MjAzNjA1MTQxYzM5MTM0NTE3MjU4NThlYWI4MTNkNDRjOGUiLCIzYjkyYWUwNGNiMmIyNzQ4YzNjMDUyYmRlMTYxY2ExMDc2NWI5OTE1YTRjYjU2YzljYmU5ZWQ2MTM5YjkzMzk4IiwiNzg5MmQ5MjhjZWQwZmI5ZTZhNGY5NjAwZWQwMWJmMDU4YWE5ZmVjODUyN2FmYWQ2MmQ3NTI0MDM2N2QwYzRlYSIsIjkxMmMwN2Y0MTEwMGU1Nzk1ZjJiNmI5YWY5MTQ0NTZmZjFlM2NmMzI4ODQyMjUzODFiOWYxYWNjNDExY2E3NzQiLCI5OWI0ZDhlMTdhM2UwMWI1ZTg0NzhmMTg0OTM4OGM2ZmI1OTBmZmE0YTQ5NTdhODg1NDcxOTcyY2UxZjNjNzYwIiwiYmNiY2E3ZGU1MjAwYjlhNDY4YmFmNjc0MTVlYjQ0YWYxMGQ4NmI1N2VmMjVhZmE1MmM0YjA0NDMyMDc1MDk3OSIsImM2MDBiMTRjNGEwMDliZGI0M2ZjOTc1MmMzMjM3MWVkYzIxMGY0ODllNzA4OTFhNTNmMjI3YTFkZWEwNGZlYTIiLCJkMmMwZTA4ZTE3NWQ0NGU5NTBlYTc2NmVhNjcyZjg2YjNkYzcxYjhmZDM4YWMwYzQyNWQzYTE1MGMyNDRhZDU0IiwiZTA5ODgyZTMxMWMyZWUzNDFlOGVhYWM3Yjg1OGFhNDE1MWNiOGRiYzA3Y2VjZjUwMjgwZGNiMzU5MGE0YTg5ZCIsImVlOGMyYTk4MTUyODUwYWIyN2I4NjQ5MGU0Nzc0MDM0ZjA5NjkyYjg5MDljNzEzZTkyODU5MzIzYzhlYmZiMTYiXSwicHJlcGFyZWRfYXQiOjE3OTE3NzUyNjAwMDAsInJ1bGVzX3ZlcnNpb24iOiJva3gtYWxlcnQtdHJpYWwtdjEiLCJydW5faWQiOjM4MTk5NDAzNjE4LCJzY2hlbWEiOiJva3gtcHVibGljLXJlY2VpcHQtdjEiLCJzY29wZSI6eyJyZWYiOiJyZWZzL2hlYWRzL21haW4iLCJyZXBvc2l0b3J5Ijoibm9zdGFsdW1lL2NyeXB0by1ib29tLWFsZXJ0Iiwid29ya2Zsb3dfaWQiOjM4MTI3MzE5M30sInNvdXJjZV9zaGEiOiIyY2VmOTUwOWQ5YzUxN2U3YTc5N2FkNzQ4NzRmYjdlMjUwNzhkN2NlIn0= -->
<!-- OKX_ALERT_END -->
