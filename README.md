# Crypto Boom Alert

只读 OKX 公共行情，监测 USDT 现货与 USDT 永续的价量异常；不交易、不需要交易所密钥。
GitHub Actions 目标每 10 分钟运行一次，但可能延迟或漏触发。

## 何时提醒

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

## 运行与检查

[运维说明](docs/okx-operations.md) 包含暂停、状态恢复和免费边界。
当前使用标准 GitHub-hosted runner、严格限额的 native cache，不上传 artifact。
缺失、不兼容或未知发布状态停止通知，不自动清空账本重发。

```sh
uv sync --locked
export PYTHONPATH=src
uv run --locked --no-sync python -m pytest -q -p no:cacheprovider
uv run --locked --no-sync python -m ruff check --no-cache --select F,I,RUF100 src tests
```

依赖以 `pyproject.toml + uv.lock` 为唯一来源；Python 3.12/3.13，非打包项目。
生产只安装运行依赖，CI 安装 dev 组。更新依赖须显式更新并检查 lock，不在运行时自动升级。
Windows PowerShell 使用 `$env:PYTHONPATH="src"` 代替上面的 `export`；uv 由 aqua 管理时可调用其真实 exe 路径。

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
