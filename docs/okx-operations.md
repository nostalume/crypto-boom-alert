# OKX 告警运维

只读 OKX 公共数据，覆盖 USDT 现货和 USDT 永续；不交易，不需要交易所密钥。
生产目标每 10 分钟扫描一次，使用标准 GitHub-hosted Ubuntu runner。
GitHub 定时任务可能延迟或漏触发，不承诺分钟级送达。

## 启动与暂停

`okx-live.yml` 手动默认 `preview`，不发行情提交；生产定时使用 `live`。
`OKX_LIVE_ENABLED=true` 是生产开关。暂停应停用此 workflow，不取消正在发布的 job。
仓库只保留一个生产 writer；不要另外启动修改 README 的行情脚本。

首次启动：确认专用 cache 完整清单为空，手动 `preview rebuild=true` 建立状态。
必要时用 `preview rebuild=false` 续接下载；这只是冷启动，不要求 24 小时观察。
核对前序独立 final 证明、完整池进度及无未决发布，再手动 `live` 提供其 `arm_manifest`。
首次 live 成功后启用生产调度。后续 live 从已 armed final 续接，不重复填写 arm_manifest。

```text
gh workflow run okx-live.yml --repo nostalume/crypto-boom-alert --ref main -f mode=preview -f rebuild=true
gh workflow run okx-live.yml --repo nostalume/crypto-boom-alert --ref main -f mode=live -f arm_manifest=REVIEWED_SHA256
gh workflow disable okx-live.yml --repo nostalume/crypto-boom-alert
```

## 邮件与排序

有新事件才产生 `OKX ALTCOIN ALERT ...` 提交，GitHub push 通知沿用已有邮件设置。
没有事件不制造邮件；commit/readback 成功不等于邮件已经送达。
及时项（事件年龄不超过 30 分钟）先于延迟项，同类 PERSISTENT 先于 OBSERVED，
再按币种及产品身份稳定排序。现货、永续各留一行和完整回执，不合并信号身份。
OBSERVED/PERSISTENT 是脚本条件，不是上涨/下跌概率或盈利承诺。

## 状态与免费边界

不上传 artifact，不使用收费 runner，不提高预算；保留账户原有 $0 停用预算。
仓库须公开；平台计费规则仍由 GitHub 决定，不承诺未来永远免费。
专用 native cache 每个最多 128 MiB，repo cache 使用量须可读且不超过 8 GiB。
正常保留一个 final，交接时有严格临时上界；只按验证后的精确 ID 清理自己的对象。
cache 可被淘汰，不是可靠数据库或 exactly-once 保证。

缺失、损坏、过期、不兼容的状态，失败前序、未知发布或额度不可读时停止通知。
不要挑选“最近成功”绕过紧邻失败，也不要删除状态来盲目重发。
恢复先暂停 workflow，核对 run、cache 和提交。只有确认无未决发布且专用池为空，
才可重新执行非通知 rebuild；重新启用还需明确审核基线。
回滚先停新 writer，核对未决及已确认提交，再恢复替代 writer，不能群发旧告警。

## 诊断与本地检查

```text
gh run list --repo nostalume/crypto-boom-alert --workflow okx-live.yml --limit 5
gh cache list --repo nostalume/crypto-boom-alert --limit 100
gh run view RUN_ID --repo nostalume/crypto-boom-alert --log
```

`OKX_RUN_V1` 给出 scan 耗时、数据截止年龄、分市场覆盖与 final；
`partial/deferred/warmup` 不能解释为没有异常，`complete` 也不是邮件到达证明。
`OKX_TRANSITION_V1` 按事件 identity+stage 核对变化，ENDED 不发告警。

离线检查：`uv sync --locked`，设置 `PYTHONPATH=src`，
运行 `uv run --locked --no-sync python -m pytest -q -p no:cacheprovider`、
`uv run --locked --no-sync python -m ruff check --no-cache --select F,I,RUF100 src tests`。
生产用 `uv sync --locked --no-dev`，不会安装 dev 组；`uv.lock` 是唯一完整依赖锁。
依赖升级须显式执行 `uv lock --upgrade-package NAME` 并检查 diff、测试后再提交。
CI 只执行离线检查，不扫描市场、不发布、不保存 cache/artifact。
Actions 使用固定提交的官方 `astral-sh/setup-uv` 自动安装 uv 0.12.17，创建并激活 Python 3.12 环境；
关闭依赖及 Python 的远程缓存，后续只运行已同步环境中的 `python -m`，不隐式重新解锁/安装。
准备与发布两个 job 保留权限隔离；YAML anchors 仅复用完全相同的安装、额度检查和清理内容。
Python 3.12 不在 runner 时可由 uv 下载，因此安装时延需线上实测，不承诺此次改动加速扫描。

可选的有限 preview 输入 observation_start_ms、observation_end_ms、observation_digest 和离线 report 仍保留供诊断；
它们不是上线前置。生产必须清空观察变量，不使用到期轮次作恢复基线。

源码模块已去掉冗余前缀；workflow 路径及版本化缓存/回执标识保持不变。
源码改名或删减会改变 cache 的代码摘要，不能直接续用旧摘要的缓存，
也不能靠清空缓存跳过去重；发布这类变更前须完成保留账本的状态交接。
