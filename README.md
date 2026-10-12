# Crypto Boom: public hourly altcoin status

<!-- HOURLY_ALERT_STATUS_START -->
**Last complete scan:** 2026-10-12T00:00:00+00:00 (UTC hour close).  
**New conditions:** 1; evaluated 493 / eligible 497; unavailable history 4.

| Stage | Pair | Turnover (USDT) | Ratio | Z-score | EMA premium | Net taker (USDT) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| WATCH | AXLUSDT | 117,356 | 5.2 | 4.2 | +2.5% | 63,022 |

Attention filter only; not a return forecast or buy recommendation.
<!-- HOURLY_ALERT_RECEIPT_V1:eyJiYXJfY2xvc2VfdXRjIjoiMjAyNi0xMC0xMlQwMDowMDowMCswMDowMCIsImJpbmFuY2Vfc2VydmVyX3RpbWVfdXRjIjoiMjAyNi0xMC0xMlQwMDoyNjoyOC42NDQwMDArMDA6MDAiLCJlbGlnaWJsZV9zeW1ib2xzIjo0OTcsImV2YWx1YXRlZF9zeW1ib2xzIjo0OTMsInNjYW5fY29tcGxldGVkX3V0YyI6IjIwMjYtMTAtMTJUMDA6Mjc6MzQuMjE0MDc4KzAwOjAwIiwic2Nhbl9zdGFydGVkX3V0YyI6IjIwMjYtMTAtMTJUMDA6MjY6MjcuNzAyNDQyKzAwOjAwIiwic2NoZW1hX3ZlcnNpb24iOjEsInNpZ25hbHMiOlt7ImJhcl9vcGVuX21zIjoxNzkxNzU5NjAwMDAwLCJlbWFfcHJlbWl1bSI6MC4wMjUyODEwMDYwNTQzOTEwOTcsIm5ldF90YWtlcl9xdW90ZV91c2R0Ijo2MzAyMi40MTYwMDgsInF1b3RlX3ZvbHVtZV91c2R0IjoxMTczNTYuMzQ5NzI4LCJzdGFnZSI6IldBVENIIiwic3ltYm9sIjoiQVhMVVNEVCIsInZvbHVtZV9yYXRpbyI6NS4yMjY3MTkxMDc3NTI2MzIsInZvbHVtZV96c2NvcmUiOjQuMjA2MDY4MTE2Njk2ODQ5fV0sInVuYXZhaWxhYmxlX2hpc3Rvcnlfc3ltYm9scyI6NH0= -->
<!-- HOURLY_ALERT_STATUS_END -->

This is a free, read-only **attention filter** for unusual Binance Spot altcoin activity. It does not predict a 20% or 30% rise, recommend an entry, place an order, or establish profitability. The public status is updated after a successful hourly scan; a failed or missed scan leaves its timestamp stale. A README update is **not** a guaranteed push notification.

If the repository administrator enables GitHub push-email notifications, each successful status commit now names up to five top-ranked `STAGE:SYMBOL` matches in its subject, followed by `+N more` when needed. No-match commits explicitly say `NO NEW ALTCOIN SIGNALS`. This lets an email reader see the leading pairs without opening the diff; the full status still lives above. GitHub delivery, scheduling, and email-client truncation are not guaranteed.

## Which pairs are scanned?

Each run reads Binance's current exchange metadata and considers pairs that are `TRADING`, Spot-enabled, quoted in USDT, and have an ASCII alphanumeric symbol. BTC, ETH, BNB, and an explicit stable/fiat base-asset list are excluded. This is a current eligible pool, not a historical census. The scanner refuses a pool above 1,000 pairs rather than silently truncating it. A pair without 170 contiguous, completed hourly bars is counted as unavailable, not as a negative signal.

## What is compared, and how are matches ordered?

The rule compares **each pair with its own preceding 168 completed hours**, not with other coins' absolute volume. Its hourly quote turnover is measured in USDT. It computes turnover divided by the preceding 168-hour mean, a z-score against that same prior distribution, close relative to the prior EMA20, and executed taker-buy quote volume minus taker-sell quote volume. These are descriptive conditions, not calibrated probabilities.

`WATCH` requires at least $100,000 hourly turnover, a 3× turnover ratio, z-score at least 3, close at or above the prior EMA20, and positive net taker flow. `CONFIRMED` adds at least $500,000 hourly turnover and net taker flow, plus close at least 5% above the prior EMA20. A pair appears only when it first enters a stage or moves from WATCH to CONFIRMED; a persistent same-stage hour does not repeat.

Matches are sorted by **CONFIRMED before WATCH**, then descending *within-pair turnover ratio*, then alphabetically by symbol. The first 20 appear above; the complete list, if longer, remains in the Actions run log. This ordering is a display priority, **not** a cross-coin probability ranking or a profitability estimate. A high ratio can reflect a very low prior base and may be misleading.

From the first scan after this version is deployed, the status block also contains a hidden `HOURLY_ALERT_RECEIPT_V1` HTML comment. It is URL-safe Base64-encoded JSON holding the **complete ranked match list**, eligible/evaluated/unavailable counts, Binance server time, source-bar close, and scanner start/completion times. The visible table remains limited to 20 pairs and the commit title to five. Prior README commits cannot be retroactively given this receipt. Each successful status commit preserves its own receipt in Git history; an absent commit or stale timestamp is **not** a zero-match observation. The Git commit time is a publication proxy, not an email-delivery timestamp.

## Operation and limits

The [scheduled workflow](.github/workflows/hourly-altcoin-alert.yml) starts at minute 07 after each UTC hour. It requests the latest completed hourly bar and 169 prior bars, starts at most eight requests per second, uses at most 16 workers, and aborts if the latest bar is more than 45 minutes old. Scheduled GitHub Actions can be delayed or skipped. The job writes this README using GitHub's Contents API only after a complete scan, and prints a JSON receipt plus Linux process peak RSS in the run log. The Python scanner uses no third-party runtime packages.

To perform a local scan without publishing:

```sh
PYTHONPATH=src python -m crypto_boom.hourly_alert
```

The 1-hour grain and 168-hour baseline are **provisional**, inspired by a published volume-shock example rather than optimized for large-move event recall. A proper comparison needs independent 20%/30% event definitions, alternative scan grains and baselines, out-of-time evaluation, alert lead time, false matches per day, and missed-event accounting. Public status should never be mistaken for a validated trading system.

<!-- OKX_ALERT_START -->
## 最近 OKX 告警 / Latest alerts

源数据截止：2026-10-12 00:41:00 UTC。本批生成：2026-10-12 00:44:06 UTC / 上海 2026-10-12 08:44:06。
仅记录价量异常；UP/DOWN 是观测方向，不是买卖建议。PERSISTENT 表示持续条件，不代表收益确认。
本页不是实时扫描健康状态；无新告警不更新，扫描覆盖/失败请查看 Actions summary。

展示顺序：及时项优先、同类持续项优先，再按币种分组；现货/永续保留独立事实，不是收益概率排名。

| 币种分组 | 标的 | 市场 | 方向 | 规则 | 阶段 | 首次条件 | 本次条件 | 条件年龄 |
|---|---|---|---|---|---|---|---|---|
| GRASS | GRASS-USDT-SWAP | SWAP | DOWN | A0 | OBSERVED | 2026-10-12 00:37:00 UTC | 2026-10-12 00:37:00 UTC | 7m · 及时 / TIMELY |
| GRASS | GRASS-USDT-SWAP | SWAP | UP | A0 | OBSERVED | 2026-10-12 00:40:00 UTC | 2026-10-12 00:40:00 UTC | 4m · 及时 / TIMELY |
| ICP | ICP-USDT-SWAP | SWAP | DOWN | A0 | OBSERVED | 2026-10-12 00:39:00 UTC | 2026-10-12 00:39:00 UTC | 5m · 及时 / TIMELY |
| ONDO | ONDO-USDT | SPOT | UP | A0 | OBSERVED | 2026-10-12 00:41:00 UTC | 2026-10-12 00:41:00 UTC | 3m · 及时 / TIMELY |
| S | S-USDT-SWAP | SWAP | UP | A0 | OBSERVED | 2026-10-12 00:36:00 UTC | 2026-10-12 00:36:00 UTC | 8m · 及时 / TIMELY |
| S | S-USDT-SWAP | SWAP | DOWN | A0 | OBSERVED | 2026-10-12 00:39:00 UTC | 2026-10-12 00:39:00 UTC | 5m · 及时 / TIMELY |
| STRK | STRK-USDT-SWAP | SWAP | UP | A0 | OBSERVED | 2026-10-12 00:41:00 UTC | 2026-10-12 00:41:00 UTC | 3m · 及时 / TIMELY |
| W | W-USDT-SWAP | SWAP | DOWN | A0 | OBSERVED | 2026-10-12 00:38:00 UTC | 2026-10-12 00:38:00 UTC | 6m · 及时 / TIMELY |

<!-- OKX_ALERT_RECEIPT_V1:eyJhdHRlbXB0IjoxLCJiYXRjaCI6IjYzMTNhNTMzNWU5OGRlYTUwZGQ4ZjVhODhjMTk0OWZhNGU4MThjOGZmZjI0MWVhYTc3ZTAwMTNiOWNhYmQ5OTgiLCJnZW5lcmF0aW9uIjo2LCJpZHMiOlsiMWRlYzhiM2I2ZWFmMzYzMGQxZmI1NWM2ZDZhM2Q5ZDYyZDAyYTE4NDFjMmUwMThjMmE4OWMzYTU4OTU5ZWNhYiIsIjQ4NGZmZThmN2UyOGQ4NzkxM2I4YjZkYzM4OGFlNjYxZWRhM2NmNmMzMjczYjQ1MzEyMDVjYTYyOTJhODVmZTEiLCI0YzA4Y2ExYzdmOWNmMzFjMWQzMWMwYmNlNmY5MzIwNmMzM2E1Y2Q2NmRkZjlmMzJiODBjMGFmYzY2ODZkOWFkIiwiNmY4Y2Q4MTFkYmY1MDJhNmZjMmQwYWQ4ZTNiZmE1MGIzZjA2NDliMDEwMTM3YjdiNDMyZmY3N2M4MDJkZTExMSIsIjg3MGMzYjcyOGQxYzA1ZTY5OGEyNTFlOGMxNDIzYWYzMzY5MDUyMTU0NWExMzY4YmMwYjI3N2EzNGVmMDkzY2MiLCI5NzBkMWExYjNhNzAwY2NkOGY2MWUwNjE2ZDA3MDE3YWY2N2Y2YWMxNTAzODNkNDNlYzQyYWQ0MGM5ZmIwNjlhIiwiYjQ5NTk0NTk5NzA4MDkxY2I2MGZjNjI2MWFlNzgzZjVlM2MwNDNmOTZjYzg1NGU1OWQwNDgyNGZiZDgzYjQxOSIsImY3NDVmOWI2NDAwYTVhMmU4MWM0YjUzYTA1NjI4MmZmOWMzMjcyZjA0ZjM1NjUxNWJiZGRkYTM3NmM0NzRhNjMiXSwicHJlcGFyZWRfYXQiOjE3OTE3NjU2NjAwMDAsInJ1bGVzX3ZlcnNpb24iOiJva3gtYWxlcnQtdHJpYWwtdjEiLCJydW5faWQiOjM4MTg4OTQ5ODg1LCJzY2hlbWEiOiJva3gtcHVibGljLXJlY2VpcHQtdjEiLCJzY29wZSI6eyJyZWYiOiJyZWZzL2hlYWRzL21haW4iLCJyZXBvc2l0b3J5Ijoibm9zdGFsdW1lL2NyeXB0by1ib29tLWFsZXJ0Iiwid29ya2Zsb3dfaWQiOjM4MTI3MzE5M30sInNvdXJjZV9zaGEiOiJhY2UxMzg2NzA5NDk5Njg1ZGY3MGQ0NDY3NTc0YzM2YjAyY2I1MzM1In0= -->
<!-- OKX_ALERT_END -->
