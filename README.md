# Crypto Boom: public hourly altcoin status

<!-- HOURLY_ALERT_STATUS_START -->
**Last complete scan:** 2026-10-05T18:00:00+00:00 (UTC hour close).  
**New conditions:** 3; evaluated 487 / eligible 494; unavailable history 7.

| Stage | Pair | Turnover (USDT) | Ratio | Z-score | EMA premium | Net taker (USDT) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| WATCH | AVNTUSDT | 793,776 | 27.2 | 25.5 | +3.0% | 119,852 |
| WATCH | RLCUSDT | 2,535,884 | 26.1 | 5.5 | +41.3% | 194,750 |
| WATCH | NILUSDT | 1,708,546 | 7.9 | 5.5 | +15.3% | 97,118 |

Attention filter only; not a return forecast or buy recommendation.
<!-- HOURLY_ALERT_RECEIPT_V1:eyJiYXJfY2xvc2VfdXRjIjoiMjAyNi0xMC0wNVQxODowMDowMCswMDowMCIsImJpbmFuY2Vfc2VydmVyX3RpbWVfdXRjIjoiMjAyNi0xMC0wNVQxODoxODo0NS4zMTUwMDArMDA6MDAiLCJlbGlnaWJsZV9zeW1ib2xzIjo0OTQsImV2YWx1YXRlZF9zeW1ib2xzIjo0ODcsInNjYW5fY29tcGxldGVkX3V0YyI6IjIwMjYtMTAtMDVUMTg6MTk6NDkuODY4MzkyKzAwOjAwIiwic2Nhbl9zdGFydGVkX3V0YyI6IjIwMjYtMTAtMDVUMTg6MTg6NDQuOTEzOTc2KzAwOjAwIiwic2NoZW1hX3ZlcnNpb24iOjEsInNpZ25hbHMiOlt7ImJhcl9vcGVuX21zIjoxNzkxMjE5NjAwMDAwLCJlbWFfcHJlbWl1bSI6MC4wMzAyOTEwNDA1MjAyMTQzNjMsIm5ldF90YWtlcl9xdW90ZV91c2R0IjoxMTk4NTIuMDI2MjIsInF1b3RlX3ZvbHVtZV91c2R0Ijo3OTM3NzYuMDgzOTYsInN0YWdlIjoiV0FUQ0giLCJzeW1ib2wiOiJBVk5UVVNEVCIsInZvbHVtZV9yYXRpbyI6MjcuMjAyNzAwMDcyMTcwOTQ1LCJ2b2x1bWVfenNjb3JlIjoyNS41MTU3OTcxNzAyNDkzMzJ9LHsiYmFyX29wZW5fbXMiOjE3OTEyMTk2MDAwMDAsImVtYV9wcmVtaXVtIjowLjQxMzIyNTUzMjc3NzYyLCJuZXRfdGFrZXJfcXVvdGVfdXNkdCI6MTk0NzUwLjA3OTczOTk5OTk2LCJxdW90ZV92b2x1bWVfdXNkdCI6MjUzNTg4My44OTg0OCwic3RhZ2UiOiJXQVRDSCIsInN5bWJvbCI6IlJMQ1VTRFQiLCJ2b2x1bWVfcmF0aW8iOjI2LjExMDA3MTI3NjU4MjUyLCJ2b2x1bWVfenNjb3JlIjo1LjQ2OTEzNjMwNDE2NTMyN30seyJiYXJfb3Blbl9tcyI6MTc5MTIxOTYwMDAwMCwiZW1hX3ByZW1pdW0iOjAuMTUyOTU0Mzg1Njg0NDAyNDUsIm5ldF90YWtlcl9xdW90ZV91c2R0Ijo5NzExNy44MDI0NTAwMDAwOCwicXVvdGVfdm9sdW1lX3VzZHQiOjE3MDg1NDYuMzcwMzQ4LCJzdGFnZSI6IldBVENIIiwic3ltYm9sIjoiTklMVVNEVCIsInZvbHVtZV9yYXRpbyI6Ny44ODUxNzQwNjExMTIwOTEsInZvbHVtZV96c2NvcmUiOjUuNDY4MTk2NDUxODUwMzZ9XSwidW5hdmFpbGFibGVfaGlzdG9yeV9zeW1ib2xzIjo3fQ== -->
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
