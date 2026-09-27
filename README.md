# Crypto Boom: public hourly altcoin status

<!-- HOURLY_ALERT_STATUS_START -->
No completed public scan has been published. A missing or stale timestamp is not a no-signal result.
<!-- HOURLY_ALERT_STATUS_END -->

This is a free, read-only **attention filter** for unusual Binance Spot altcoin activity. It does not predict a 20% or 30% rise, recommend an entry, place an order, or establish profitability. The public status is updated after a successful hourly scan; a failed or missed scan leaves its timestamp stale. A README update is **not** a guaranteed push notification.

## Which pairs are scanned?

Each run reads Binance's current exchange metadata and considers pairs that are `TRADING`, Spot-enabled, quoted in USDT, and have an ASCII alphanumeric symbol. BTC, ETH, BNB, and an explicit stable/fiat base-asset list are excluded. This is a current eligible pool, not a historical census. The scanner refuses a pool above 1,000 pairs rather than silently truncating it. A pair without 170 contiguous, completed hourly bars is counted as unavailable, not as a negative signal.

## What is compared, and how are matches ordered?

The rule compares **each pair with its own preceding 168 completed hours**, not with other coins' absolute volume. Its hourly quote turnover is measured in USDT. It computes turnover divided by the preceding 168-hour mean, a z-score against that same prior distribution, close relative to the prior EMA20, and executed taker-buy quote volume minus taker-sell quote volume. These are descriptive conditions, not calibrated probabilities.

`WATCH` requires at least $100,000 hourly turnover, a 3× turnover ratio, z-score at least 3, close at or above the prior EMA20, and positive net taker flow. `CONFIRMED` adds at least $500,000 hourly turnover and net taker flow, plus close at least 5% above the prior EMA20. A pair appears only when it first enters a stage or moves from WATCH to CONFIRMED; a persistent same-stage hour does not repeat.

Matches are sorted by **CONFIRMED before WATCH**, then descending *within-pair turnover ratio*, then alphabetically by symbol. The first 20 appear above; the complete list, if longer, remains in the Actions run log. This ordering is a display priority, **not** a cross-coin probability ranking or a profitability estimate. A high ratio can reflect a very low prior base and may be misleading.

## Operation and limits

The [scheduled workflow](.github/workflows/hourly-altcoin-alert.yml) starts at minute 07 after each UTC hour. It requests the latest completed hourly bar and 169 prior bars, starts at most eight requests per second, uses at most 16 workers, and aborts if the latest bar is more than 45 minutes old. Scheduled GitHub Actions can be delayed or skipped. The job writes this README using GitHub's Contents API only after a complete scan, and prints a JSON receipt plus Linux process peak RSS in the run log. The Python scanner uses no third-party runtime packages.

To perform a local scan without publishing:

```sh
PYTHONPATH=src python -m crypto_boom.hourly_alert
```

The 1-hour grain and 168-hour baseline are **provisional**, inspired by a published volume-shock example rather than optimized for large-move event recall. A proper comparison needs independent 20%/30% event definitions, alternative scan grains and baselines, out-of-time evaluation, alert lead time, false matches per day, and missed-event accounting. Public status should never be mistaken for a validated trading system.
