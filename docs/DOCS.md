# hedge_mm Strategy

## How It Works

```
                     ┌─────────────────────────────────┐
                     │          hedge_mm tick           │
                     └──────────────┬──────────────────┘
                                    │
               ┌────────────────────▼────────────────────┐
               │            update_news_state             │
               │  phase-1: shift ref price N seconds      │
               │           before event                   │
               │  phase-2: skew order sizes against       │
               │           post-news taker flow           │
               └──────────┬──────────────────────────────┘
                          │
          ┌───────────────▼────────────────┐
          │            long leg            │
          │  buy limit orders only         │
          │  profit-take: close sell       │
          │  stop-loss:  close sell        │
          └───────────────┬────────────────┘
          ┌───────────────▼────────────────┐
          │           short leg            │
          │  sell limit orders only        │
          │  profit-take: close buy        │
          │  stop-loss:  close buy         │
          └────────────────────────────────┘
```

### Order Ladder

Each leg places `order_levels` limit orders stepped away from mid-price. Size grows from `order_amount × top_order_size_multiplier` at the nearest level to `order_amount` at the farthest, plus `order_level_amount × level` for additional pyramid growth.

| Level | Price (absolute mode)           | Size                                   |
|-------|---------------------------------|----------------------------------------|
| 0     | `mid × (1 - bid_spread)`        | `order_amount × top_order_size_mult`   |
| 1     | level-0 price − `price_step`    | level-0 size + `order_level_amount`    |
| N     | level-0 price − N × `price_step`| level-0 size + N × `order_level_amount`|

### Exit Logic

**Profit-taking (long):** When `(best_non_mm_bid - entry) / entry ≥ long_profit_taking_spread`, place a reduce-only sell at the best non-MM bid. "Non-MM bid" = the highest bid in the orderbook that is not one of this leg's own entry orders, so the close sells against a real counterparty instead of bouncing off its own ladder.

**Profit-taking (short):** When `(entry - best_non_mm_ask) / entry ≥ short_profit_taking_spread`, place a reduce-only buy at the best non-MM ask.

**Stop-loss (long):** When `bid ≤ entry × (1 - stop_loss_spread)`, cancel any open profit-take and immediately place a reduce-only sell at `stop_price × (1 - stop_loss_slippage_buffer)`.

**Stop-loss (short):** When `ask ≥ entry × (1 + stop_loss_spread)`, cancel any open profit-take and immediately place a reduce-only buy at `stop_price × (1 + stop_loss_slippage_buffer)`.

**Rate limiting:** `time_between_stop_loss_orders` throttles profit-take and rebalance close orders — a new close is not placed until that many seconds have elapsed since the last one. **Stop-loss orders bypass this limit** and fire immediately.

### Inventory Rebalancing

When `|long_qty - short_qty| > rebalance_size_diff_threshold` and the heavier leg's unrealized PnL reaches `rebalance_target_pnl_pct`:

1. Emit an immediate reduce-only close on the heavy leg.
2. Scale entry sizes: heavy side × `rebalance_heavy_side_size_mult`, light side × `rebalance_light_side_size_mult`.

### News Integration

Two-phase reaction to `NewsEvent` objects fired by the connector:

| Phase | Trigger | Effect |
|-------|---------|--------|
| Pre-news price shift | Event within `news_pre_window_seconds` | Shift reference price by `news_price_shift_pct × severity_rank` in the sentiment direction |
| Post-news flow skew | Within `news_flow_window_seconds` after publish | Increase order sizes on the side opposing taker flow by up to `news_flow_skew_mult` |

Severity → rank: `low=1`, `medium=2`, `high=3`, `critical=4`.

---

## Setup

### 1. Connect the exchange_sim connector

```
connect exchange_sim
```

Prompts:

| Field | Description |
|-------|-------------|
| `exchange_sim_account_id` | Leave blank for dual-subaccount mode |
| `exchange_sim_long_account_id` | Subaccount ID for the long leg |
| `exchange_sim_short_account_id` | Subaccount ID for the short leg |


### 2. Create a strategy config

Copy the template and edit:

```bash
cp conf/strategies/orderbook_balance.yml conf/strategies/my_hedge_mm.yml
```

Or generate interactively:

```
create --script hedge_mm
```

### 3. Run

```
start --config-file-name my_hedge_mm.yml
```

---

## Configuration Reference

### Core

| Parameter | Default | Description |
|-----------|---------|-------------|
| `derivative` | — | Connector name, e.g. `exchange_sim` |
| `market` | — | Trading pair, e.g. `BTC-USDT` |
| `leverage` | `10` | Position leverage applied to both legs |

### Order Ladder

| Parameter | Default | Description |
|-----------|---------|-------------|
| `bid_spread` | — | % below mid for the nearest buy (level 0) |
| `ask_spread` | — | % above mid for the nearest sell (level 0) |
| `minimum_spread` | `-100` | Filter out orders closer than this % to mid |
| `order_amount` | — | Base size (base asset) at level 0 |
| `order_levels` | `1` | Number of price levels per side |
| `order_level_mode` | `percent` | `percent` = % step per level; `absolute` = fixed price step |
| `order_level_spread` | `1` | (percent mode) Extra spread % per level |
| `order_level_price_step` | `1` | (absolute mode) Price distance between levels |
| `order_level_amount` | `0` | Extra size added per level |
| `top_order_size_multiplier` | `0.5` | Fraction of `order_amount` at the nearest level |

### Order Lifecycle

| Parameter | Default | Description |
|-----------|---------|-------------|
| `order_refresh_time` | `30.0` | Seconds between cancel-and-replace cycles |
| `order_refresh_tolerance_pct` | `0` | Skip refresh if all prices moved less than this % |
| `filled_order_delay` | `60.0` | Seconds before re-entering after a fill |

### Exits

| Parameter | Default | Description |
|-----------|---------|-------------|
| `long_profit_taking_spread` | `0` | Min PnL% to close a long (0 = disabled) |
| `short_profit_taking_spread` | `0` | Min PnL% to close a short (0 = disabled) |
| `stop_loss_spread` | `0` | % loss from entry to trigger stop-loss (0 = disabled) |
| `stop_loss_slippage_buffer` | `0.5` | Extra % beyond stop price to improve fill |
| `time_between_stop_loss_orders` | `60.0` | Minimum seconds between stop-loss placements |

### Inventory Rebalancing

| Parameter | Default | Description |
|-----------|---------|-------------|
| `rebalance_enabled` | `true` | Enable auto-rebalancing |
| `rebalance_size_diff_threshold` | `0.5` | `|long_qty - short_qty|` trigger level |
| `rebalance_target_pnl_pct` | `0.5` | Min unrealized PnL% on heavy leg to close early |
| `rebalance_heavy_side_size_mult` | `0.5` | Size multiplier for heavy-leg entry orders |
| `rebalance_light_side_size_mult` | `1.5` | Size multiplier for light-leg entry orders |

### News

| Parameter | Default | Description |
|-----------|---------|-------------|
| `news_pre_window_seconds` | `2.0` | Seconds before event to start shifting ref price |
| `news_price_shift_pct` | `0.1` | Ref price shift per severity rank (%) |
| `news_flow_window_seconds` | `30.0` | Seconds after publish to observe taker flow |
| `news_flow_skew_mult` | `2.0` | Max size multiplier when skewing against taker flow |

---

## Example Config
```
conf/strategies/orderbook_balance.yml
```

---

## News Feed Format

When `exchange_sim_news_enabled: true`, the connector polls `exchange_sim_news_endpoint` (or reads `exchange_sim_news_feed_path`) for a list of events:

```json
{
  "events": [
    {
      "id": "evt-001",
      "title": "Fed rate decision",
      "timestamp": "2026-04-12T14:00:00Z",
      "severity": "high",
      "sentiment": "negative",
      "symbols": ["BTC-USDT"],
      "source": "reuters"
    }
  ]
}
```

| Field | Required | Values |
|-------|----------|--------|
| `id` | yes | unique string |
| `title` | yes | headline text |
| `timestamp` | yes | ISO-8601 or Unix seconds/ms |
| `severity` | no | `low`, `medium`, `high`, `critical` |
| `sentiment` | no | `positive`/`bullish`, `negative`/`bearish`, `neutral` |
| `symbols` | no | list of trading pairs; empty = affects all |

**Replay mode** (`exchange_sim_news_replay_mode: true`): future events are visible to the strategy for pre-news positioning. In live mode only past events are published.

---

## File Locations

| File | Purpose |
|------|---------|
| `hummingbot/strategy/hedge_mm/hedge_mm.py` | Strategy core logic |
| `hummingbot/strategy/hedge_mm/hedge_mm_config_map.py` | Config schema and validators |
| `hummingbot/strategy/hedge_mm/start.py` | Strategy startup and wiring |
| `hummingbot/connector/derivative/exchange_sim/exchange_sim_derivative.py` | Connector implementation |
| `hummingbot/connector/derivative/exchange_sim/exchange_sim_utils.py` | Connector config map |
| `hummingbot/connector/derivative/exchange_sim/news_service.py` | News polling and dispatch |
| `hummingbot/connector/derivative/exchange_sim/news_types.py` | `NewsEvent` dataclass |
| `conf/strategies/orderbook_balance.yml` | Reference config template |
