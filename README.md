# Hermes Sentinel

Binance futures smart money monitoring system.

## Structure
```
sentinel/          # Main monitoring system
  ├── ws_client.py       # WebSocket market data
  ├── account_stream.py   # User data stream (positions/orders)
  ├── data_collector.py   # REST polling (klines/OI/funding/LSR)
  ├── bootstrap.py        # Initial historical data load
  ├── stream_router.py    # Data bus
  ├── engine/             # Large frame + second level engines
  ├── patterns/           # 13 large + 6 second patterns
  ├── ai/                 # DeepSeek analysis
  ├── push/               # Feishu notifications
  ├── backtest/           # Daily backtesting
  └── dashboard/          # Real-time web dashboard
shared/
  └── feishu_notify/      # Shared Feishu notification module
```

## Quick Start
1. Copy `sentinel/.env.template` to `sentinel/.env` and fill in API keys
2. Set `env: testnet` or `env: mainnet` in `sentinel/config.yaml`
3. Run: `python3 sentinel.py`

## Config
All endpoints in `sentinel/config.yaml` — one `env` switch controls testnet/mainnet.
