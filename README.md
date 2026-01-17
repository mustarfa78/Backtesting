# Futures Listings → MEXC Reaction Dataset Builder

This project builds a dataset for the "new futures listings announcement → MEXC reaction" strategy. It scans recent futures listing announcements, verifies the asset was trading on MEXC futures at announcement time, and computes MEXC-only price reaction metrics using 1-minute close data.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Optional environment variable for CoinMarketCap historical market cap:

```bash
export CMC_API_KEY=your_key_here
```

## Run

```bash
python main.py --target 15 --days 30 --out events.csv
```

## Notes

- The pipeline uses cached HTTP responses (SQLite) to avoid hammering APIs.
- If CoinMarketCap is unavailable, CoinGecko supply is used to approximate market cap.
- HTML-based adapters (Gate, Kraken, Binance fallback, XT) are best-effort and may require selector updates.

## Tests

```bash
python micro_highs.py
```
