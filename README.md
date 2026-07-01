# vdliq history — daily VDL2/ACARS message data

[![Latest release](https://img.shields.io/github/v/release/Sky-Power-Services/vdliq-data?display_name=tag&label=latest%20day&color=brightgreen)](https://github.com/Sky-Power-Services/vdliq-data/releases/latest)
[![Updated daily](https://img.shields.io/badge/updated-daily%20~00%3A30%20UTC-blue)](https://github.com/Sky-Power-Services/vdliq-data/releases)
[![License: ODbL-1.0](https://img.shields.io/badge/license-ODbL--1.0-lightgrey)](LICENSE)

Free, open daily exports of decoded **VDL2/ACARS** messages received by the
[vdliq](https://vdliq.com) feeder network. One [Release](../../releases) per UTC
day, generated automatically. Inspired by [adsb.lol](https://github.com/adsblol)'s
open-data drops.

## What's in each release
| Asset | Description |
|---|---|
| `vdl2_messages_<date>.parquet` | That day's decoded VDL2/ACARS messages (zstd Parquet) |
| `manifest_<date>.json` | `rows`, `bytes`, `sha256`, schema note, license |

## Latest — permanent download link

The newest day is always at a fixed URL (no date needed):

```bash
curl -L -o vdl2_messages_latest.parquet \n  https://github.com/Sky-Power-Services/vdliq-data/releases/latest/download/vdl2_messages_latest.parquet
```

Or query it in place — DuckDB reads the release URL directly:

```sql
SELECT * FROM 'https://github.com/Sky-Power-Services/vdliq-data/releases/latest/download/vdl2_messages_latest.parquet' LIMIT 10;
```

A committed **`sample.parquet`** (200 rows) at the repo root lets you peek at the schema without a download.

## Data model
One row per decoded message. The stable top-level fields are native columns; the
full decoder output (`raw`) and the receiving `station` are kept as JSON-string
columns so nothing is lost even as message types vary.

### Key columns
`timestamp` (UTC), `icaoHex`, `fromHex`, `toHex`, `tail`, `flightNumber`,
`label` (ACARS label, e.g. `5Z`, `15`), `frequency`, `mode`, `source`,
`sourceType`, `text`, plus `station` and `raw` as JSON strings.

> Only a subset of messages carry a position. Filter on the relevant labels /
> presence of lat-lon in `raw` if you need positions specifically.

## Loading
```python
import duckdb
duckdb.sql("SELECT timestamp, tail, label, text FROM 'vdl2_messages_2026-06-29.parquet' LIMIT 10")
```
```python
import pandas as pd
df = pd.read_parquet("vdl2_messages_2026-06-29.parquet")
```

## Related

- [adsbiq-data](https://github.com/Sky-Power-Services/adsbiq-data) — ADS-B aircraft position/state data
- [adsbiq-feeder](https://github.com/adsbiq/adsbiq-feeder) — join the network: open-source feeder install scripts
- [vdliq](https://vdliq.com) · [become a feeder](https://vdliq.com/join) · open data at [vdliq.com/data](https://vdliq.com/data)

## License
Open Database License (**ODbL-1.0**). Attribution: "vdliq feeder network".
