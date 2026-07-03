# vdliq history — daily VDL2/ACARS message data

Free, open daily exports of decoded **VDL2/ACARS** messages received by the
[vdliq](https://vdliq.com) feeder network. One [Release](../../releases) per UTC
**month**, holding one Parquet file per day, generated automatically. Inspired by
[adsb.lol](https://github.com/adsblol)'s open-data drops.

## What's in each release
Each monthly release (tag `YYYY-MM`) holds one file per UTC day:

| Asset | Description |
|---|---|
| `vdl2_messages_<date>.parquet` | That day's decoded VDL2/ACARS messages (zstd Parquet) |

Per-day **row counts and SHA-256 checksums** are listed in the release notes.
Download a specific day at
`.../releases/download/<YYYY-MM>/vdl2_messages_<date>.parquet`.

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
# read a specific day straight from GitHub (no download needed)
duckdb.sql("SELECT timestamp, tail, label, text FROM read_parquet('https://github.com/Sky-Power-Services/vdliq-data/releases/download/2026-06/vdl2_messages_2026-06-29.parquet') LIMIT 10")
```
```python
import pandas as pd
df = pd.read_parquet("vdl2_messages_2026-06-29.parquet")
```

## License
Open Database License (**ODbL-1.0**). Attribution: "vdliq feeder network".
