# vdliq history — daily VDL2/ACARS message data

Free, open daily exports of **non-specialty VDL2/ACARS application messages**
received by the [vdliq](https://vdliq.com) feeder network. One
[Release](../../releases) per UTC **month** holds one Parquet file per day.

The public derivative contains recognized position, weather, OOOI, and
ATS/operational messages. Private collection is separate and is not represented
by this reduced public dataset.

## What's in each release
Each monthly release (tag `YYYY-MM`) holds one file per UTC day:

| Asset | Description |
|---|---|
| `vdl2_messages_<date>.parquet` | That day's public-safe non-specialty messages (zstd Parquet) |

Per-day **row counts and SHA-256 checksums** are listed in the release notes.
Download a specific day at
`.../releases/download/<YYYY-MM>/vdl2_messages_<date>.parquet`.

## Data model
One row per public-safe decoded application message. Empty/link-only frames,
engine/APU/ACMS/maintenance content, full raw decoder objects, station/feeder
identity, RF measurements, and source UUIDs are intentionally excluded.

### Key columns
`timestamp` (UTC), `icaoHex`, `fromHex`, `toHex`, `tail`, `flightNumber`,
`label` (ACARS label), `mode`, `sourceType`, `text`, `public_category`, and
`policy_version`.

`public_category` is one of `position`, `weather`, `oooi`, or
`ats_operational`. The publication policy fails closed: unclassified content is
not included.

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
