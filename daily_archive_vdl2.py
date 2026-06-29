#!/usr/bin/env python3
"""vdliq daily VDL2/ACARS archiver — adsb.lol-style free GitHub data drop.

The vdliq.com counterpart to daily_archive.py. Reads ONE UTC day of decoded
VDL2/ACARS messages from the canonical raw lake and packages them as a single
zstd Parquet plus a JSON manifest, for publishing as a GitHub Release asset.

The source is zstd-compressed JSONL (one decoded message per line), partitioned
by ``y=YYYY/m=MM/d=DD/...`` under a base location supplied at runtime (never
embedded here — see Configuration). This is a raw message lake, not a Delta table.

Output schema: the stable top-level message fields as columns, with the nested
`raw` (full dumpvdl2/acarsdec decode) and `station` kept as JSON strings so the
file is queryable yet loses nothing.

Runs on GitHub Actions (free runners) — never on the prod box. DuckDB streams the
source -> local Parquet, so memory stays flat regardless of day size.

Configuration (no locations are hard-coded)
--------------------------------------------
    SOURCE_S3_BASE   the partition root, e.g. via a repo *secret*  (required)
    --source         same thing, as a CLI override (for local runs)
    DUCKDB_THREADS   optional integer CPU cap (keeps shared runners friendly)

Usage:
    SOURCE_S3_BASE=... python daily_archive_vdl2.py                  # yesterday (UTC)
    python daily_archive_vdl2.py --source ... --date 2026-06-27      # specific day
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import sys

# Top-level message fields kept as native Parquet columns (verified against the
# live lake). `raw` + `station` are nested -> serialized to JSON strings.
_FLAT_COLS = [
    "timestamp", "icaoHex", "fromHex", "toHex", "tail", "flightNumber", "label",
    "blockId", "msgNum", "frequency", "level", "mode", "source", "sourceType",
    "text", "feederId", "id", "uuid",
]


def _yesterday_utc() -> str:
    return (_dt.datetime.now(_dt.timezone.utc).date() - _dt.timedelta(days=1)).isoformat()


def _valid_date(s: str) -> str:
    try:
        _dt.date.fromisoformat(s)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a YYYY-MM-DD date: {s!r}")
    return s


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def build(date: str, source_base: str, region: str, out_dir: str) -> dict:
    import duckdb

    if not source_base:
        raise SystemExit("source location not configured — set SOURCE_S3_BASE (or pass --source).")

    os.makedirs(out_dir, exist_ok=True)
    out_parquet = os.path.join(out_dir, f"vdl2_messages_{date}.parquet")
    out_manifest = os.path.join(out_dir, f"manifest_{date}.json")

    y, m, d = date.split("-")
    src_glob = f"{source_base.rstrip('/')}/y={y}/m={m}/d={d}/**/*.jsonl.zst"

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    if region:
        con.execute(f"SET s3_region='{region}';")
    threads = os.environ.get("DUCKDB_THREADS")
    if threads:
        con.execute(f"PRAGMA threads={int(threads)};")
    con.execute("CREATE SECRET aws (TYPE S3, PROVIDER credential_chain);")

    # union_by_name tolerates the per-message-type schema variance; raw/station
    # are reduced to JSON text so a wide/explosive nested schema can't break us.
    select_cols = ",\n               ".join(
        [f"COALESCE(TRY_CAST({c} AS VARCHAR), NULL) AS {c}" if c not in ("timestamp",)
         else "timestamp" for c in _FLAT_COLS]
    )
    read = (f"read_json('{src_glob}', format='newline_delimited', "
            "union_by_name=true, maximum_object_size=20000000, ignore_errors=true)")

    (row_count,) = con.execute(f"SELECT COUNT(*) FROM {read}").fetchone()
    if row_count == 0:
        raise SystemExit(f"No messages for {date} — refusing to publish an empty archive.")

    con.execute(
        f"""
        COPY (
            SELECT {select_cols},
                   to_json(station) AS station,
                   to_json(raw)     AS raw
            FROM {read}
            ORDER BY timestamp
        ) TO ? (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE 1000000)
        """,
        [out_parquet],
    )

    size = os.path.getsize(out_parquet)
    digest = _sha256(out_parquet)
    manifest = {
        "dataset": "vdliq-vdl2-messages",
        "date": date,
        "rows": int(row_count),
        "file": os.path.basename(out_parquet),
        "bytes": size,
        "sha256": digest,
        "compression": "zstd",
        "source": "vdliq feeder network",
        "schema_note": "decoded VDL2/ACARS messages; stable top-level fields as columns, "
                       "nested `raw` (full decode) and `station` as JSON strings. "
                       "Position-bearing messages are a subset — see repo README.",
        "license": "ODbL-1.0",
        "generated_by": "vdliq daily_archive_vdl2.py",
    }
    with open(out_manifest, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"[ok] {date}: {row_count:,} messages -> {out_parquet} "
          f"({size/1e6:.1f} MB, sha256={digest[:12]}...)")
    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a") as f:
            f.write(f"parquet={out_parquet}\n")
            f.write(f"manifest={out_manifest}\n")
            f.write(f"rows={row_count}\n")
            f.write(f"tag={date}\n")
    return manifest


def main(argv=None):
    ap = argparse.ArgumentParser(description="vdliq daily VDL2/ACARS archiver")
    ap.add_argument("--date", type=_valid_date, default=_yesterday_utc(),
                    help="UTC day to archive (YYYY-MM-DD). Default: yesterday.")
    ap.add_argument("--source", default=os.environ.get("SOURCE_S3_BASE", ""),
                    help="Partition root for the source lake (or set SOURCE_S3_BASE).")
    ap.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    ap.add_argument("--out-dir", default="dist")
    args = ap.parse_args(argv)
    build(args.date, args.source, args.region, args.out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
