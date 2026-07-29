#!/usr/bin/env python3
"""Build the public-safe daily vdliq VDL2/ACARS archive.

The private tier-0 source is immutable and remains complete.  This program
creates a deliberately reduced public derivative containing only recognized
ordinary position, weather, OOOI, and ATS/operational application messages.
Engine/APU/ACMS/maintenance content fails closed and is never selected.

The transformation runs entirely in DuckDB so source JSONL is streamed from S3
to Parquet without loading a day into Python memory.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import sys

POLICY_VERSION = "public-non-specialty-v1"

# These fingerprints are intentionally conservative. They cover explicit
# engine/APU terms plus the compact ACMS families known to include position or
# weather fields (which must not make them public-safe).
SPECIALTY_RE = (
    r"(ENGINE|ENG[ _-]?[1-4]|APU|ACMS|ACMF|APM|C1TRP|"
    r"REP[0-9]{3}|ABS071BA|MDC[ _-]REPORT|SYSTEM[ _-]PARAMETERS|"
    r"SFEC[0-9]?|BCG3[A-Z]-L200|OFFOFFAUTO|MFS[ _-]POSITION|"
    r"L/E[ _-]TEMP|FLAP[ _-](?:POS|SKEW)|SLAT[ _-]POS|"
    r"N1L|N1R|N2L|N2R|EGT|FUEL[ _-]?FLOW|"
    r"OIL[ _-]?(?:PRESS|TEMP|QTY)|VIB(?:RATION)?|FADEC|EEC|"
    r"NO LIGHT OFF|FAULT|FAIL(?:URE)?|MAINT(?:ENANCE)?|"
    r"\bCMC\b|CFDS|\bPFR\b|\bLRU\b|\bBITE\b|"
    r"^[A-Z0-9]{5},[0-9]{4},B73[789])"
)

POSITION_RE = (
    r"(\bPOS(?:N|ITION)?\b|\bLAT(?:ITUDE)?\b|\bLON(?:G|GITUDE)?\b|"
    r"[NS][0-9]{2,4}[0-9.]*[, /]+[EW][0-9]{3,5})"
)
WEATHER_RE = (
    r"(\bMETAR\b|\bTAF\b|\bSIGMET\b|\bWEATHER\b|\bWX\b|"
    r"\bWIND\b|\bTURB(?:ULENCE)?\b|\bSAT\s*[:=]|\bTAT\s*[:=])"
)
OOOI_RE = (
    r"(\bOUT\s*[:=]?\s*[0-9]{4}\b|\bOFF\s*[:=]?\s*[0-9]{4}\b|"
    r"\bON\s*[:=]?\s*[0-9]{4}\b|\bIN\s*[:=]?\s*[0-9]{4}\b|"
    r"\bOOOI\b|/(?:OUT|OFF|ON|IN)\b)"
)
ATS_RE = (
    r"(\bATC\b|\bCLEARANCE\b|\bCLRD\b|\bROUTE\b|"
    r"\bFLIGHT\s*PLAN\b|\bFPL\b|\bSQUAWK\b|\bCPDLC\b)"
)

PUBLIC_COLS = [
    "timestamp", "icaoHex", "fromHex", "toHex", "tail", "flightNumber",
    "label", "blockId", "msgNum", "mode", "sourceType", "text",
]


def _yesterday_utc() -> str:
    return (_dt.datetime.now(_dt.timezone.utc).date() -
            _dt.timedelta(days=1)).isoformat()


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


def _match(column: str, pattern: str) -> str:
    escaped = pattern.replace("'", "''")
    return f"regexp_matches({column}, '{escaped}', 'i')"


def _public_query(read: str) -> str:
    """Return the audited public-only query.

    ``raw``, ``station``, feeder IDs, RF measurements, source IDs, and UUIDs are
    omitted by construction rather than selected and later scrubbed.
    """
    cast_cols = ",\n                   ".join(
        "TRY_CAST(timestamp AS DOUBLE) AS timestamp" if c == "timestamp"
        else f"TRY_CAST({c} AS VARCHAR) AS {c}"
        for c in PUBLIC_COLS
    )
    specialty = _match("text", SPECIALTY_RE)
    position = _match("text", POSITION_RE)
    weather = _match("text", WEATHER_RE)
    oooi = _match("text", OOOI_RE)
    ats = _match("text", ATS_RE)
    return f"""
        WITH normalized AS (
            SELECT {cast_cols}
            FROM {read}
            WHERE text IS NOT NULL AND length(trim(TRY_CAST(text AS VARCHAR))) > 0
        ),
        classified AS (
            SELECT *,
                   CASE
                     WHEN {specialty} THEN NULL
                     WHEN {position} THEN 'position'
                     WHEN {weather} THEN 'weather'
                     WHEN {oooi} THEN 'oooi'
                     WHEN {ats} THEN 'ats_operational'
                     ELSE NULL
                   END AS public_category
            FROM normalized
        ),
        safe AS (
            SELECT *, lag(timestamp) OVER (
                       PARTITION BY coalesce(fromHex, ''), coalesce(tail, ''),
                                    coalesce(label, ''), text
                       ORDER BY timestamp
                   ) AS prior_same_timestamp
            FROM classified
            WHERE public_category IS NOT NULL
        )
        SELECT {", ".join(PUBLIC_COLS)},
               public_category,
               '{POLICY_VERSION}' AS policy_version
        FROM safe
        WHERE prior_same_timestamp IS NULL
           OR timestamp IS NULL
           OR timestamp - prior_same_timestamp > 60
        ORDER BY timestamp
    """


def build(date: str, source_base: str, region: str, out_dir: str,
          input_parquet: str = "") -> dict:
    import duckdb

    if not source_base and not input_parquet:
        raise SystemExit("set SOURCE_S3_BASE/pass --source, or use --input-parquet")

    os.makedirs(out_dir, exist_ok=True)
    out_parquet = os.path.join(out_dir, f"vdl2_messages_{date}.parquet")
    out_manifest = os.path.join(out_dir, f"manifest_{date}.json")

    con = duckdb.connect()
    threads = os.environ.get("DUCKDB_THREADS")
    if threads:
        con.execute(f"PRAGMA threads={int(threads)};")

    if input_parquet:
        safe_path = input_parquet.replace("'", "''")
        read = f"read_parquet('{safe_path}')"
    else:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        if region:
            con.execute(f"SET s3_region='{region}';")
        con.execute("CREATE SECRET aws (TYPE S3, PROVIDER credential_chain);")
        y, m, d = date.split("-")
        src_glob = (
            f"{source_base.rstrip('/')}/y={y}/m={m}/d={d}/**/*.jsonl.zst"
        )
        read = (
            f"read_json('{src_glob}', format='newline_delimited', "
            "union_by_name=true, maximum_object_size=20000000, "
            "ignore_errors=true)"
        )

    (source_rows,) = con.execute(f"SELECT COUNT(*) FROM {read}").fetchone()
    if source_rows == 0:
        raise SystemExit(f"No private source messages for {date}; refusing empty archive")

    query = _public_query(read)
    (public_rows,) = con.execute(f"SELECT COUNT(*) FROM ({query}) q").fetchone()
    if public_rows == 0:
        raise SystemExit(
            f"No public-safe messages for {date}; refusing to publish an empty archive"
        )
    con.execute(
        f"COPY ({query}) TO ? "
        "(FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE 1000000)",
        [out_parquet],
    )

    size = os.path.getsize(out_parquet)
    digest = _sha256(out_parquet)
    categories = dict(con.execute(
        f"SELECT public_category, COUNT(*) FROM ({query}) q "
        "GROUP BY public_category ORDER BY public_category"
    ).fetchall())
    manifest = {
        "dataset": "vdliq-public-non-specialty-messages",
        "date": date,
        "rows": int(public_rows),
        "source_rows_private": int(source_rows),
        "file": os.path.basename(out_parquet),
        "bytes": size,
        "sha256": digest,
        "compression": "zstd",
        "source": "vdliq feeder network public derivative",
        "public_categories": categories,
        "policy_version": POLICY_VERSION,
        "schema_note": (
            "Public-safe position, weather, OOOI, and ATS/operational application "
            "messages only. Private tier-0/1 is unchanged. Full raw decoder output, "
            "engine/APU/ACMS/maintenance content, empty/link frames, feeder identity, "
            "station data, RF measurements, and source UUIDs are excluded."
        ),
        "license": "ODbL-1.0",
        "generated_by": "vdliq daily_archive_vdl2.py",
    }
    with open(out_manifest, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print(
        f"[ok] {date}: private source {source_rows:,} -> public {public_rows:,} "
        f"messages ({size/1e6:.1f} MB, policy={POLICY_VERSION}, "
        f"sha256={digest[:12]}...)"
    )
    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a", encoding="utf-8") as f:
            f.write(f"parquet={out_parquet}\n")
            f.write(f"manifest={out_manifest}\n")
            f.write(f"rows={public_rows}\n")
            f.write(f"tag={date}\n")
    return manifest


def main(argv=None):
    ap = argparse.ArgumentParser(description="vdliq public-safe daily archiver")
    ap.add_argument("--date", type=_valid_date, default=_yesterday_utc())
    ap.add_argument("--source", default=os.environ.get("SOURCE_S3_BASE", ""))
    ap.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    ap.add_argument("--out-dir", default="dist")
    ap.add_argument("--input-parquet", default="",
                    help="local raw Parquet input for tests/audits; bypasses S3")
    args = ap.parse_args(argv)
    build(args.date, args.source, args.region, args.out_dir, args.input_parquet)
    return 0


if __name__ == "__main__":
    sys.exit(main())
