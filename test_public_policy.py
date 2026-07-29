import pathlib

import duckdb

import daily_archive_vdl2 as archive


def _build_fixture(path: pathlib.Path):
    rows = [
        (1.0, "A1", "N1", "H1", "POSN N4020 W07340"),
        (2.0, "A2", "N2", "H1", "METAR KJFK 291651Z 18010KT"),
        (3.0, "A3", "N3", "10", "OUT 1201 OFF 1210 ON 1450 IN 1458"),
        (4.0, "A4", "N4", "B6", "ATC CLEARANCE ROUTE KJFK..KBOS"),
        # Engine telemetry also contains position: it must fail closed.
        (5.0, "A5", "N5", "H1",
         "8X201,8318,B737-800,260726,WN0342,KAUS,KMAF "
         "N3012.5 W09740.7 N1L N2L EGT FUEL FLOW"),
        (6.0, "A6", "N6", "H1",
         "A321,029367,1,1,TB000000/REP001 WEATHER POSN N4000 W07300"),
        (7.0, "A7", "N7", "H1", "APU START REPORT POSN N4000 W07300"),
        (8.0, "A8", "N8", "H1", "unclassified carrier proprietary payload"),
        (9.0, "A9", "N9", None, ""),
        (9.1, "A10", "N10", "H1",
         "B43B N8728Q BCG3B-L200-0003 OFFOFFAUTO POSITION 37993"),
        (9.2, "A11", "N11", "H1",
         "A321,018010/REP083 POS-KLM1248 5844N01920E"),
        (9.3, "A12", "N12", "H1",
         "MFS POSITION 0.04 DEG L/E TEMP 39.8 C"),
        # Same RF message within 60 sec is a duplicate public capture.
        (10.0, "A1", "N1", "H1", "POSN N4020 W07340"),
    ]
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE src(
          timestamp DOUBLE, fromHex VARCHAR, tail VARCHAR, label VARCHAR,
          text VARCHAR
        )
    """)
    con.executemany("INSERT INTO src VALUES (?, ?, ?, ?, ?)", rows)
    con.execute("""
        COPY (
          SELECT timestamp, NULL::VARCHAR AS icaoHex, fromHex,
                 NULL::VARCHAR AS toHex, tail, NULL::VARCHAR AS flightNumber,
                 label, NULL::VARCHAR AS blockId, NULL::VARCHAR AS msgNum,
                 NULL::VARCHAR AS frequency, NULL::VARCHAR AS level,
                 NULL::VARCHAR AS mode, NULL::VARCHAR AS source,
                 NULL::VARCHAR AS sourceType, text,
                 NULL::VARCHAR AS feederId, NULL::VARCHAR AS id,
                 NULL::VARCHAR AS uuid, NULL::VARCHAR AS station,
                 NULL::VARCHAR AS raw
          FROM src
        ) TO ? (FORMAT PARQUET)
    """, [str(path)])


def test_public_policy_is_fail_closed_and_minimized(tmp_path):
    raw = tmp_path / "raw.parquet"
    _build_fixture(raw)
    manifest = archive.build(
        "2026-07-27", "", "", str(tmp_path / "out"), str(raw)
    )
    out = tmp_path / "out" / "vdl2_messages_2026-07-27.parquet"
    con = duckdb.connect()
    rows = con.execute(
        "SELECT text, public_category, policy_version FROM read_parquet(?) "
        "ORDER BY timestamp", [str(out)]
    ).fetchall()

    assert len(rows) == 4
    assert {r[1] for r in rows} == {
        "position", "weather", "oooi", "ats_operational"
    }
    assert all(r[2] == archive.POLICY_VERSION for r in rows)
    assert not any("N1L" in r[0] or "REP001" in r[0] or "APU" in r[0]
                   for r in rows)
    columns = {
        r[0] for r in con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(out)]
        ).fetchall()
    }
    assert not {
        "raw", "station", "feederId", "frequency", "level", "source",
        "id", "uuid",
    } & columns
    assert manifest["source_rows_private"] == 13
    assert manifest["rows"] == 4
