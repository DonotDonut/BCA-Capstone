from methods.database import Database

import csv
from typing import Optional, Dict, Tuple

from psycopg2.extras import execute_values



class Aircraft:
    @staticmethod
    def nullify(value: str) -> Optional[str]:
        """Convert '\\N' or empty string to None."""
        if value is None:
            return None
        v = value.strip()
        return None if v == r"\N" or v == "" else v

    @staticmethod
    def nullify_int(value: str) -> Optional[int]:
        v = Aircraft.nullify(value)
        if v is None:
            return None
        try:
            return int(v)
        except ValueError:
            return None

    @staticmethod
    def nullify_float(value: str) -> Optional[float]:
        v = Aircraft.nullify(value)
        if v is None:
            return None
        try:
            return float(v)
        except ValueError:
            return None

    @staticmethod
    def load_aircraft_to_db(file_path: str, db_parameters: dict):
        """
        Loads aircraft data into Postgres table `aircraft`.

        Supports two input formats:
          - 6 columns: name,iata,icao,seat_capacity,cargo_amount_cuft,source
          - 7 columns: name,iata,icao,seat_capacity,source
            (cargo_amount_cuft not present; stored as NULL)

        Deduplicates by ICAO for safe ON CONFLICT upsert.
        """

        create_sql = """
        CREATE TABLE IF NOT EXISTS aircraft (
            aircraft_id         BIGSERIAL PRIMARY KEY,
            name                TEXT,
            iata_code           TEXT,
            icao_code           TEXT,
            seat_capacity       INTEGER,
            cargo_amount_cuft   DOUBLE PRECISION,
            source_of_capacity  TEXT
        );
        """

        create_indexes_sql = """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_aircraft_icao ON aircraft(icao_code);
        CREATE INDEX IF NOT EXISTS idx_aircraft_iata ON aircraft(iata_code);
        """

        insert_sql = """
        INSERT INTO aircraft (
            name,
            iata_code,
            icao_code,
            seat_capacity,
            cargo_amount_cuft,
            source_of_capacity
        )
        VALUES %s
        ON CONFLICT (icao_code) DO UPDATE SET
            name               = EXCLUDED.name,
            iata_code          = EXCLUDED.iata_code,
            seat_capacity      = EXCLUDED.seat_capacity,
            cargo_amount_cuft  = EXCLUDED.cargo_amount_cuft,
            source_of_capacity = EXCLUDED.source_of_capacity;
        """

        rows_by_icao: Dict[str, Tuple[
            Optional[str], Optional[str], str,
            Optional[str], Optional[str],
            Optional[int], Optional[float], Optional[str]
        ]] = {}

        with open(file_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            for line_num, row in enumerate(reader, start=1):
                if not row:
                    continue

                # Allow 6 or 7 columns
                if len(row) not in (6, 7):
                    raise ValueError(
                        f"Line {line_num}: expected 6 or 7 columns, got {len(row)}: {row}"
                    )

                name = Aircraft.nullify(row[0])
                iata = Aircraft.nullify(row[1])
                icao = Aircraft.nullify(row[2])

                if icao is None:
                    continue


                seat_capacity = None
                cargo_cuft = None
                source = None

                if len(row) == 6:
                    # name,iata,icao,seat,cargo,source
                    seat_capacity = Aircraft.nullify_int(row[3])
                    cargo_cuft = Aircraft.nullify_float(row[4])
                    source = Aircraft.nullify(row[5])
                else:
                    # name,iata,icao,source
                    seat_capacity = Aircraft.nullify_int(row[5])
                    source = Aircraft.nullify(row[6])
                    cargo_cuft = None  # not provided in this format

                rows_by_icao[icao] = (name, iata, icao, seat_capacity, cargo_cuft, source)

        rows = list(rows_by_icao.values())

        if not rows:
            print("No aircraft rows found to insert.")
            return

        conn = Database.get_connection(db_parameters)
        try:
            with conn.cursor() as cur:
                cur.execute(create_sql)
                cur.execute(create_indexes_sql)
                execute_values(cur, insert_sql, rows, page_size=10000)
                conn.commit()
            print(f"Inserted/updated {len(rows)} unique ICAO aircraft rows into `aircraft`.")
        except Exception as e:
            print("Error loading aircraft:", e)
        finally:
            conn.close()
