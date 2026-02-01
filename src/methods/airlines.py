
from methods.database import Database

import csv
import psycopg2
from psycopg2.extras import execute_values

class Airlines:

    @staticmethod
    def nullify(value: str):
        """Convert OpenFlights null marker to None."""
        if value is None:
            return None
        v = value.strip()
        return None if v == r"\N" or v == "" else v

    @staticmethod
    def load_airlines_to_db(file_path: str, conn_params: dict):
        # Map your parameter names -> psycopg2 names
        pg_params = {
            "host": conn_params["database_host"],
            "port": conn_params["database_port"],
            "database": conn_params["database_name"],
            "user": conn_params["database_username"],
            "password": conn_params["database_password"],
        }

        create_sql = """
        CREATE TABLE IF NOT EXISTS airlines (
            airline_id INTEGER PRIMARY KEY,
            name       TEXT,
            alias      TEXT,
            iata       TEXT,
            icao       TEXT,
            callsign   TEXT,
            country    TEXT,
            active     CHAR(1)
        );
        """

        insert_sql = """
            INSERT INTO airlines
                (airline_id, name, alias, iata, icao, callsign, country, active)
            VALUES %s
            ON CONFLICT (airline_id) DO UPDATE SET
                name     = EXCLUDED.name,
                alias    = EXCLUDED.alias,
                iata     = EXCLUDED.iata,
                icao     = EXCLUDED.icao,
                callsign = EXCLUDED.callsign,
                country  = EXCLUDED.country,
                active   = EXCLUDED.active;
        """

        rows = []
        with open(file_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            for line_num, row in enumerate(reader, start=1):
                if not row:
                    continue
                if len(row) != 8:
                    raise ValueError(f"Line {line_num}: expected 8 columns, got {len(row)}: {row}")

                airline_id = Airlines.nullify(row[0])
                if airline_id is None:
                    continue

                rows.append((
                    int(airline_id),
                    Airlines.nullify(row[1]),
                    Airlines.nullify(row[2]),
                    Airlines.nullify(row[3]),
                    Airlines.nullify(row[4]),
                    Airlines.nullify(row[5]),
                    Airlines.nullify(row[6]),
                    Airlines.nullify(row[7])
                ))

        if not rows:
            print("No rows found to insert.")
            return

        with psycopg2.connect(**pg_params) as conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)
                execute_values(cur, insert_sql, rows, page_size=5000)
            conn.commit()

        print(f"Inserted/updated {len(rows)} airlines into `airlines`.")

    
    