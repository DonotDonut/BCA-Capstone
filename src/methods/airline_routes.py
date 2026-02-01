from methods.database import Database


import csv
import psycopg2
from psycopg2.extras import execute_values


class AirlineRoutes:

    @staticmethod
    def nullify(value: str):
        """Convert OpenFlights null marker '\\N' or empty string to None."""
        if value is None:
            return None
        v = value.strip()
        return None if v == r"\N" or v == "" else v

    @staticmethod
    def to_int_or_none(value: str):
        v = AirlineRoutes.nullify(value)
        return None if v is None else int(v)

    @staticmethod
    def to_bool_codeshare(value: str):
        """
        OpenFlights: 'Y' if codeshare, empty otherwise.
        We'll store as boolean.
        """
        v = AirlineRoutes.nullify(value)
        return True if v == "Y" else False

    @staticmethod
    def load_routes_to_db(file_path: str, conn_params: dict):
        """
        Load OpenFlights routes.dat (or similar CSV) into PostgreSQL table `airline_routes`.

        Expected columns (9):
        Airline, Airline ID, Source airport, Source airport ID,
        Destination airport, Destination airport ID, Codeshare,
        Stops, Equipment
        """

        # Map your conn_params naming (if you use the same dict style as before)
        # If your dict is already psycopg2-style (host/user/password/database/port),
        # you can delete this mapping and use conn_params directly.
        pg_params = {
            "host": conn_params.get("database_host", conn_params.get("host")),
            "port": conn_params.get("database_port", conn_params.get("port")),
            "database": conn_params.get("database_name", conn_params.get("database")),
            "user": conn_params.get("database_username", conn_params.get("user")),
            "password": conn_params.get("database_password", conn_params.get("password")),
        }

        create_sql = """
        CREATE TABLE IF NOT EXISTS airline_routes (
            route_id BIGSERIAL PRIMARY KEY,

            airline_code          TEXT,     -- IATA or ICAO
            airline_id            INTEGER,  -- OpenFlights airline id

            source_airport_code   TEXT,     -- IATA or ICAO
            source_airport_id     INTEGER,  -- OpenFlights airport id

            dest_airport_code     TEXT,     -- IATA or ICAO
            dest_airport_id       INTEGER,  -- OpenFlights airport id

            codeshare             BOOLEAN,
            stops                 INTEGER,
            equipment             TEXT,

            -- Natural key to prevent duplicates
            CONSTRAINT airline_routes_uk UNIQUE (
                airline_code,
                airline_id,
                source_airport_code,
                source_airport_id,
                dest_airport_code,
                dest_airport_id,
                codeshare,
                stops,
                equipment
            )
        );
        """

        insert_sql = """
        INSERT INTO airline_routes (
            airline_code,
            airline_id,
            source_airport_code,
            source_airport_id,
            dest_airport_code,
            dest_airport_id,
            codeshare,
            stops,
            equipment
        )
        VALUES %s
        ON CONFLICT ON CONSTRAINT airline_routes_uk DO NOTHING;
        """

        rows = []
        with open(file_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)  # OpenFlights routes is comma-delimited with quotes as needed
            for line_num, row in enumerate(reader, start=1):
                if not row:
                    continue

                if len(row) != 9:
                    raise ValueError(f"Line {line_num}: expected 9 columns, got {len(row)}: {row}")

                airline_code = AirlineRoutes.nullify(row[0])
                airline_id = AirlineRoutes.to_int_or_none(row[1])

                source_code = AirlineRoutes.nullify(row[2])
                source_id = AirlineRoutes.to_int_or_none(row[3])

                dest_code = AirlineRoutes.nullify(row[4])
                dest_id = AirlineRoutes.to_int_or_none(row[5])

                codeshare = AirlineRoutes.to_bool_codeshare(row[6])

                stops_raw = AirlineRoutes.nullify(row[7])
                stops = None if stops_raw is None else int(stops_raw)

                equipment = AirlineRoutes.nullify(row[8])

                rows.append((
                    airline_code,
                    airline_id,
                    source_code,
                    source_id,
                    dest_code,
                    dest_id,
                    codeshare,
                    stops,
                    equipment
                ))

        if not rows:
            print("No rows found to insert.")
            return

        with psycopg2.connect(**pg_params) as conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)
                execute_values(cur, insert_sql, rows, page_size=10000)
            conn.commit()

        print(f"Inserted {len(rows)} rows into `airline_routes` (duplicates ignored).")

    
    @staticmethod
    def map_asia_flags(db_parameters: dict):
        """
        Adds source_in_asia and dest_in_asia columns to airline_routes
        using airports.country membership in Asia.
        """

        # A practical Asia country list (you can add/remove as needed)
        asia_countries = [
            'Afghanistan','Armenia','Azerbaijan','Bahrain','Bangladesh','Bhutan','Brunei',
            'Cambodia','China','Cyprus','Georgia','India','Indonesia','Iran','Iraq','Israel',
            'Japan','Jordan','Kazakhstan','Kuwait','Kyrgyzstan','Laos','Lebanon','Malaysia',
            'Maldives','Mongolia','Myanmar','Nepal','North Korea','Oman','Pakistan',
            'Palestine','Philippines','Qatar','Saudi Arabia','Singapore','South Korea',
            'Sri Lanka','Syria','Taiwan','Tajikistan','Thailand','Timor-Leste','Turkey',
            'Turkmenistan','United Arab Emirates','Uzbekistan','Vietnam','Yemen','Hong Kong','Macau'
        ]

        # Build safe SQL literals
        country_list_sql = ", ".join(["%s"] * len(asia_countries))

        sql = f"""
        ALTER TABLE airline_routes
        ADD COLUMN IF NOT EXISTS source_in_asia BOOLEAN,
        ADD COLUMN IF NOT EXISTS dest_in_asia BOOLEAN;

        -- Source flag
        UPDATE airline_routes r
        SET source_in_asia = (a.country IN ({country_list_sql}))
        FROM airports a
        WHERE r.source_airport_id = a.airport_id;

        -- Destination flag
        UPDATE airline_routes r
        SET dest_in_asia = (a.country IN ({country_list_sql}))
        FROM airports a
        WHERE r.dest_airport_id = a.airport_id;

        -- Any routes with missing airport_id match -> set to FALSE
        UPDATE airline_routes
        SET source_in_asia = FALSE
        WHERE source_in_asia IS NULL;

        UPDATE airline_routes
        SET dest_in_asia = FALSE
        WHERE dest_in_asia IS NULL;
        """

        conn = Database.get_connection(db_parameters)
        try:
            with conn.cursor() as cur:
                # We use the same country list twice (source + dest), so pass it twice
                cur.execute(sql, asia_countries + asia_countries)
                conn.commit()
            print("Asia flags updated on airline_routes.")
        finally:
            conn.close()
            
    @staticmethod
    def count_asia_routes(db_parameters):

        sql = """
        SELECT
            COUNT(*) FILTER (WHERE source_in_asia = TRUE) AS source_in_asia,
            COUNT(*) FILTER (WHERE dest_in_asia = TRUE)   AS dest_in_asia,
            COUNT(*) FILTER (
                WHERE source_in_asia = TRUE AND dest_in_asia = TRUE
            ) AS both_in_asia,
            COUNT(*) FILTER (
                WHERE source_in_asia = TRUE OR dest_in_asia = TRUE
            ) AS touches_asia
        FROM airline_routes;
        """

        conn = Database.get_connection(db_parameters)

        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                result = cur.fetchone()

            print("Routes with source_in_asia = TRUE:", result[0])
            print("Routes with dest_in_asia   = TRUE:", result[1])
            print("Routes with BOTH in Asia   :", result[2])
            print("Routes that touch Asia     :", result[3])

        finally:
            conn.close()
            
    @staticmethod
    def report_asia_airline_frequencies(db_parameters, limit=50):
        sql = """
        SELECT
          r.airline_id,
          r.airline_code,
          COALESCE(a.name, '(unknown)') AS airline_name,

          COUNT(*) FILTER (WHERE r.source_in_asia = TRUE AND r.dest_in_asia = TRUE)  AS within_asia,
          COUNT(*) FILTER (WHERE r.source_in_asia = TRUE AND r.dest_in_asia = FALSE) AS out_of_asia,
          COUNT(*) FILTER (WHERE r.source_in_asia = FALSE AND r.dest_in_asia = TRUE) AS into_asia,
          COUNT(*) FILTER (WHERE r.source_in_asia = TRUE OR  r.dest_in_asia = TRUE)  AS touches_asia_total

        FROM airline_routes r
        LEFT JOIN airlines a
          ON a.airline_id = r.airline_id

        GROUP BY r.airline_id, r.airline_code, a.name
        HAVING COUNT(*) FILTER (WHERE r.source_in_asia = TRUE AND r.dest_in_asia = TRUE) > 0
        ORDER BY touches_asia_total DESC, within_asia DESC
        LIMIT %s;
        """

        conn = Database.get_connection(db_parameters)
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (limit,))
                rows = cur.fetchall()

            print("airline_id | code | name | within | out | into | touches_total")
            for r in rows:
                print(f"{r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} | {r[5]} | {r[6]}")
        finally:
            conn.close()