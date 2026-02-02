import csv
from psycopg2.extras import execute_values
from methods.database import Database


class Airports:

    @staticmethod
    def nullify(value: str):
        if value is None:
            return None
        v = value.strip()
        return None if v == r"\N" or v == "" else v

    @staticmethod
    def to_float_or_none(value: str):
        v = Airports.nullify(value)
        return None if v is None else float(v)

    @staticmethod
    def to_int_or_none(value: str):
        v = Airports.nullify(value)
        return None if v is None else int(v)

    @staticmethod
    def load_airports_to_db(file_path: str, db_parameters: dict):
        """
        Load OpenFlights airport.dat into Postgres table `airports`.
        Expected columns (14):
        Airport ID, Name, City, Country, IATA, ICAO, Latitude, Longitude,
        Altitude, Timezone, DST, Tz database timezone, Type, Source
        """

        create_sql = """
        CREATE TABLE IF NOT EXISTS airports (
            airport_id  INTEGER PRIMARY KEY,
            name        TEXT,
            city        TEXT,
            country     TEXT,
            iata        TEXT,
            icao        TEXT,
            latitude    DOUBLE PRECISION,
            longitude   DOUBLE PRECISION,
            altitude_ft INTEGER,
            timezone_utc_offset DOUBLE PRECISION,
            dst         TEXT,
            tz_database TEXT,
            type        TEXT,
            source      TEXT
        );
        """

        insert_sql = """
        INSERT INTO airports (
            airport_id, name, city, country, iata, icao,
            latitude, longitude, altitude_ft, timezone_utc_offset,
            dst, tz_database, type, source
        )
        VALUES %s
        ON CONFLICT (airport_id) DO UPDATE SET
            name = EXCLUDED.name,
            city = EXCLUDED.city,
            country = EXCLUDED.country,
            iata = EXCLUDED.iata,
            icao = EXCLUDED.icao,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            altitude_ft = EXCLUDED.altitude_ft,
            timezone_utc_offset = EXCLUDED.timezone_utc_offset,
            dst = EXCLUDED.dst,
            tz_database = EXCLUDED.tz_database,
            type = EXCLUDED.type,
            source = EXCLUDED.source;
        """

        rows = []
        with open(file_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            for line_num, row in enumerate(reader, start=1):
                if not row:
                    continue
                if len(row) != 14:
                    raise ValueError(f"Line {line_num}: expected 14 columns, got {len(row)}: {row}")

                airport_id = Airports.to_int_or_none(row[0])
                if airport_id is None:
                    continue

                rows.append((
                    airport_id,
                    Airports.nullify(row[1]),   # name
                    Airports.nullify(row[2]),   # city
                    Airports.nullify(row[3]),   # country
                    Airports.nullify(row[4]),   # iata
                    Airports.nullify(row[5]),   # icao
                    Airports.to_float_or_none(row[6]),  # lat
                    Airports.to_float_or_none(row[7]),  # lon
                    Airports.to_int_or_none(row[8]),    # altitude
                    Airports.to_float_or_none(row[9]),  # timezone offset
                    Airports.nullify(row[10]),  # dst
                    Airports.nullify(row[11]),  # tz database
                    Airports.nullify(row[12]),  # type
                    Airports.nullify(row[13])   # source
                ))

        if not rows:
            print("No airport rows found.")
            return

        conn = Database.get_connection(db_parameters)
        try:
            with conn.cursor() as cur:
                cur.execute(create_sql)
                execute_values(cur, insert_sql, rows, page_size=10000)
                conn.commit()
            print(f"Inserted/updated {len(rows)} airports into `airports`.")
        finally:
            conn.close()
            
    def add_columns(db_parameters):
        conn = Database.get_connection(db_parameters)

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    ALTER TABLE airports
                    ADD COLUMN IF NOT EXISTS inbound_count  INTEGER DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS outbound_count INTEGER DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS total_in_out   INTEGER DEFAULT 0;
                """)

            conn.commit()
            print("Columns added successfully!")

        finally:
            conn.close()

            
    def calculate_flights_per_airport(db_parameters):
        conn = Database.get_connection(db_parameters)
        try:
            with conn.cursor() as cur:
                #  reset so airports with no routes are 0
                cur.execute("""
                    UPDATE airports
                    SET inbound_count = 0,
                        outbound_count = 0,
                        total_in_out = 0;
                """)

                # Outbound update (by source_airport_id)
                cur.execute("""
                    UPDATE airports a
                    SET outbound_count = sub.out_count
                    FROM (
                        SELECT source_airport_id AS airport_id, COUNT(*) AS out_count
                        FROM airline_routes
                        WHERE source_airport_id IS NOT NULL
                        GROUP BY source_airport_id
                    ) sub
                    WHERE a.airport_id = sub.airport_id;
                """)

                # Inbound update (by destination_airport_id AKA dest_airport_id)
                cur.execute("""
                    UPDATE airports a
                    SET inbound_count = sub.in_count
                    FROM (
                        SELECT dest_airport_id AS airport_id, COUNT(*) AS in_count
                        FROM airline_routes
                        WHERE dest_airport_id IS NOT NULL
                        GROUP BY dest_airport_id
                    ) sub
                    WHERE a.airport_id = sub.airport_id;
                """)

                # Total update
                cur.execute("""
                    UPDATE airports
                    SET total_in_out = COALESCE(inbound_count, 0) + COALESCE(outbound_count, 0);
                """)

            conn.commit()
            print("Airport counts updated successfully!")
        finally:
            conn.close()



            