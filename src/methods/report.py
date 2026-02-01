from methods.database import Database

import psycopg2
import pandas as pd


class Report:
    @staticmethod
    def create_asia_report_table(db_parameters):
        conn = Database.get_connection(db_parameters)

        try:
            with conn.cursor() as cur:
                # Get columns for airline_routes
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'airline_routes';
                """)
                routes_cols = {r[0] for r in cur.fetchall()}

                # Pick the best available join from airline_routes -> aircraft
                join_sql = None

                if "aircraft_id" in routes_cols:
                    join_sql = "ON ac.aircraft_id = r.aircraft_id"
                elif "iata_code" in routes_cols:
                    join_sql = "ON ac.iata_code = r.iata_code"
                elif "icao_code" in routes_cols:
                    join_sql = "ON ac.icao_code = r.icao_code"
                elif "equipment" in routes_cols:
                    join_sql = "ON (ac.iata_code = r.equipment OR ac.icao_code = r.equipment)"

                aircraft_join_clause = (
                    f"LEFT JOIN aircraft ac {join_sql}"
                    if join_sql
                    else "LEFT JOIN aircraft ac ON 1=0"
                )

                cur.execute("DROP TABLE IF EXISTS asia_report;")

                create_sql = f"""
                CREATE TABLE asia_report AS
                SELECT
                  r.airline_id,
                  r.airline_code,
                  COALESCE(al.name, '(unknown)') AS airline_name,

                  -- Asia -> non-Asia
                  COUNT(*) FILTER (
                    WHERE r.source_in_asia = TRUE AND r.dest_in_asia = FALSE
                  ) AS flghts_out_of_asia,

                  -- non-Asia -> Asia
                  COUNT(*) FILTER (
                    WHERE r.source_in_asia = FALSE AND r.dest_in_asia = TRUE
                  ) AS flghts_in_asia,

                  -- Asia -> Asia
                  COUNT(*) FILTER (
                    WHERE r.source_in_asia = TRUE AND r.dest_in_asia = TRUE
                  ) AS flghts_within_asia,

                  -- total includes out + in + within (touches Asia)
                  COUNT(*) FILTER (
                    WHERE r.source_in_asia = TRUE OR r.dest_in_asia = TRUE
                  ) AS total_flights_to_asia,

                  -- Passenger capacity sums
                  SUM(COALESCE(ac.seat_capacity, 0)) FILTER (
                    WHERE r.source_in_asia = TRUE AND r.dest_in_asia = FALSE
                  ) AS pax_out_of_asia,

                  SUM(COALESCE(ac.seat_capacity, 0)) FILTER (
                    WHERE r.source_in_asia = FALSE AND r.dest_in_asia = TRUE
                  ) AS pax_in_asia,

                  -- Asia -> Asia passengers
                  SUM(COALESCE(ac.seat_capacity, 0)) FILTER (
                    WHERE r.source_in_asia = TRUE AND r.dest_in_asia = TRUE
                  ) AS pax_within_asia,

                  -- pax total includes out + in + within (touches Asia)
                  SUM(COALESCE(ac.seat_capacity, 0)) FILTER (
                    WHERE r.source_in_asia = TRUE OR r.dest_in_asia = TRUE
                  ) AS pax_total_to_asia

                FROM airline_routes r
                LEFT JOIN airlines al
                  ON al.airline_id = r.airline_id
                {aircraft_join_clause}

                GROUP BY r.airline_id, r.airline_code, al.name

                -- Keep airlines that have ANY Asia-related flight (in/out/within)
                HAVING COUNT(*) FILTER (
                  WHERE r.source_in_asia = TRUE OR r.dest_in_asia = TRUE
                ) > 0;
                """

                cur.execute(create_sql)

            conn.commit()
            print("asia_report table created successfully.")
            if not join_sql:
                print("Warning: No aircraft link column found in airline_routes; pax_* columns will be 0.")
            else:
                print(f"Aircraft join used: {join_sql}")
                
            # Load table into DataFrame
            df = pd.read_sql("SELECT * FROM asia_report;", conn)

            # Export to Excel
            df.to_excel("output data/asia_report.xlsx", index=False)

        except Exception as e:
            conn.rollback()
            print("Error creating asia_report:", e)

        finally:
            conn.close()
