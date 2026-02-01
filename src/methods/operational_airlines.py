from methods.database import Database

import csv
import psycopg2
from psycopg2.extras import execute_values

class OperationalAirlines:
    
    @staticmethod
    def create_table(db_parameters):
        """
        Creates a new table called operational_airlines
        containing only airlines where active = 'Y'
        """

        sql = """
        DROP TABLE IF EXISTS operational_airlines;

        CREATE TABLE operational_airlines AS
        SELECT *
        FROM airlines
        WHERE active = 'Y';
        """

        conn = Database.get_connection(db_parameters)

        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()

            print("operational_airlines table created successfully.")

        except Exception as e:
            print("Error creating operational_airlines table:", e)
            raise
        finally:
            conn.close()