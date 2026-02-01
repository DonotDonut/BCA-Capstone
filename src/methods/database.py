from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import psycopg2
from psycopg2.extras import RealDictCursor


class Database: 
    
    @staticmethod
    def get_connection(db_parameters):
        db_connection = psycopg2.connect(
            host=db_parameters["database_host"],
            port=db_parameters["database_port"],
            database=db_parameters["database_name"],
            user=db_parameters["database_username"],
            password=db_parameters["database_password"]
        )
        return db_connection
    
    @staticmethod
    def print_table_length(db_parameters, table_name):

        conn = Database.get_connection(db_parameters)

        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table_name};")
                count = cur.fetchone()[0]

            print(f"Table '{table_name}' has {count} rows.")

        except Exception as e:
            print("Error:", e)

        finally:
            conn.close()
    