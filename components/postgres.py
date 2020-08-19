import psycopg2
from configparser import ConfigParser
import os
import csv
from io import StringIO
from psycopg2.extensions import AsIs

class PostgresInterface():
    def read_config(self, filename='components/configs/database.ini', section='postgresql'):
        # create a parser
        parser = ConfigParser()
        # read config file
        parser.read(filename)

        # get section, default to postgresql
        db_params = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db_params[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))
        return db_params

    def check_version(self):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            params = self.read_config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)
            
            # create a cursor
            cur = conn.cursor()
            
        # execute a statement
            print('PostgreSQL database version:')
            cur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            print(db_version)
        
            # close the communication with the PostgreSQL
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')

    def create_tal_table(self):
        """ create tables in the PostgreSQL database"""
        command = (
            """
            CREATE TABLE tals (
                Route_ID SERIAL PRIMARY KEY,
                Autonomous_System bigint NOT NULL,
                Route INET NOT NULL,
                Start_Date DATE NOT NULL,
                End_Date DATE NOT NULL
            )
            """)
        conn = None
        try:
            # read the connection parameters
            params = self.read_config()
            # connect to the PostgreSQL server
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            # create table one by one
            cur.execute(command)
            # close communication with the PostgreSQL database server
            cur.close()
            # commit the changes
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def create_as_name_table(self):
        """ create tables in the PostgreSQL database"""
        command = (
            """
            CREATE TABLE as_names (
                Id SERIAL PRIMARY KEY,
                ASN bigint NOT NULL,
                Name TEXT,
                Country TEXT
            )
            """)
        conn = None
        try:
            # read the connection parameters
            params = self.read_config()
            # connect to the PostgreSQL server
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            # create table one by one
            cur.execute(command)
            # close communication with the PostgreSQL database server
            cur.close()
            # commit the changes
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def insert_tals(self, data):
        try:
            # read the connection parameters
            params = self.read_config()
            # connect to the PostgreSQL server
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            # create table one by one
            cur.executemany("INSERT into tals(Autonomous_System, Route, Start_Date, End_Date) VALUES (%s, %s, %s, %s)", data)
            # close communication with the PostgreSQL database server
            cur.close()
            # commit the changes
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
    def import_ribs_to_postgres(self, data):
        return False
        #copy_expert(sql, file, size=8192):

    def insert_asn_names(self, data):
        try:
            # read the connection parameters
            params = self.read_config()
            # connect to the PostgreSQL server
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.executemany(
                'INSERT INTO as_names(asn, name, country) '
                'VALUES (%(asn)s, %(name)s, %(country)s)',
                data
            )
            # close communication with the PostgreSQL database server
            cur.close()
            # commit the changes
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
        return None

if __name__ == '__main__':
    print('Postgres interface is main')