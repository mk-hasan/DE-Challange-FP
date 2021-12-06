"""
Author: kamrul Hasan
Date: 05.12.2021
Email: hasan.alive@gmail.com
"""


"""
This is the DB Engine script to establish and make different operation into database
"""
import psycopg2



class DBFactory:

    def __init__(self, host: str, port: int, db: str, user: str, password: str) -> None:
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.conn = None

    def connect(self) -> None:
        """
        Establish database connection and make available the db instance for other methods
        """
        try:
            self.conn = psycopg2.connect(host=self.host, database=self.db, port=self.port, user=self.user,
                                         password=self.password)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def conn_close(self) -> None:
        """
        Close the db connection
        """
        if self.conn is not None:
            self.conn.close()
            print("Connection Terminated")

    def execute_command(self, commands: str) -> None:
        """
        Run any sql command
        """
        try:
            cur = self.conn.cursor()
            cur.execute(commands)
            self.conn.commit()
            cur.close()
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if self.conn is not None:
                self.conn.close()

    def create_table(self, table_name: str) -> None:
        """
        Creat table with table name
        """
        self.connect()
        if table_name == 'metrics_data':

            sql = "CREATE TABLE " + table_name + " (\
                id SERIAL PRIMARY KEY,\
                param_id VARCHAR(255),\
                val float(24),\
                time VARCHAR(255)\
            )"

        else:
            sql = "CREATE TABLE " + table_name + " (\
                id SERIAL PRIMARY KEY,\
                time VARCHAR(255),\
                product VARCHAR(255),\
                production float(24)\
            )"

        if self.table_exists(table_name) is False:
            self.execute_command(sql)
            print("Table is created")
        else:
            print("Table is already exists")



    def table_exists(self, table_name: str) -> bool:
        """
        Check if the table exists in the database
        """
        exists = False
        try:
            self.connect()
            cur = self.conn.cursor()
            cur.execute("select exists(select relname from pg_class where relname='" + table_name + "')")
            exists = cur.fetchone()[0]
            cur.close()
        except psycopg2.Error as e:
            print(e)
        return exists

