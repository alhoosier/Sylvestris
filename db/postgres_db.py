# Application Python Modules
from config.db_constants import *
import psycopg2
import logging


class PostgresDbMain:
    """
    Postgres Db Main class

    """
    # DSN generated from db_constants
    dsn = f'host={CONST_DB_HOST} port={CONST_DB_PORT} ' \
          f'user={CONST_DB_USER} password={CONST_DB_PASS}' \
          f' dbname={CONST_DB_NAME} sslmode={CONST_DB_SSL_MODE}'

    def __init__(self):
        self.conn = self.get_connection()

    def get_connection(self):
        """
        Connect to a Postgres DB and create a session with the provided connection details and return a connection
        object.

        :return: database connection object
        """
        try:
            logging.info('Getting database connection...')
            db_conn = psycopg2.connect(self.dsn)
            logging.info('Connection to database has been established.')
            return db_conn
        except psycopg2.Error as e:
            logging.error(e)
            raise SystemExit

    def execute(self, exec_statement, exec_vars=None, return_results=False):
        """
        Execute a given statement against a database from the provided connection.

        :param exec_statement: SQL database statement to execute
        :param exec_vars: tuple or dictionary of variables to pass into exec_statement
            (Ref: http://initd.org/psycopg/docs/usage.html#query-parameters)
        :param return_results: flag to return results of SQL database statement
        :return: None
        """
        try:
            db_cursor = self.conn.cursor()
            db_cursor.execute(exec_statement, exec_vars)
            self.conn.commit()
            if return_results:
                return db_cursor.fetchall()
        # If error has to do with the connection/operation timing out then reset connection and try again
        except (psycopg2.DatabaseError, ConnectionError) as e:
            if isinstance(e, ConnectionError) or (isinstance(e, psycopg2.DatabaseError)
                                                  and 'Operation timed out' in str(e)):
                try:
                    self.conn.close()
                    self.conn = self.get_connection()
                    self.execute(exec_statement, exec_vars)
                except psycopg2.Error as e:
                    logging.error(e)
                    raise SystemExit
        except psycopg2.Error as e:
            logging.error(e)
            raise SystemExit

    def __del__(self):
        self.conn.close()
