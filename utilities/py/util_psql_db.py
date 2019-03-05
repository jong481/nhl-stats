
from sqlalchemy import create_engine
import psycopg2
import pandas as pd

class UtilPostgreSQL:
    """
    Class used to represent a PostgreSQL connection to a single database
    """
    
    """
    General Functions
    """
    
    def __init__(self, host, port, user, pw, db):
        """
        Parameters
        ----------
        host : str
            host name of PostgreSQL database
        port : int
            port for the PostgreSQL database
        user : str
            user name used for connecting to the database
        pw : str
            password used for connecting to the database
        db : str
            database to establish connection
        """
        
        self.host = host
        self.port = port
        self.user = user
        self.pw = pw
        self.db = db
        
        self.params = {
            'database': db,
            'user': user,
            'password': pw,
            'host': host,
            'port': port
        }
        
    def createSQLAlchemyEngine(self):
        """Creates a SQL Alchemy Engine with the specified connection parameters
        
        Returns
        -------
        engine : SQL Alchemy Engine
            engine to be used for SQL Alchemy Connections
        """
        
        engine = create_engine('postgresql://{0}:{1}@{2}:{3}/{4}'.format(self.user, self.pw, self.host, self.port, self.db))
    
        return engine
    
    def createNativeConnection(self):
        """Creates a pyscopg2 connection with the specified connection parameters
        
        Returns
        -------
        conn : pyscopg2 Connection
            connection object to be used for pyscopg2 commands
        cur : pyscopg2 Cursor
            cursor object to be used for pyscopg2 commands
        """
    
        conn = psycopg2.connect(**self.params)
        cur = conn.cursor()
        
        return conn, cur
        
    def dropTable(self, table):
        """Drops a specified PostgreSQL table
        
        Parameters
        ----------
        table : str
            name of table to be dropped
        
        Returns
        -------
        bool : True if successful, False if error
            indicates if command is successfully completed
        """
        
        conn, cur = self.createNativeConnection()
        
        try:
            cur.execute('DROP TABLE {0}'.format(table))
            conn.commit()
            
            return True
        except:
            return False
        finally:
            conn.close()
            
    def renameTable(self, table_from, table_to):
        """Renames a specified PostgreSQL table
        
        Parameters
        ----------
        table_from : str
            name of the table to be renamed
        table_to : str
            new name of table
        
        Returns
        -------
        bool : True if successful, False if error
            indicates if command is successfully completed
        """
        
        conn, cur = self.createNativeConnection()
        
        try:
            cur.execute('ALTER TABLE {0} RENAME TO {1}'.format(table_from, table_to))
            conn.commit()
            
            return True
        except:
            return False
        finally:
            conn.close()
        
    def execQuery(self, query):
        """Executes a specified query
        
        Parameters
        ----------
        query : str
            query string to be executed against the specified database 
            
        Returns
        -------
        res : query result
            psycopg2 result object containing query results
        """
        
        conn, cur = self.createNativeConnection()
        
        cur.execute(query)
        res = cur.fetchall()

        conn.close()

        return res
    
    def writeDFToTable(self, df, table, mode):
        """Writes a Pandas Dataframe to a PostgreSQL table with the specified mode and table name
        
        Parameters
        ----------
        df : pandas dataframe
            dataframe containing the records to be written
        table : str
            name of table to be written to
        mode : str
            replace or append
            
        Returns
        -------
        bool : True if successful, False if error
            indicates if command is successfully completed
        """
        
        engine = self.createSQLAlchemyEngine()
        try: 
            df.to_sql(table, engine, if_exists=mode)
            return True
        except Exception as e:
            return False
        
    def queryToDF(self, query):
        """Executes a query and returns the results in a Pandas dataframe object
        
        Parameters
        ----------
        query : str
            query to be executed
            
        Returns
        -------
        df : pandas dataframe
            dataframe containing the query results
        """
        
        engine = self.createSQLAlchemyEngine()
        return pd.read_sql_query(query, con=engine)
