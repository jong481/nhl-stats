
from sqlalchemy import create_engine
import psycopg2
import pandas as pd

def createEngine(host, port, user, pw, db):
    engine = create_engine('postgresql://{0}:{1}@{2}:{3}/{4}'.format(user, pw, host, port, db))
    
    return engine

def writeDF(df, host, port, user, pw, db, tb, mode):
    engine = createEngine(host, port, user, pw, db)
    df.to_sql(tb, engine, if_exists=mode)
    
def queryToDF(host, port, user, pw, db, query):
    engine = createEngine(host, port, user, pw, db)
    return pd.read_sql_query(query, con=engine)

def createConn(host, port, user, pw, db):
    params = {
      'database': db,
      'user': user,
      'password': pw,
      'host': host,
      'port': port
    }
    
    conn = psycopg2.connect(**params)
    return conn

def closeConn(conn):
    conn.close()
        
def execQuery(host, port, user, pw, db, query):
    
    conn = createConn(host, port, user, pw, db)
    cur = conn.cursor()

    cur.execute(query)
    res = cur.fetchall()
    
    closeConn(conn)
    
    return res

def renameTable(host, port, user, pw, db, table_from, table_to):
    
    conn = createConn(host, port, user, pw, db)
    cur = conn.cursor()

    cur.execute('ALTER TABLE {0} RENAME TO {1}'.format(table_from, table_to))
    
    conn.commit()
    closeConn(conn)
    
    return True

def dropTable(host, port, user, pw, db, table):
    
    conn = createConn(host, port, user, pw, db)
    cur = conn.cursor()

    cur.execute('DROP TABLE {0}'.format(table))
    
    conn.commit()
    closeConn(conn)
    
    return True

'''
NHL Utilities
'''

def nhl_getGameIDByDate(host, port, user, pw, db, tb, date_col, tgt_date):
    conn = createConn(host, port, user, pw, db)
    cur = conn.cursor()
        
    cur.execute("SELECT distinct _id FROM {1} WHERE {0}='{2}'".format(date_col, tb, tgt_date))
    res = cur.fetchall()
    
    closeConn(conn)
    
    res_list = []
    for r in res:
        res_list.append(r[0])
    
    return res_list
