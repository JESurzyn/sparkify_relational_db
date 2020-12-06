#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec  5 23:20:23 2020

@author: jordansurzyn
"""

import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def create_database():
    """
    -Creates and connects to the sparkifydb
    
    Returns
    -------
    Connection and cursor to the sparkify db
    """
    
    #connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=default_db user=postgres")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    #create sparkify dabtabase with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    
    #close connection to default database
    conn.close()
    
    #connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres")
    cur = conn.cursor()
    
    return cur, conn

def drop_tables(cur, conn):
    """
    Drops each table using the queries in the 'drop_tables_queries' list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
def create_tables(cur, conn):
    """
    Create each table using the queries in the 'create_tables_queries' list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 

    Returns
    -------
    None.

    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    conn.close()
    
if __name__ == "__main__":
    main()