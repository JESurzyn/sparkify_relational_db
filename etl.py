#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec  6 14:48:14 2020

@author: jordansurzyn
"""

import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    '''
    This function processes JSON files from the song dataset.  It opens the
    file as a pandas dataframe given the filepath provided as an argument.
    It then indexes the df for the songs table vairables and inserts them.  It
    then indexes the df for the artists table variables and inserts them.
    

    Parameters
    ----------
    -cur
    -filepath

    Returns
    -------
    None.

    '''
    #open song file
    df = pd.read_json(filepath, lines=True)
    
    #insert song record
    song_data = df.loc[:,['song_id','title','artist_id','year','duration']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    #insert artist record
    artist_data = df.loc[:,['artist_id', 'artist_name','artist_location','artist_latitude','artist_longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)
    
def process_log_file(cur, filepath):
    '''
    This function processes JSON files from the logs dataset.  It opens the file as a pandas dataframe given the filepath
    provided as an argument and filters for song play logs only.  After converting the ts column to datetime format, it 
    then uses this column to produce the formatted columns of the time table.  After producing the formatted columns it inserts
    them into the the time table.  It then indexes the dataframe for the variables in the users table and inserts them.  It then
    uses the song select query to select the artist name and song table from the respective tables and inserts them with the
    other relevent variables into the songplay fact table

    Parameters
    ----------
    -cur 
    -filepath

    Returns
    -------
    None.

    '''
    
    #open log file
    df=pd.read_json(filepath, lines=True)
    
    #filter by NextSong action
    df = df.loc[df['page']=='NextSong',:]
    
    #convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = df['ts']
    
    #insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        
    #load user table
    user_df = df.loc[:,['userId', 'firstName', 'lastName', 'gender', 'level']]
    
    #insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    
    #insert songplay records
    for index, row in df.iterrows():
        
        #get songid and aristid from song andf artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            
        #insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
        
def process_data(cur, conn, filepath, func):
    '''
       This function processes the JSON files.  The first block pulls all JSON file paths in the directory provided by the filepath
    argument.  The next block prints how many files were found in the path.  The third block prints an update status as the files
    are processed.

    Parameters
    ----------
    -cur 
    -conn
    -filepath
    -func : function
        specified for processing data

    Returns
    -------
    None.

    '''
    
    #get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
            
    #get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    
    #iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
        
def main():
    '''
    Creates the connection and cursor to the postgres database.  Calls the process_data function twice to process data from song
    files and log files and populate their respective tables

    Returns
    -------
    None.

    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres")
    cur =conn.cursor()
    
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    conn.close()
    
if __name__ == '__main__':
    main()