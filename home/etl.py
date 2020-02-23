import os
import glob
import psycopg2
import pandas as pd
import pandas as pd1
from sql_queries import *

conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
cur = conn.cursor()

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))   
    return all_files


def process_song_file(cur, filepath):
    """
     This procedure processes a song file whose filepath has been provided as an arugment.
     It extracts the song information in order to store it into the songs table.
     Then it extracts the artist information in order to store it into the artists table.
     INPUTS: 
     * cur the cursor variable
     * filepath the file path to the song file
    """
    
    # open song file
    song_files = get_files('data/song_data')
    filepath = song_files[0]
    
    #create a Datafrmae out of it
    df = pd.read_json(filepath, lines=True)
   
    #pick the required columns from the dataframe
    song_data = df[ ['song_id', 'title', 'artist_id','year','duration'] ]
    song_data1 =  song_data.values[0]
    
    #insert song record
    cur.execute(song_table_insert, song_data1)
    
    #pick the required columns from the dataframe
    artist_data = df[ ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'] ]
    artist_data1 = artist_data.values[0]
    
    #insert artist record
    cur.execute(artist_table_insert, artist_data1) 

#=====================================================================

def process_log_file(cur, filepath):
    """
     This procedure processes log files whose filepath has been provided as an arugment.
     It extracts the log information and stores respective data in time table and users table.
     
     INPUTS: 
     * cur the cursor variable
     * filepath the file path to the song file
    """
    
    # open log file
    log_files = get_files('data/log_data')
    filepath = log_files[0]
    df1 = pd1.read_json(filepath, lines=True)
    
    #create multiple dataframaes pointing to the above dataframe which can be used later
    df2 = df1
    df3 = df1

    
    # filter by NextSong action
    df1 = df1['ts']

    # convert timestamp column to datetime
    t =  pd1.to_datetime( df1 , unit = 'ms') 
    
    # create a time dataframe and then insert time data records
    time_data = [  df1 , t.dt.hour  ,  t.dt.day  , t.dt.weekofyear , t.dt.month, t.dt.year, t.dt.weekday ]
    column_labels = ["timestamp", "hour", "day", "week", "month", "year", "Weekday"]
    time_df = pd1.DataFrame(time_data, index = column_labels) 
    time_df = time_df.T 

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

        
    # load user table
    user_df = df2[ ['userId', 'firstName', 'lastName', 'gender', 'level'] ]
    user_df = user_df.dropna(how='any',axis=0)

    
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

        
    # insert songplay records
    for index, row in df3.iterrows():
     # get songid and artistid from song and artist tables
     if row.song != None and row.artist != None and row.length != None:
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        if results:
            songid, artistid = results
            print(songid, artistid)
        else:
            songid, artistid = None, None
       
        # insert songplay record
        songplay_data = ( row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent )
        cur.execute( songplay_table_insert, songplay_data )
         


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

            
    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    
    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data( cur, conn, filepath='data/song_data', func= process_song_file )
    process_data( cur, conn, filepath='data/log_data', func= process_log_file )

    conn.close()


if __name__ == "__main__":
    main()