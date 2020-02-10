# DROP TABLES
songplay_table_drop = "DROP TABLE IF EXISTS Songplay"
user_table_drop = "DROP TABLE IF EXISTS Users"
song_table_drop = "DROP TABLE IF EXISTS Songs"
artist_table_drop = "DROP TABLE IF EXISTS Artist"
time_table_drop = "DROP TABLE IF EXISTS Time"


# CREATE TABLES(facts and dimensions)
songplay_table_create = (
"""CREATE TABLE IF NOT EXISTS Songplay ( 
     song_id_play SERIAL PRIMARY KEY, 
     timestamp BIGINT not null , 
     userId int not null, 
     level varchar, 
     song_id varchar, 
     artist_id varchar, 
     sessionId varchar, 
     location varchar, 
     userAgent varchar )"""

)

user_table_create = (
"""CREATE TABLE IF NOT EXISTS Users ( 
     user_id int not null primary key, 
     firstName varchar not null, 
     lastName varchar not null, 
     gender Varchar, 
     level varchar) """ 
)


song_table_create = (
"""CREATE TABLE IF NOT EXISTS Songs ( 
     song_id varchar not null primary key, 
     title varchar, 
     artist_id varchar, 
     year int, 
     duration float )"""
)

artist_table_create = (
"""CREATE TABLE IF NOT EXISTS Artist ( 
    artist_id varchar not null primary key, 
    artist_name varchar not null , 
    location varchar, 
    latitude float, 
    longitude float )"""
)

time_table_create = (
"""CREATE TABLE IF NOT EXISTS Time (
    timestamp BIGINT not null primary key, 
    hour int not null, 
    day int not null, 
    week int not null, 
    month int not null, 
    year int not null, 
    Weekday int not null)"""
)



# INSERT RECORDS
songplay_table_insert = ("""Insert into Songplay( timestamp , userId , level , song_id , artist_id , sessionId, location , userAgent ) values ( %s,%s,%s,%s,%s,%s,%s,%s );
""")

user_table_insert = ("""Insert into Users( user_id, firstName , lastName , gender, level ) values (%s,%s,%s,%s,%s)
ON CONFLICT(user_id) DO UPDATE SET level = excluded.level;
""")

song_table_insert = ("""Insert into Songs( song_id, title , artist_id , year, duration ) values (%s,%s,%s,%s,%s)
ON CONFLICT(song_id) DO NOTHING;
""")

artist_table_insert = ("""Insert into Artist( artist_id , artist_name , location , latitude , longitude ) values (%s,%s,%s,%s,%s)
ON CONFLICT(artist_id) DO NOTHING;
""")

time_table_insert = ("""Insert into Time( timestamp , hour , day , week , month , year , Weekday ) values (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT(timestamp) DO NOTHING;
""")


# FIND SONGS
song_select = ("""select Songs.song_id, Songs.artist_id from 
                  Songs INNER JOIN Artist 
                  ON Songs.artist_id = Artist.artist_id
                  where Songs.title = %s AND  Artist.artist_name = %s AND Songs.duration = %s 
               """)


# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]