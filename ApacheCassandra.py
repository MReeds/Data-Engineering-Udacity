import cassandra

from cassandra.cluster import Cluster
try: 
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
except Exception as e:
    print(e)
 
 ## TO-DO: Create the keyspace
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS Music 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)
    
###Set Keyspace
try:
    session.set_keyspace('Music')
except Exception as e:
    print(e)
    
    
###Create Table
query = "CREATE TABLE IF NOT EXISTS Music"
query = query + "(artist_name text, song_title text, year int, album_name text, single boolean, PRIMARY KEY (year, artist_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)

    
###Add values to table
query = "Music" 
query = query + " VALUES (%s, %s, %s, %s, %s)"

try:
    session.execute(query, ('The Beatles', 'Across the Universe', 1970, 'Let It Be', False))
except Exception as e:
    print(e)
    
try:
    session.execute(query, ('The Beatles', 'Think for Yourself', 1965, 'Rubber Soul', True))
except Exception as e:
    print(e)

    
###Select values from table
query = 'SELECT * FROM Music'
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.album_name, row.artist_name)

    
##TO-DO: Complete the select statement to run the query 
query = "Music"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.album_name, row.artist_name)
    
session.shutdown()
cluster.shutdown()
