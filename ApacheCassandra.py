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
    
try:
    session.set_keyspace('Music')
except Exception as e:
    print(e)
    
query = "CREATE TABLE IF NOT EXISTS Music"
query = query + "(artist_name text, song_title text, year int, album_name text, single boolean, PRIMARY KEY (year, artist_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)
