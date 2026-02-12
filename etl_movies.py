import psycopg2
from datetime import datetime

try:
	conn = psycopg2.connect(
		dbname="dm_movies",
		user="cliffe",
		password="BLOOMberg411",
		host="localhost"
		)
	conn.autocommit = False
	cur = conn.cursor()

except Exception as e:
	print(f"Connection Failed: {e}")
	exit()

def setup():
	cur.execute("DROP TABLE IF EXISTS dim_users CASCADE")
	cur.execute("DROP TABLE IF EXISTS dim_movies CASCADE")
	cur.execute("DROP TABLE IF EXISTS fact_streams CASCADE")

	cur.execute("""
		CREATE TABLE dim_users(
			user_id SERIAL PRIMARY KEY,
			email VARCHAR(255) UNIQUE NOT NULL
		);
		""")

	cur.execute("""
		CREATE TABLE dim_movies(
			movie_id SERIAL PRIMARY KEY,
			title VARCHAR(255),
			genre VARCHAR(50),
			UNIQUE(title, genre)
		);
		""")

	cur.execute("""
		CREATE TABLE fact_streams(
			stream_id SERIAL PRIMARY KEY,
			user_id INT REFERENCES dim_users(user_id),
			movie_id INT REFERENCES dim_movies(movie_id),
			timestamp TIMESTAMP,
			duration INT
		);
		""")
	conn.commit()
	print("Schema Inititalized")

def get_or_create_user(email):
	cur.execute("""
		SELECT user_id
		FROM dim_users
		WHERE email = %s;
		"""
		, (email,)
		)
	result = cur.fetchone()
	
	if result:
		return result[0]
	else:
		cur.execute("INSERT INTO dim_users(email) VALUES (%s) RETURNING user_id;", (email,))
		new_id = cur.fetchone()[0]
		conn.commit()
		return new_id

def get_or_create_movie(title, genre):
	cur.execute("""
		SELECT movie_id
		FROM dim_movies
		WHERE title = %s AND genre = %s;
		""", (title, genre)
		)
	result = cur.fetchone()
	
	if result:
		return result[0]
	else:
		cur.execute("INSERT INTO dim_movies(title,genre) VALUES (%s, %s) RETURNING movie_id", (title, genre))
		new_id = cur.fetchone()[0]
		conn.commit()
		return new_id

def run_pipeline():
	setup()

	raw_data = [
    		{"timestamp": "2024-01-01 10:00:00", "user_email": "alice@example.com", "movie_title": "The Matrix", "genre": "Sci-Fi", "duration": 136},
    		{"timestamp": "2024-01-01 12:00:00", "user_email": "bob@example.com",   "movie_title": "Inception",  "genre": "Sci-Fi", "duration": 148},
	    	{"timestamp": "2024-01-02 09:30:00", "user_email": "alice@example.com", "movie_title": "Inception",  "genre": "Sci-Fi", "duration": 148}, # Alice watches Inception
		]
	
	print("Starting ETL process...")

	temp = []

	for data in raw_data:
		user_id = get_or_create_user(data['user_email'])
		movie_id = get_or_create_movie(data['movie_title'], data['genre'])
		
		print(f"Inserting Stream: User {user_id} watched movie {movie_id}")
		cols = (user_id, movie_id, data['timestamp'], data['duration'])
		temp.append(cols)

	if temp:
		print(f"Inserting {len(temp)} rows into database")
		query = """
			INSERT INTO fact_streams(user_id, movie_id, timestamp, duration)
			VALUES(%s,%s,%s,%s)
			"""
		cur.executemany(query, temp)
		conn.commit()
		print("Batch load complete.")

	cur.execute("SELECT * FROM fact_streams;")
	print("\nFinal fact table:")
	for data in cur.fetchall():
		print(data)
	
	cur.close()
	conn.close()
	
if __name__ == "__main__":
	run_pipeline()
