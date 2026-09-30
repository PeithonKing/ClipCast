# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "duckdb",
# ]
# ///

import duckdb
import time
import os

print("--- Starting IMDB Parquet Generation ---")
os.makedirs('webapp/static/data', exist_ok=True)

con = duckdb.connect(':memory:')

# 1. BASICS (Movies only)
print("1/3: Filtering basics (Movies only)...")
start = time.time()
con.execute("""
    CREATE TABLE basics AS 
    SELECT tconst, primaryTitle, startYear, genres 
    FROM read_csv_auto('title.basics.tsv.gz', delim='\t', nullstr='\\N', quote='')
    WHERE titleType = 'movie'
""")
con.execute("COPY basics TO 'static/data/basics.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD')")
print(f"  -> basics.parquet saved in {time.time() - start:.2f}s")

# 2. PRINCIPALS (Movie casts only)
print("2/3: Filtering principals (Linked to movies)...")
start = time.time()
# Join with basics to instantly drop TV show casts, keep ordering/category for scoring
con.execute("""
    CREATE TABLE principals_raw AS 
    SELECT 
        p.tconst, 
        p.nconst, 
        TRY_CAST(p.ordering AS INTEGER) as ordering, 
        p.category 
    FROM read_csv_auto('title.principals.tsv.gz', delim='\t', nullstr='\\N', quote='') p
    INNER JOIN basics b ON p.tconst = b.tconst
""")

# Create the minimal table for the webapp
con.execute("""
    CREATE TABLE principals AS 
    SELECT tconst, nconst FROM principals_raw
""")
con.execute("COPY principals TO 'static/data/principals.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD')")
print(f"  -> principals.parquet saved in {time.time() - start:.2f}s")

# 3. NAMES (Actors/Actresses with movie counts)
print("3/3: Filtering names and calculating popularity scores...")
start = time.time()
# Calculate total movies and top-billed movies
con.execute("""
    CREATE TABLE actor_counts AS 
    SELECT 
        nconst, 
        COUNT(tconst) as movieCount,
        SUM(CASE WHEN ordering <= 4 AND category IN ('actor', 'actress') THEN 1 ELSE 0 END) as popularityScore
    FROM principals_raw 
    GROUP BY nconst
""")

# INNER JOIN drops actors with 0 movies in our database
con.execute("""
    CREATE TABLE names AS 
    SELECT 
        n.nconst, 
        n.primaryName, 
        n.birthYear, 
        n.primaryProfession,
        c.movieCount,
        c.popularityScore
    FROM read_csv_auto('name.basics.tsv.gz', delim='\t', nullstr='\\N', quote='') n
    INNER JOIN actor_counts c ON n.nconst = c.nconst
    WHERE (n.primaryProfession LIKE '%actor%' OR n.primaryProfession LIKE '%actress%')
""")
con.execute("COPY names TO 'static/data/names.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD')")
print(f"  -> names.parquet saved in {time.time() - start:.2f}s")

# Write metadata.json with exact file sizes for the UI
import json
files = ['basics.parquet', 'principals.parquet', 'names.parquet']
metadata = {}
total = 0
for f in files:
    size = os.path.getsize(f'static/data/{f}')
    metadata[f] = size
    total += size
metadata['total'] = total
with open('static/data/metadata.json', 'w') as mf:
    json.dump(metadata, mf)
print(f"  -> metadata.json written ({total / 1024 / 1024:.1f} MB total)")

print("--- Generation Complete ---")
