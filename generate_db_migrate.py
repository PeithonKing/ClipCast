# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "duckdb",
#     "requests",
#     "tqdm",
# ]
# ///

import duckdb
import time
import os
import hashlib
import urllib.request
from tqdm import tqdm

is_ci = os.environ.get("CI") == "true"

print("--- Starting Clipcast V2 Database Migration ---")
os.makedirs('static/data', exist_ok=True)

def download_file(url, filename):
    if os.path.exists(filename):
        print(f"  [CACHE] {filename} already exists. Skipping download.")
        return
    
    print(f"  [DOWNLOADING] {url} -> {filename}")
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response:
            total_size = int(response.headers.get('content-length', 0))
            with open(filename, 'wb') as file, tqdm(
                desc=filename,
                total=total_size,
                unit='iB',
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
                while True:
                    chunk = response.read(8192)
                    if not chunk:
                        break
                    file.write(chunk)
                    bar.update(len(chunk))
    except Exception as e:
        print(f"  [ERROR] Failed to download {url}: {e}")
        raise

# 1. Download IMDB TSV.GZ files
print("\n--- 1/3: Downloading Source Data ---")
imdb_base = "https://datasets.imdbws.com"
imdb_files = [
    'title.basics.tsv.gz',
    'title.ratings.tsv.gz',
    'title.principals.tsv.gz',
    'name.basics.tsv.gz'
]
for f in imdb_files:
    download_file(f"{imdb_base}/{f}", f)

# 2. Download V1 Poster Parquets from old server
clipcast_base = "https://clipcast.peithonking.com/static/data"
poster_files = [
    'movie_posters.parquet',
    'actor_posters.parquet'
]
for f in poster_files:
    download_file(f"{clipcast_base}/{f}", f)

print("\n--- 2/3: Processing with DuckDB (V2 Schema) ---")
con = duckdb.connect(':memory:')

# Load Poster Caches
print("  Loading legacy poster caches...")
con.execute("CREATE TABLE movie_posters AS SELECT * FROM read_parquet('movie_posters.parquet')")
con.execute("CREATE TABLE actor_posters AS SELECT * FROM read_parquet('actor_posters.parquet')")

# Create Basics
print("  Processing titles (basics)...")
con.execute("""
    CREATE TABLE basics AS
    SELECT b.tconst, b.primaryTitle, b.startYear, b.genres, r.averageRating as rating, b.titleType
    FROM read_csv_auto('title.basics.tsv.gz', delim='\t', nullstr='\\N', quote='') b
    LEFT JOIN read_csv_auto('title.ratings.tsv.gz', delim='\t', nullstr='\\N', quote='') r ON b.tconst = r.tconst
    WHERE b.titleType IN ('movie', 'tvMovie', 'tvSeries', 'tvMiniSeries')
""")

# Create Principals Link
print("  Processing roles (principals)...")
con.execute("""
    CREATE TABLE principals_raw AS
    SELECT
        p.tconst,
        p.nconst,
        TRY_CAST(p.ordering AS INTEGER) as ordering,
        p.category
    FROM read_csv_auto('title.principals.tsv.gz', delim='\t', nullstr='\\N', quote='') p
    INNER JOIN basics b ON p.tconst = b.tconst
    WHERE p.category IN ('actor', 'actress', 'director', 'writer', 'producer', 'composer')
""")

# Compute Actor Scores & Comma-Separated Movies
print("  Aggregating actor stats & generating comma-separated movie string...")
con.execute("""
    CREATE TABLE actor_counts AS
    SELECT
        nconst,
        CAST(COUNT(DISTINCT tconst) AS INTEGER) as movieCount,
        CAST(SUM(CASE WHEN (ordering <= 4 AND category IN ('actor', 'actress')) OR category IN ('director', 'writer', 'producer', 'composer') THEN 1 ELSE 0 END) AS INTEGER) as popularityScore,
        string_agg(DISTINCT tconst, ',') as movies
    FROM principals_raw
    GROUP BY nconst
""")

# V2 File 1: search_index.parquet
print("  Exporting search_index.parquet (OPFS Cache)...")
con.execute("""
    CREATE TABLE search_index AS
    SELECT
        n.nconst,
        n.primaryName,
        c.popularityScore
    FROM read_csv_auto('name.basics.tsv.gz', delim='\t', nullstr='\\N', quote='') n
    INNER JOIN actor_counts c ON n.nconst = c.nconst
    WHERE (
        n.primaryProfession LIKE '%actor%' OR
        n.primaryProfession LIKE '%actress%' OR
        n.primaryProfession LIKE '%director%' OR
        n.primaryProfession LIKE '%writer%' OR
        n.primaryProfession LIKE '%producer%' OR
        n.primaryProfession LIKE '%composer%'
    )
""")
con.execute("COPY search_index TO 'static/data/search_index.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD')")

# V2 File 2: actor_details.parquet
print("  Exporting actor_details.parquet...")
con.execute("""
    CREATE TABLE actor_details AS
    SELECT
        n.nconst,
        n.primaryProfession,
        CAST(c.movieCount AS INTEGER) as movieCount,
        CASE
            WHEN ap.nconst IS NOT NULL AND ap.profile_path IS NULL THEN ''
            ELSE ap.profile_path
        END as profile_path,
        c.movies
    FROM read_csv_auto('name.basics.tsv.gz', delim='\t', nullstr='\\N', quote='') n
    INNER JOIN actor_counts c ON n.nconst = c.nconst
    LEFT JOIN actor_posters ap ON n.nconst = ap.nconst
    WHERE (
        n.primaryProfession LIKE '%actor%' OR
        n.primaryProfession LIKE '%actress%' OR
        n.primaryProfession LIKE '%director%' OR
        n.primaryProfession LIKE '%writer%' OR
        n.primaryProfession LIKE '%producer%' OR
        n.primaryProfession LIKE '%composer%'
    )
""")
con.execute("COPY actor_details TO 'static/data/actor_details.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD')")

# V2 File 3: movies.parquet
print("  Exporting movies.parquet...")
con.execute("""
    CREATE TABLE movies AS
    SELECT
        b.tconst,
        b.primaryTitle,
        b.startYear,
        b.genres,
        b.rating,
        b.titleType,
        CASE
            WHEN mp.tconst IS NOT NULL AND mp.poster_path IS NULL THEN ''
            ELSE mp.poster_path
        END as poster_path
    FROM basics b
    LEFT JOIN movie_posters mp ON b.tconst = mp.tconst
""")
con.execute("COPY movies TO 'static/data/movies.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD')")

# Generate Metadata
print("\n--- 3/3: Generating Metadata ---")
def get_file_hash(filepath):
    h = hashlib.sha256()
    with open(filepath, 'rb') as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()

core_files = ['search_index.parquet', 'actor_details.parquet', 'movies.parquet']
metadata = {"compiled_at": int(time.time()), "total_bytes": 0, "files": {}}
for f in core_files:
    filepath = f'static/data/{f}'
    size = os.path.getsize(filepath)
    metadata["files"][f] = {"size": size, "hash": get_file_hash(filepath)}
    metadata["total_bytes"] += size

import json
with open('static/data/metadata.json', 'w') as mf:
    json.dump(metadata, mf)

print(f"  -> metadata.json written ({metadata['total_bytes'] / 1024 / 1024:.1f} MB total)")

if is_ci:
    print("\n--- Cleaning up temporary files ---")
    for f in imdb_files + poster_files:
        if os.path.exists(f):
            os.remove(f)
            print(f"  Deleted {f}")

print("--- Migration V2 Complete ---")
