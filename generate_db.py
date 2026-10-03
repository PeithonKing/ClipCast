# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "duckdb",
#     "requests",
#     "python-dotenv",
#     "tqdm",
# ]
# ///

import duckdb
import time
import math
import os
import sys
import json
import hashlib
import requests
import urllib.request
import concurrent.futures
import threading
from tqdm import tqdm
import tempfile

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

is_ci = os.environ.get("CI") == "true"
SITE_URL = "https://clipcast.peithonking.com/static/data"
TMDB_BASE = "https://api.themoviedb.org/3"
os.makedirs('static/data', exist_ok=True)

def download_file(url, filename):
    if os.path.exists(filename):
        print(f"  [CACHE] {filename} already exists.", flush=True)
        return
    print(f"  [DOWNLOADING] {url} -> {filename}", flush=True)
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response:
            total_size = int(response.headers.get('content-length', 0))
            with open(filename, 'wb') as file, tqdm(
                desc=filename, total=total_size, unit='iB',
                unit_scale=True, unit_divisor=1024, disable=is_ci
            ) as bar:
                while True:
                    chunk = response.read(8192)
                    if not chunk: break
                    file.write(chunk)
                    bar.update(len(chunk))
    except Exception as e:
        print(f"  [ERROR] Failed to download {url}: {e}", flush=True)

print("--- Starting Clipcast V2 Database Generation ---")
print("\n--- 1/4: Downloading Source Data ---")
imdb_base = "https://datasets.imdbws.com"
for f in ['title.basics.tsv.gz', 'title.ratings.tsv.gz', 'title.principals.tsv.gz', 'name.basics.tsv.gz']:
    download_file(f"{imdb_base}/{f}", f)

print("\n--- 2/4: Processing with DuckDB ---")
con = duckdb.connect(':memory:')

# Try to load existing V2 cache to reuse image paths
print("  Attempting to load live caches...", flush=True)
try:
    download_file(f"{SITE_URL}/movies.parquet", "old_movies.parquet")
    con.execute("CREATE TABLE old_movies AS SELECT * FROM read_parquet('old_movies.parquet')")
except Exception:
    print("  [WARN] old_movies.parquet not found. Starting fresh.")
    con.execute("CREATE TABLE old_movies (tconst VARCHAR, poster_path VARCHAR)")

try:
    download_file(f"{SITE_URL}/actor_details.parquet", "old_actor_details.parquet")
    con.execute("CREATE TABLE old_actors AS SELECT * FROM read_parquet('old_actor_details.parquet')")
except Exception:
    print("  [WARN] old_actor_details.parquet not found. Starting fresh.")
    con.execute("CREATE TABLE old_actors (nconst VARCHAR, profile_path VARCHAR)")

print("  Processing titles (basics)...", flush=True)
con.execute("""
    CREATE TABLE basics AS
    SELECT b.tconst, b.primaryTitle, b.startYear, b.genres, r.averageRating as rating, b.titleType
    FROM read_csv_auto('title.basics.tsv.gz', delim='\t', nullstr='\\N', quote='') b
    LEFT JOIN read_csv_auto('title.ratings.tsv.gz', delim='\t', nullstr='\\N', quote='') r ON b.tconst = r.tconst
    WHERE b.titleType IN ('movie', 'tvMovie', 'tvSeries', 'tvMiniSeries')
""")

print("  Processing roles (principals)...", flush=True)
con.execute("""
    CREATE TABLE principals_raw AS
    SELECT
        p.tconst, p.nconst, TRY_CAST(p.ordering AS INTEGER) as ordering, p.category
    FROM read_csv_auto('title.principals.tsv.gz', delim='\t', nullstr='\\N', quote='') p
    INNER JOIN basics b ON p.tconst = b.tconst
    WHERE p.category IN ('actor', 'actress', 'director', 'writer', 'producer', 'composer')
""")

print("  Aggregating actor stats...", flush=True)
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

print("  Building search_index...", flush=True)
con.execute("""
    CREATE TABLE search_index AS
    SELECT n.nconst, n.primaryName, c.popularityScore
    FROM read_csv_auto('name.basics.tsv.gz', delim='\t', nullstr='\\N', quote='') n
    INNER JOIN actor_counts c ON n.nconst = c.nconst
    WHERE n.primaryProfession LIKE '%actor%' OR n.primaryProfession LIKE '%actress%' OR n.primaryProfession LIKE '%director%' OR n.primaryProfession LIKE '%writer%' OR n.primaryProfession LIKE '%producer%' OR n.primaryProfession LIKE '%composer%'
""")
con.execute("COPY search_index TO 'static/data/search_index.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD')")

print("  Building actor_details...", flush=True)
con.execute("""
    CREATE TABLE actor_details AS
    SELECT n.nconst, n.primaryProfession, c.movieCount, COALESCE(oa.profile_path, NULL) as profile_path, c.movies
    FROM read_csv_auto('name.basics.tsv.gz', delim='\t', nullstr='\\N', quote='') n
    INNER JOIN actor_counts c ON n.nconst = c.nconst
    LEFT JOIN old_actors oa ON n.nconst = oa.nconst
    WHERE n.primaryProfession LIKE '%actor%' OR n.primaryProfession LIKE '%actress%' OR n.primaryProfession LIKE '%director%' OR n.primaryProfession LIKE '%writer%' OR n.primaryProfession LIKE '%producer%' OR n.primaryProfession LIKE '%composer%'
""")
con.execute("COPY actor_details TO 'static/data/actor_details.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD')")

print("  Building movies...", flush=True)
con.execute("""
    CREATE TABLE movies AS
    SELECT b.tconst, b.primaryTitle, b.startYear, b.genres, b.rating, b.titleType, COALESCE(om.poster_path, NULL) as poster_path
    FROM basics b
    LEFT JOIN old_movies om ON b.tconst = om.tconst
""")
con.execute("COPY movies TO 'static/data/movies.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD')")


# -----------------------------------------------------------------------
# TMDB POSTER FETCHING
# -----------------------------------------------------------------------
api_key = os.environ.get('TMDB_API_KEY')
if not api_key:
    print("\nNo TMDB_API_KEY found. Skipping poster generation.")
    sys.exit(0)

print("\n--- 3/4: TMDB Poster Generation ---")
global_backoff_until = 0
backoff_lock = threading.Lock()

def tmdb_fetch(imdb_id, api_key, max_retries=6):
    global global_backoff_until
    url = f"{TMDB_BASE}/find/{imdb_id}"
    params = {"external_source": "imdb_id", "api_key": api_key}
    
    for attempt in range(max_retries):
        now = time.time()
        if now < global_backoff_until:
            time.sleep(global_backoff_until - now + 0.1)
            
        try:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                with backoff_lock:
                    if global_backoff_until <= time.time():
                        wait = min(64, 2 ** attempt)
                        global_backoff_until = time.time() + wait
                if wait > 0: time.sleep(wait)
            elif r.status_code == 404:
                return None
            else:
                with backoff_lock:
                    if global_backoff_until <= time.time():
                        wait = min(64, 2 ** attempt)
                        global_backoff_until = time.time() + wait
                if wait > 0: time.sleep(wait)
        except requests.RequestException:
            with backoff_lock:
                if global_backoff_until <= time.time():
                    wait = min(64, 2 ** attempt)
                    global_backoff_until = time.time() + wait
            if wait > 0: time.sleep(wait)
    return None

def process_movie(item, api_key):
    tconst = item[0]
    data = tmdb_fetch(tconst, api_key)
    poster_path = "" # Empty string sentinel for "checked, not found"
    if data:
        results = data.get('movie_results') or data.get('tv_results') or []
        if results:
            poster_path = results[0].get('poster_path') or ""
    return tconst, poster_path

def process_actor(item, api_key):
    nconst = item[0]
    data = tmdb_fetch(nconst, api_key)
    profile_path = "" # Empty string sentinel for "checked, not found"
    if data:
        person_r = data.get('person_results') or []
        if person_r:
            profile_path = person_r[0].get('profile_path') or ""
    return nconst, profile_path

def chunk_iterable(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

max_threads = int(os.environ.get('MAX_THREADS', 20))

# 3a. Movies
movie_limit_mins = int(os.environ.get('MOVIE_LIMIT_MINS', 150))
print(f"\n3a: Fetching movie posters (cap: {movie_limit_mins} mins)...")
movie_start_time = time.time()
MOVIE_DEADLINE = movie_start_time + movie_limit_mins * 60

missing_movies = con.execute("SELECT tconst FROM movies WHERE poster_path IS NULL ORDER BY tconst").fetchall()
print(f"  {len(missing_movies):,} titles still need poster lookup.", flush=True)
con.execute("CREATE TABLE tmp_movies (tconst VARCHAR, poster_path VARCHAR)")

fetched = 0
with tqdm(total=len(missing_movies), desc="Movies", unit="req", disable=is_ci) as pbar:
    cap_reached = False
    for chunk in chunk_iterable(missing_movies, 10000):
        if cap_reached: break
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            future_to_item = {executor.submit(process_movie, item, api_key): item for item in chunk}
            batch = []
            for future in concurrent.futures.as_completed(future_to_item):
                if time.time() > MOVIE_DEADLINE:
                    executor.shutdown(wait=False, cancel_futures=True)
                    print(f"\n  Cap reached after {fetched:,} fetches this run.")
                    cap_reached = True
                    break
                    
                tconst, poster_path = future.result()
                batch.append((tconst, poster_path))
                fetched += 1
                pbar.update(1)
                
            if batch:
                con.executemany("INSERT INTO tmp_movies (tconst, poster_path) VALUES (?, ?)", batch)
                con.execute("UPDATE movies SET poster_path = tmp.poster_path FROM tmp_movies tmp WHERE movies.tconst = tmp.tconst")
                con.execute("DELETE FROM tmp_movies")
                con.execute("COPY movies TO 'static/data/movies.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD')")
            del future_to_item

# 3b. Actors
actor_limit_mins = int(os.environ.get('ACTOR_LIMIT_MINS', 150))
print(f"\n3b: Fetching actor photos (cap: {actor_limit_mins} mins)...")
actor_start_time = time.time()
ACTOR_DEADLINE = actor_start_time + actor_limit_mins * 60

missing_actors = con.execute("SELECT nconst FROM actor_details WHERE profile_path IS NULL ORDER BY nconst").fetchall()
print(f"  {len(missing_actors):,} actors still need photo lookup.", flush=True)
con.execute("CREATE TABLE tmp_actors (nconst VARCHAR, profile_path VARCHAR)")

fetched = 0
with tqdm(total=len(missing_actors), desc="Actors", unit="req", disable=is_ci) as pbar:
    cap_reached = False
    for chunk in chunk_iterable(missing_actors, 10000):
        if cap_reached: break
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            future_to_item = {executor.submit(process_actor, item, api_key): item for item in chunk}
            batch = []
            for future in concurrent.futures.as_completed(future_to_item):
                if time.time() > ACTOR_DEADLINE:
                    executor.shutdown(wait=False, cancel_futures=True)
                    print(f"\n  Cap reached after {fetched:,} fetches this run.")
                    cap_reached = True
                    break
                    
                nconst, profile_path = future.result()
                batch.append((nconst, profile_path))
                fetched += 1
                pbar.update(1)
                
            if batch:
                con.executemany("INSERT INTO tmp_actors (nconst, profile_path) VALUES (?, ?)", batch)
                con.execute("UPDATE actor_details SET profile_path = tmp.profile_path FROM tmp_actors tmp WHERE actor_details.nconst = tmp.nconst")
                con.execute("DELETE FROM tmp_actors")
                con.execute("COPY actor_details TO 'static/data/actor_details.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD')")
            del future_to_item

print("\n--- 4/4: Generating Metadata ---")
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

with open('static/data/metadata.json', 'w') as mf:
    json.dump(metadata, mf)

print(f"  -> metadata.json written ({metadata['total_bytes'] / 1024 / 1024:.1f} MB total)")

if is_ci:
    print("\n--- Cleaning up temporary files ---")
    files_to_delete = [
        'title.basics.tsv.gz', 'title.ratings.tsv.gz', 
        'title.principals.tsv.gz', 'name.basics.tsv.gz',
        'old_movies.parquet', 'old_actor_details.parquet'
    ]
    for f in files_to_delete:
        if os.path.exists(f):
            os.remove(f)
            print(f"  Deleted {f}")

print("--- Generation Complete ---")
