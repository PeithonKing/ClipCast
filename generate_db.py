# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "duckdb",
#     "requests",
#     "python-dotenv",
# ]
# ///

import duckdb
import time
import os
import sys
import json
import hashlib
import requests
import urllib.request
from tqdm import tqdm
import tempfile

# Load .env if present (local dev)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

SITE_URL = "https://clipcast.peithonking.com/static/data"
TMDB_BASE = "https://api.themoviedb.org/3"

print("--- Starting IMDB Parquet Generation ---")
os.makedirs('static/data', exist_ok=True)

con = duckdb.connect(':memory:')

# 1. BASICS (Movies + TV Shows)
print("1/3: Filtering basics...")
start = time.time()
con.execute("""
    CREATE TABLE basics AS
    SELECT b.tconst, b.primaryTitle, b.startYear, b.genres, r.averageRating as rating, b.titleType
    FROM read_csv_auto('title.basics.tsv.gz', delim='\t', nullstr='\\N', quote='') b
    LEFT JOIN read_csv_auto('title.ratings.tsv.gz', delim='\t', nullstr='\\N', quote='') r ON b.tconst = r.tconst
    WHERE b.titleType IN ('movie', 'tvMovie', 'tvSeries', 'tvMiniSeries')
""")
con.execute("COPY basics TO 'static/data/basics.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD')")
print(f"  -> basics.parquet saved in {time.time() - start:.2f}s")

# 2. PRINCIPALS (Filtered cast/crew linked to valid titles)
print("2/3: Filtering principals...")
start = time.time()
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
con.execute("CREATE TABLE principals AS SELECT tconst, nconst FROM principals_raw")
con.execute("COPY principals TO 'static/data/principals.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD')")
print(f"  -> principals.parquet saved in {time.time() - start:.2f}s")

# 3. NAMES (with popularity score)
print("3/3: Filtering names and scoring...")
start = time.time()
con.execute("""
    CREATE TABLE actor_counts AS
    SELECT
        nconst,
        COUNT(tconst) as movieCount,
        SUM(CASE WHEN (ordering <= 4 AND category IN ('actor', 'actress')) OR category IN ('director', 'writer', 'producer', 'composer') THEN 1 ELSE 0 END) as popularityScore
    FROM principals_raw
    GROUP BY nconst
""")
con.execute("""
    CREATE TABLE names AS
    SELECT
        n.nconst,
        n.primaryName,
        n.primaryProfession,
        c.movieCount,
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
con.execute("COPY names TO 'static/data/names.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD')")
print(f"  -> names.parquet saved in {time.time() - start:.2f}s")

# Write metadata.json (only the 3 OPFS-cached files)
def get_file_hash(filepath):
    h = hashlib.sha256()
    with open(filepath, 'rb') as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()

core_files = ['basics.parquet', 'principals.parquet', 'names.parquet']
metadata = {"compiled_at": int(time.time()), "total_bytes": 0, "files": {}}
for f in core_files:
    filepath = f'static/data/{f}'
    size = os.path.getsize(filepath)
    metadata["files"][f] = {"size": size, "hash": get_file_hash(filepath)}
    metadata["total_bytes"] += size

with open('static/data/metadata.json', 'w') as mf:
    json.dump(metadata, mf)
print(f"  -> metadata.json written ({metadata['total_bytes'] / 1024 / 1024:.1f} MB total)")
print("--- Core Generation Complete ---")

# -----------------------------------------------------------------------
# TMDB POSTER FETCHING (Only runs if TMDB_API_KEY is available)
# -----------------------------------------------------------------------
api_key = os.environ.get('TMDB_API_KEY')
if not api_key:
    print("\nNo TMDB_API_KEY found. Skipping poster generation.")
    print("Set TMDB_API_KEY in .env (local) or GitHub Secrets (CI).")
    sys.exit(0)

print("\n--- Starting TMDB Poster Generation ---")

def try_load_existing_parquet(con, table_name, remote_filename, columns_ddl):
    """Pull the currently-deployed parquet as checkpoint. Falls back to empty table."""
    url = f"{SITE_URL}/{remote_filename}"
    try:
        print(f"  Pulling existing {remote_filename} from live site...", flush=True)
        req = urllib.request.Request(url, headers={'User-Agent': 'ClipCast-CI/1.0'})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = r.read()
        tmp = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
        tmp.write(data)
        tmp.close()
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{tmp.name}')")
        os.unlink(tmp.name)
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"  Loaded {count:,} existing entries from live site.", flush=True)
    except Exception as e:
        print(f"  Could not load existing data: {e}. Starting fresh.", flush=True)
        con.execute(f"CREATE TABLE {table_name} ({columns_ddl})")

def tmdb_fetch(imdb_id, api_key, max_retries=6):
    """Fetch TMDB find result for an IMDb ID with exponential backoff on 429."""
    url = f"{TMDB_BASE}/find/{imdb_id}"
    params = {"external_source": "imdb_id", "api_key": api_key}
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                wait = min(64, 2 ** attempt)
                print(f"    429 Rate limited. Waiting {wait}s...", flush=True)
                time.sleep(wait)
            elif r.status_code == 404:
                return None
            else:
                # Unexpected error: wait briefly and retry
                wait = min(64, 2 ** attempt)
                print(f"    HTTP {r.status_code}. Waiting {wait}s...", flush=True)
                time.sleep(wait)
        except requests.RequestException as e:
            wait = min(64, 2 ** attempt)
            print(f"    Request error: {e}. Waiting {wait}s...", flush=True)
            time.sleep(wait)
    return None

def safe_insert(con, table, cols, vals):
    """Insert a row, escaping string values for DuckDB."""
    parts = []
    for v in vals:
        if v is None:
            parts.append("NULL")
        else:
            escaped = str(v).replace("'", "''")
            parts.append(f"'{escaped}'")
    con.execute(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(parts)})")

# Step 4a: Movie Posters (3 hour cap)
print(f"\n4a: Fetching movie posters (cap: 3 hours)...")
MOVIE_DEADLINE = time.time() + 3 * 3600

try_load_existing_parquet(
    con, 'movie_posters', 'movie_posters.parquet',
    'tconst VARCHAR, poster_path VARCHAR'
)

missing_movies = con.execute("""
    SELECT b.tconst, b.titleType
    FROM basics b
    LEFT JOIN movie_posters mp ON b.tconst = mp.tconst
    WHERE mp.tconst IS NULL
    ORDER BY b.tconst
""").fetchall()

print(f"  {len(missing_movies):,} titles still need poster lookup.", flush=True)
fetched = 0
with tqdm(total=len(missing_movies), desc="Movies", unit="req") as pbar:
    for tconst, title_type in missing_movies:
        if time.time() > MOVIE_DEADLINE:
            print(f"\n  Cap reached after {fetched:,} fetches this run. Will resume next run.")
            break

        data = tmdb_fetch(tconst, api_key)
        poster_path = None
        if data:
            results = data.get('movie_results') or data.get('tv_results') or []
            if results:
                poster_path = results[0].get('poster_path')

        safe_insert(con, 'movie_posters', ['tconst', 'poster_path'], [tconst, poster_path])
        fetched += 1
        pbar.update(1)

con.execute("COPY movie_posters TO 'static/data/movie_posters.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD')")
total_m = con.execute("SELECT COUNT(*) FROM movie_posters").fetchone()[0]
with_poster = con.execute("SELECT COUNT(*) FROM movie_posters WHERE poster_path IS NOT NULL").fetchone()[0]
print(f"  -> movie_posters.parquet saved ({total_m:,} entries, {with_poster:,} with images).")

# Step 4b: Actor Posters (1 hour cap)
print(f"\n4b: Fetching actor photos (cap: 1 hour)...")
ACTOR_DEADLINE = time.time() + 1 * 3600

try_load_existing_parquet(
    con, 'actor_posters', 'actor_posters.parquet',
    'nconst VARCHAR, profile_path VARCHAR'
)

missing_actors = con.execute("""
    SELECT n.nconst
    FROM names n
    LEFT JOIN actor_posters ap ON n.nconst = ap.nconst
    WHERE ap.nconst IS NULL
    ORDER BY n.nconst
""").fetchall()

print(f"  {len(missing_actors):,} actors still need photo lookup.", flush=True)
fetched = 0
with tqdm(total=len(missing_actors), desc="Actors", unit="req") as pbar:
    for (nconst,) in missing_actors:
        if time.time() > ACTOR_DEADLINE:
            print(f"\n  Cap reached after {fetched:,} fetches this run. Will resume next run.")
            break

        data = tmdb_fetch(nconst, api_key)
        profile_path = None
        if data:
            person_r = data.get('person_results') or []
            if person_r:
                profile_path = person_r[0].get('profile_path')

        safe_insert(con, 'actor_posters', ['nconst', 'profile_path'], [nconst, profile_path])
        fetched += 1
        pbar.update(1)

con.execute("COPY actor_posters TO 'static/data/actor_posters.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD')")
total_a = con.execute("SELECT COUNT(*) FROM actor_posters").fetchone()[0]
with_photo = con.execute("SELECT COUNT(*) FROM actor_posters WHERE profile_path IS NOT NULL").fetchone()[0]
print(f"  -> actor_posters.parquet saved ({total_a:,} entries, {with_photo:,} with images).")

print("\n--- Generation Complete ---")
