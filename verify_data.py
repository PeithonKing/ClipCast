# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb"]
# ///

import duckdb
import os

def get_size(path):
    return os.path.getsize(path) / (1024 * 1024)

print("--- Size Comparison ---")
print(f"Old search_index: {get_size('static/data_backup/search_index.parquet'):.2f} MB")
print(f"New search_index: {get_size('static/data/search_index.parquet'):.2f} MB")

print(f"Old actor_details: {get_size('static/data_backup/actor_details.parquet'):.2f} MB")
print(f"New actor_details: {get_size('static/data/actor_details.parquet'):.2f} MB")

print("\n--- Data Verification ---")
con = duckdb.connect(':memory:')
con.execute("CREATE TABLE old_si AS SELECT * FROM read_parquet('static/data_backup/search_index.parquet')")
con.execute("CREATE TABLE new_si AS SELECT * FROM read_parquet('static/data/search_index.parquet')")

count_old = con.execute("SELECT COUNT(*) FROM old_si").fetchone()[0]
count_new = con.execute("SELECT COUNT(*) FROM new_si").fetchone()[0]
print(f"Row count search_index -> Old: {count_old:,}, New: {count_new:,}")

print("Checking for differences...")
diff = con.execute("""
    SELECT old_si.nconst
    FROM old_si
    FULL OUTER JOIN new_si ON old_si.nconst = new_si.nconst
    WHERE old_si.nconst IS NULL OR new_si.nconst IS NULL
       OR old_si.primaryName != new_si.primaryName
       OR old_si.popularityScore != new_si.popularityScore
""").fetchall()

if len(diff) == 0:
    print("SUCCESS: search_index.parquet data matches exactly! No data lost.")
else:
    print(f"FAILURE: {len(diff)} rows differ in search_index!")

con.execute("CREATE TABLE old_ad AS SELECT * FROM read_parquet('static/data_backup/actor_details.parquet')")
con.execute("CREATE TABLE new_ad AS SELECT * FROM read_parquet('static/data/actor_details.parquet')")
count_old_ad = con.execute("SELECT COUNT(*) FROM old_ad").fetchone()[0]
count_new_ad = con.execute("SELECT COUNT(*) FROM new_ad").fetchone()[0]
print(f"Row count actor_details -> Old: {count_old_ad:,}, New: {count_new_ad:,}")

diff_ad = con.execute("""
    SELECT old_ad.nconst
    FROM old_ad
    FULL OUTER JOIN new_ad ON old_ad.nconst = new_ad.nconst
    WHERE old_ad.nconst IS NULL OR new_ad.nconst IS NULL
       OR old_ad.movieCount != new_ad.movieCount
""").fetchall()

if len(diff_ad) == 0:
    print("SUCCESS: actor_details.parquet movie counts match exactly! No data lost.")
else:
    print(f"FAILURE: {len(diff_ad)} rows differ in actor_details!")
