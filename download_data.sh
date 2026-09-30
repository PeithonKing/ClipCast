#!/bin/bash
set -e

echo "Downloading IMDb datasets..."
wget -c https://datasets.imdbws.com/name.basics.tsv.gz
wget -c https://datasets.imdbws.com/title.basics.tsv.gz
wget -c https://datasets.imdbws.com/title.principals.tsv.gz
wget -c https://datasets.imdbws.com/title.ratings.tsv.gz

echo "Generating DuckDB Parquet databases directly from .gz files..."
uv run generate_db.py

echo "Done! The PWA is ready to serve."
