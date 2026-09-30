#!/bin/bash
set -e

WGET_OPTS="-c"
if [ "$CI" = "true" ]; then
  WGET_OPTS="-q -c"
fi

echo "Downloading IMDb datasets..."
wget $WGET_OPTS https://datasets.imdbws.com/name.basics.tsv.gz
wget $WGET_OPTS https://datasets.imdbws.com/title.basics.tsv.gz
wget $WGET_OPTS https://datasets.imdbws.com/title.principals.tsv.gz
wget $WGET_OPTS https://datasets.imdbws.com/title.ratings.tsv.gz

echo "Generating DuckDB Parquet databases directly from .gz files..."
uv run generate_db.py

echo "Done! The PWA is ready to serve."
