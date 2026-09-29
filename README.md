# IMDb Search Engine

A fully static, lightning-fast IMDb search engine running entirely in the browser using [DuckDB-Wasm](https://duckdb.org/docs/api/wasm/overview).

## Architecture
- **Frontend:** Pure HTML/JS/CSS. No build steps (Vite/Webpack). Zero server required.
- **Database:** DuckDB-Wasm executes SQL queries on compressed `.parquet` files via HTTP Range Requests.
- **CI/CD:** GitHub Actions automatically downloads the massive 6GB raw IMDb TSV files, crunches them into a highly optimized 67MB Parquet payload using Python (`uv`), and deploys directly to GitHub Pages without polluting the git history.

## Search Ranking
Uses a custom composite search ranking algorithm:
- Initial fuzzy string match via `jaro_winkler_similarity()`.
- String similarity is cubed to exponentially punish typos.
- Weighted against a pre-calculated `popularityScore` (Lead/Co-Lead credits only) via `log10()`.

## Local Development
To run the web app locally, you need a local server (because DuckDB-Wasm relies on HTTP Range requests which fail on `file://` protocols).

1. Generate the databases:
```bash
uv run generate_db.py
```

2. Serve the app:
```bash
python3 -m http.server 8000
```
Open `http://localhost:8000` in your browser.
