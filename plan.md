# Clipcast V2 Architecture Plan

This document outlines the complete transition from the V1 fragmented database to the V2 Columnar Mega-Table architecture.

## 1. V2 Database Schema (`generate_db.py`)

The pipeline must compress the existing 5 fragmented tables down into 3 optimized tiers:

### File 1: `search_index.parquet` (~26.7 MB)
*Pre-loaded into OPFS on page load. Powers instant offline fuzzy searching.*
- `nconst` (VARCHAR)
- `primaryName` (VARCHAR)
- `popularityScore` (INTEGER)

### File 2: `actor_details.parquet` (~62.4 MB)
*Queried lazily via HTTP Range Requests when an actor chip is selected.*
- `nconst` (VARCHAR)
- `primaryProfession` (VARCHAR)
- `movieCount` (INTEGER)
- `profile_path` (VARCHAR)
- `movies` (VARCHAR[]) ➔ *Generated using `list(DISTINCT tconst)` grouped by `nconst`. Completely replaces `principals.parquet`.*

### File 3: `movies.parquet` (~30.4 MB)
*Queried lazily via HTTP Range Requests to render the final shared movie results.*
- `tconst` (VARCHAR)
- `primaryTitle` (VARCHAR)
- `startYear` (INTEGER)
- `genres` (VARCHAR)
- `rating` (DOUBLE)
- `titleType` (VARCHAR)
- `poster_path` (VARCHAR) ➔ *Merged directly from the old `movie_posters` cache.*

**Stage 1 Migration Constraints:**
To avoid triggering a massive 3-hour TMDB API run during migration, `generate_db.py` must hardcode a download of the existing V1 files (`actor_posters.parquet` and `movie_posters.parquet`) from `https://clipcast.peithonking.com/static/data/` to use as the image fallback cache.

---

## 2. Frontend Edits (`index.html`)
- Remove `<button id="offlineBtn">` completely.
- Remove the `<div id="downloadToast">` popup completely.
- Ensure the `<input type="text" id="searchBox">` starts with the `disabled` attribute by default.

---

## 3. Frontend Logic (`app.js`)

### 1. OPFS Silent Cache (Stale-While-Revalidate)
- Remove manual download logic. 
- On `initDB()`, fetch `metadata.json`. Check the OPFS timestamp against the remote timestamp.
- If the OPFS `search_index.parquet` is missing or >30 days old (2,592,000 seconds), silently download it in the background using a normal `fetch()`.
- Show a subtle loading spinner next to the search box while downloading.
- Once registered with DuckDB WASM, set `searchBox.disabled = false`.

### 2. HTTP Range Request Intersection Query
- The `searchBox` Jaro-Winkler query should read from the local OPFS `search_index.parquet`.
- `fetchChipPhotos` should read from the remote `actor_details.parquet`.
- `runIntersectionQuery` must use recursive `list_intersect()` against the `movies` arrays in `actor_details.parquet`.
- To avoid DuckDB scalar subquery restrictions, construct the query using CTEs (e.g., `WITH a0 AS (SELECT movies...), a1 AS (SELECT movies...) SELECT list_intersect(a0.movies, a1.movies)`).
- The final SELECT should read from the remote `movies.parquet`.
- **Crucial:** Delete the asynchronous `fetchMoviePosters()` function completely, as `poster_path` is now natively returned in the main intersection query.

### 3. Shareable Deep Linking (Self-Healing URL)
- Create an `updateUrlState()` function that writes `?actors=nm1,nm2&movies=true&tv=false` to the URL using `window.history.replaceState`.
- Call this function inside `addActorChip`, `removeActorChip`, and the TV/Movie filter checkbox event listeners.
- Create a `loadFromUrl()` function triggered after OPFS is initialized. It should:
  1. Extract valid `nm...` strings from the URL (stripping out user tampering).
  2. Instantly call `updateUrlState()` to rewrite the browser URL with only the valid IDs (self-healing).
  3. Fetch the real names for those IDs from `search_index.parquet`.
  4. Hydrate the `selectedActors` state and execute `runIntersectionQuery()` once.
