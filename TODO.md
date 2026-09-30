# Clipcast Future Roadmap (TODO)

## 1. Deep Linking (Shareable URLs)
- [ ] Automatically update the browser URL to include query parameters when actors are selected (e.g., `?actors=nm00001,nm00002`).
- [ ] On page load, read the URL parameters and instantly populate the actor chips and run the intersection query.
- [ ] **Benefit:** Users can copy-paste URLs to share specific actor combinations with friends.

## 2. Search History (Offcanvas Drawer)
- [ ] Persist completed searches (arrays of actor IDs + names) to browser `localStorage`.
- [ ] Build a sleek, slide-out side panel (offcanvas drawer) to display "Recent Searches".
- [ ] Clicking a history item instantly loads that actor combination (and updates the URL).

## 3. Interactive Results Table
- [x] Upgrade the current static movie list into a robust, interactive Data Table.
- [x] **Columns:** Poster, Title, Year, Genres, IMDb Rating. (Poster pending TMDB)
- [x] **Features:** Click headers to sort (e.g., chronologically by Year, or by highest Rating). Add a text input to instantly filter results by Genre or Title.
- [x] *Note:* Requires adding `title.ratings.tsv.gz` to the `generate_db.py` pipeline.

## 4. Dynamic Movie Posters (TMDB API)
- [ ] Hook into the free TMDB (The Movie Database) client-side API.
- [ ] Dynamically fetch and display official movie posters in the results table.
- [ ] *Note:* IMDb's non-commercial dataset legally excludes images, so TMDB is required for this visual upgrade.

## 5. TV Shows & Miniseries Support
- [ ] Currently, the database is strictly filtered to `titleType = 'movie'`.
- [ ] Add support for `tvSeries` and `tvMiniSeries` in the DuckDB generation script.
- [ ] Add a UI toggle switch to let users filter between Movies and TV Shows.

## 6. Director & Writer Support
- [x] Expand the search beyond just actors/actresses to include directors and writers.
- [x] Allows users to search for every collaboration between a specific director and actor (e.g., Christopher Nolan + Cillian Murphy).

## 8. OPFS Cache Invalidation (Hash-Based)
- [x] Add a SHA-256 hash (or similar identifier) for each individual Parquet file in `metadata.json`.
- [x] When the app loads, fetch the live `metadata.json` and compare file hashes against the local OPFS cache.
- [x] Only re-download the specific `.parquet` files whose hashes have changed, and automatically delete old/unused files from OPFS to save space.

## 9. Database Cutoff Timestamp in UI
- [x] Inject a `compiled_at` UNIX timestamp into `metadata.json` during the `generate_db.py` GitHub Action run.
- [x] Display this explicitly in the UI footer (e.g., "Database last updated: October 1st, 2026. Updates monthly.").
- [x] **Benefit:** Sets clear expectations for users searching for movies that literally came out yesterday.
