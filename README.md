# Clipcast

**Saw actors in a YouTube Short or a random clip, but have no idea what movie it is?**

You know the faces. You just don't know the title. Google doesn't help. IMDb's own search can't do it. You end up in mercy of the comments section or a Reddit thread asking strangers.

**Clipcast solves this.** Type the names of the actors you recognise from the clip. It immediately shows every movie they appeared in together. Two actors, three actors, five, the more you add, the shorter and more precise the list gets.

---

## How It Works

1. Start typing an actor's name. A fuzzy search autocomplete appears with their role count and IMDb ID.
2. Click to add them as a chip.
3. Add more actors.
4. The shared movie list updates instantly as you add or remove actors.
5. Every movie title links directly to its IMDb page.

---

## What Makes It Different

Most tools that do actor-based movie search:
- Only support **exactly two** actors.
- Require **perfect spelling**.
- Run on a **backend server** that can go down, rate-limit you, or disappear.

Clipcast supports any number of actors, has typo-tolerant fuzzy search, and runs **100% in your browser** locally, so it never goes down and never tracks you.

---

## Technical Architecture

The entire database engine runs inside the browser using [DuckDB-Wasm](https://duckdb.org/docs/api/wasm/overview). The IMDb dataset is pre-processed into compressed `.parquet` files (67MB total, down from 6.3GB of raw TSVs) using a Python pipeline. DuckDB-Wasm reads these files via HTTP Range Requests, meaning it fetches only the data it needs for each query.

**Search Ranking** uses a composite score:
```
pow(jaro_winkler_similarity(name, query), 3) * log10(popularityScore + 50)
```
The JW score is cubed to aggressively punish loose matches. Popularity is derived only from lead/co-lead billing credits (`ordering <= 4`) to prevent character actors with 1000 background cameos from drowning out legitimate stars.

**CI/CD:** GitHub Actions downloads the raw IMDb `.gz` files, runs the Python pipeline, and deploys to GitHub Pages, all without a single commit to the repository. The site auto-refreshes on every push to `main` and on the 1st of every month.

---

## Local Development

```bash
# Step 1: Download data and generate the Parquet databases
./download_data.sh

# Step 2: Serve the app (DuckDB-Wasm requires HTTP, not file://)
python3 -m http.server 8000
```

Open `http://localhost:8000`.

> **uv users:** `uv run generate_db.py` works directly without setting up a virtual environment.
