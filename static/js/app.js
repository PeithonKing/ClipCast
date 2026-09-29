import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/+esm';

let db, conn;
const selectedActors = new Map(); // nconst -> { name }

const els = {
    searchBox: document.getElementById('searchBox'),
    searchSpinner: document.getElementById('searchSpinner'),
    autocompleteList: document.getElementById('autocompleteList'),
    chipsContainer: document.getElementById('chipsContainer'),
    movieResults: document.getElementById('movieResults'),
    statusText: document.getElementById('statusText')
};

async function initDB() {
    els.statusText.innerText = 'Initializing Engine...';
    try {
        const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
        const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
        
        const worker_url = URL.createObjectURL(
            new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
        );
        const worker = new Worker(worker_url);
        const logger = new duckdb.ConsoleLogger();
        db = new duckdb.AsyncDuckDB(logger, worker);
        await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
        URL.revokeObjectURL(worker_url);
        
        conn = await db.connect();
        els.statusText.innerText = 'Ready. Search for actors.';
        els.searchBox.disabled = false;
        els.searchBox.focus();
    } catch (e) {
        els.statusText.innerText = 'Error loading engine.';
        console.error(e);
    }
}

// Helper to get full URL for duckdb http range requests
// Safely construct URLs regardless of whether hosted at root or in a subdirectory (like /clipcast/)
let basePath = window.location.pathname;
if (!basePath.endsWith('/')) basePath += '/';
const getParquetUrl = (filename) => `${window.location.origin}${basePath}static/data/${filename}`;

let debounceTimer;
els.searchBox.addEventListener('input', (e) => {
    clearTimeout(debounceTimer);
    const query = e.target.value.trim();
    if (query.length < 2) {
        els.autocompleteList.innerHTML = '';
        els.searchSpinner.style.display = 'none';
        return;
    }
    
    els.searchSpinner.style.display = 'block';
    
    debounceTimer = setTimeout(() => {
        searchActors(query);
    }, 300); // 300ms debounce
});

async function searchActors(query) {
    try {
        const isIdSearch = query.toLowerCase().startsWith('nm');
        const safeQuery = query.replace(/'/g, "''");
        
        let q;
        if (isIdSearch) {
            // Search by IMDb ID directly
            q = `
                SELECT nconst, primaryName, birthYear, primaryProfession, movieCount, popularityScore 
                FROM read_parquet('${getParquetUrl('names.parquet')}')
                WHERE nconst ILIKE '${safeQuery}%'
                ORDER BY popularityScore DESC, movieCount DESC
                LIMIT 50
            `;
        } else {
            // Fuzzy search by Name
            q = `
                SELECT 
                    nconst, primaryName, birthYear, primaryProfession, movieCount, popularityScore,
                    jaro_winkler_similarity(lower(primaryName), lower('${safeQuery}')) AS jw_score,
                    (pow(jaro_winkler_similarity(lower(primaryName), lower('${safeQuery}')), 3) * log10(COALESCE(popularityScore, 0) + 50)) AS final_score
                FROM read_parquet('${getParquetUrl('names.parquet')}')
                ORDER BY final_score DESC
                LIMIT 50
            `;
        }
        
        const result = await conn.query(q);
        const rows = result.toArray().map(r => r.toJSON());
        
        renderAutocomplete(rows);
    } catch (e) {
        console.error("Search error:", e);
    } finally {
        els.searchSpinner.style.display = 'none';
    }
}

function renderAutocomplete(rows) {
    els.autocompleteList.innerHTML = '';
    rows.forEach(row => {
        // Skip if already selected
        if (selectedActors.has(row.nconst)) return;
        
        const div = document.createElement('div');
        div.className = 'autocomplete-item';
        
        let debugText = '';
        // if (row.final_score !== undefined) {
        //     debugText = `<div style="font-size: 0.75rem; color: #ff5555; margin-top: 0.2rem; font-family: monospace;">[DEBUG] JW: ${row.jw_score.toFixed(3)} | PopScore: ${row.popularityScore || 0} | Final: ${row.final_score.toFixed(3)}</div>`;
        // }
        
        div.innerHTML = `
            <div class="actor-info">
                <div class="actor-header">
                    <span class="actor-name">${row.primaryName}</span>
                    <span class="movie-badge" title="Total Movies">${row.movieCount}</span>
                    <span class="actor-id">${row.nconst.toLowerCase()}</span>
                </div>
                <div class="actor-prof">
                    <span class="prof-list">${(row.primaryProfession || '').replace(/,/g, ', ')}</span>
                </div>
            </div>
            <a href="https://www.imdb.com/name/${row.nconst}/" target="_blank" class="external-link" title="Open IMDb Profile">
                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path>
                    <polyline points="15 3 21 3 21 9"></polyline>
                    <line x1="10" y1="14" x2="21" y2="3"></line>
                </svg>
            </a>
        `;
        div.onclick = (e) => {
            // If they clicked the external link icon, let the browser open it. Do NOT select the actor.
            if (e.target.closest('.external-link')) {
                return;
            }
            
            addActorChip(row.nconst, row.primaryName);
            els.searchBox.value = '';
            els.autocompleteList.innerHTML = '';
        };
        els.autocompleteList.appendChild(div);
    });
}

function addActorChip(nconst, name) {
    selectedActors.set(nconst, { name });
    renderChips();
    runIntersectionQuery();
}

function removeActorChip(nconst) {
    selectedActors.delete(nconst);
    renderChips();
    if (selectedActors.size > 0) {
        runIntersectionQuery();
    } else {
        els.movieResults.innerHTML = '<div class="empty-state">Search and select actors to find their shared movies.</div>';
    }
}

function renderChips() {
    els.chipsContainer.innerHTML = '';
    selectedActors.forEach((data, nconst) => {
        const chip = document.createElement('div');
        chip.className = 'chip';
        chip.innerHTML = `
            ${data.name}
            <a href="https://www.imdb.com/name/${nconst}/" target="_blank" class="chip-link" title="Open IMDb Profile">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path>
                    <polyline points="15 3 21 3 21 9"></polyline>
                    <line x1="10" y1="14" x2="21" y2="3"></line>
                </svg>
            </a>
            <span class="close" onclick="removeActorChip('${nconst}')">&times;</span>
        `;
        els.chipsContainer.appendChild(chip);
    });
}

async function runIntersectionQuery() {
    if (selectedActors.size === 0) return;
    
    els.movieResults.innerHTML = '<div class="loading">Finding shared movies...</div>';
    
    const actorIds = Array.from(selectedActors.keys());
    const placeholders = actorIds.map(id => `'${id}'`).join(', ');
    
    const q = `
        SELECT 
            b.tconst,
            b.primaryTitle, 
            b.startYear,
            b.genres
        FROM read_parquet('${getParquetUrl('basics.parquet')}') b
        JOIN (
            SELECT tconst
            FROM read_parquet('${getParquetUrl('principals.parquet')}')
            WHERE nconst IN (${placeholders})
            GROUP BY tconst
            HAVING COUNT(DISTINCT nconst) = ${actorIds.length}
        ) p ON b.tconst = p.tconst
        ORDER BY b.startYear DESC
    `;
    
    try {
        const t0 = performance.now();
        const result = await conn.query(q);
        const t1 = performance.now();
        
        const rows = result.toArray().map(r => r.toJSON());
        renderMovies(rows, t1 - t0);
    } catch (e) {
        console.error("Intersection query failed:", e);
        els.movieResults.innerHTML = '<div class="error">Query failed. Check console.</div>';
    }
}

function renderMovies(movies, timeMs) {
    if (movies.length === 0) {
        els.movieResults.innerHTML = '<div class="no-results">No shared movies found.</div>';
        return;
    }
    
    console.log(`Found ${movies.length} movies in ${timeMs.toFixed(0)} ms.`);
    let html = '<ul class="movie-list">';
    movies.forEach(m => {
        html += `
            <li>
                <div class="movie-title">
                    <a href="https://www.imdb.com/title/${m.tconst}/" target="_blank">${m.primaryTitle}</a> 
                    <span class="movie-year">(${m.startYear || 'N/A'})</span>
                </div>
                <div class="movie-genre">${m.genres || ''}</div>
            </li>
        `;
    });
    html += '</ul>';
    
    els.movieResults.innerHTML = html;
}

// Initialize on load
initDB();

// Expose remove chip function globally for the onclick handler in html string
window.removeActorChip = removeActorChip;
