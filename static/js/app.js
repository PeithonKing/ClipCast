import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/+esm';

let db, conn;
const selectedActors = new Map(); // nconst -> { name }

const OPFS_DIR = 'clipcast-data';
const SEARCH_INDEX = 'search_index.parquet';
const TMDB_IMG = 'https://image.tmdb.org/t/p';

let basePath = window.location.pathname;
if (!basePath.endsWith('/')) basePath = basePath.substring(0, basePath.lastIndexOf('/') + 1);
const isLocal = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1';
const getParquetUrl = (filename) => isLocal ? `${window.location.origin}${basePath}static/data/${filename}` : `https://clipcast.peithonking.com/static/data/${filename}`;

const ACTOR_DETAILS = getParquetUrl('actor_details.parquet');
const MOVIES = getParquetUrl('movies.parquet');

const els = {
    searchBox: document.getElementById('searchBox'),
    searchSpinner: document.getElementById('searchSpinner'),
    autocompleteList: document.getElementById('autocompleteList'),
    chipsContainer: document.getElementById('chipsContainer'),
    movieResults: document.getElementById('movieResults'),
    statusText: document.getElementById('statusText'),
    filterMovies: document.getElementById('filterMovies'),
    filterTV: document.getElementById('filterTV')
};

// --- URL State Management ---
function updateUrlState() {
    const actors = Array.from(selectedActors.keys()).join(',');
    const url = new URL(window.location);
    if (actors) url.searchParams.set('actors', actors);
    else url.searchParams.delete('actors');
    url.searchParams.set('movies', els.filterMovies.checked);
    url.searchParams.set('tv', els.filterTV.checked);
    window.history.replaceState({}, '', url);
}

async function loadFromUrl() {
    const url = new URL(window.location);
    const moviesParam = url.searchParams.get('movies');
    const tvParam = url.searchParams.get('tv');
    if (moviesParam !== null) els.filterMovies.checked = moviesParam === 'true';
    if (tvParam !== null) els.filterTV.checked = tvParam === 'true';

    const actorsParam = url.searchParams.get('actors');
    if (!actorsParam) return;

    const rawIds = actorsParam.split(',').map(id => id.trim()).filter(id => id.match(/^nm\d+$/i));
    if (rawIds.length === 0) {
        updateUrlState();
        return;
    }

    try {
        const placeholders = rawIds.map(id => `'${id}'`).join(', ');
        const q = `SELECT nconst, primaryName FROM read_parquet('${OPFS_DIR}/${SEARCH_INDEX}') WHERE nconst IN (${placeholders})`;
        const result = await conn.query(q);
        const validActors = result.toArray().map(r => r.toJSON());
        
        validActors.forEach(row => {
            selectedActors.set(row.nconst, { name: row.primaryName });
        });

        updateUrlState();
        renderChips();
        if (selectedActors.size > 0) runIntersectionQuery();
    } catch (e) {
        console.error("Failed to load actors from URL", e);
    }
}

function initFilters() {
    const updateFilters = (changedEl, otherEl) => {
        if (!els.filterMovies.checked && !els.filterTV.checked) otherEl.checked = true;
        updateUrlState();
        if (selectedActors.size > 0) runIntersectionQuery();
    };
    els.filterMovies.addEventListener('change', () => updateFilters(els.filterMovies, els.filterTV));
    els.filterTV.addEventListener('change', () => updateFilters(els.filterTV, els.filterMovies));
}

// --- OPFS Silent Cache ---
async function syncSearchIndex() {
    let remoteMeta = null;
    try {
        const res = await fetch(getParquetUrl('metadata.json'));
        remoteMeta = await res.json();
    } catch (e) {}

    const root = await navigator.storage.getDirectory();
    const dir = await root.getDirectoryHandle(OPFS_DIR, { create: true });
    
    let needsDownload = true;
    const localMetaStr = localStorage.getItem('clipcast_metadata');
    
    if (localMetaStr && remoteMeta) {
        try {
            const localMeta = JSON.parse(localMetaStr);
            const ageSeconds = (Date.now() / 1000) - localMeta.compiled_at;
            if (ageSeconds < 2592000 && localMeta.files[SEARCH_INDEX] && localMeta.files[SEARCH_INDEX].hash === remoteMeta.files[SEARCH_INDEX].hash) {
                await dir.getFileHandle(SEARCH_INDEX, { create: false });
                needsDownload = false;
            }
        } catch (e) {}
    }

    if (needsDownload) {
        els.statusText.innerText = 'Syncing offline search cache...';
        els.searchSpinner.style.display = 'block';
        try {
            console.log(`Downloading new search_index.parquet from server...`);
            const response = await fetch(getParquetUrl(SEARCH_INDEX));
            const fileHandle = await dir.getFileHandle(SEARCH_INDEX, { create: true });
            const writable = await fileHandle.createWritable();
            await response.body.pipeTo(writable);
            if (remoteMeta) localStorage.setItem('clipcast_metadata', JSON.stringify(remoteMeta));
            
            // Cleanup orphans
            for await (const [name, handle] of dir.entries()) {
                if (name !== SEARCH_INDEX) await dir.removeEntry(name).catch(()=>{});
            }
        } catch (e) {
            console.error("Background download failed", e);
            throw e;
        }
    }
    
    const fileHandle = await dir.getFileHandle(SEARCH_INDEX, { create: false });
    const file = await fileHandle.getFile();
    const mbSize = (file.size / 1024 / 1024).toFixed(1);
    if (needsDownload) {
        console.log(`✅ ${mbSize} MB search_index.parquet downloaded and cached in OPFS successfully.`);
    } else {
        console.log(`⚡ ${mbSize} MB search_index.parquet loaded instantly from local OPFS disk.`);
    }
    
    const buffer = await file.arrayBuffer();
    await db.registerFileBuffer(`${OPFS_DIR}/${SEARCH_INDEX}`, new Uint8Array(buffer));
    els.searchSpinner.style.display = 'none';
}

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

        await syncSearchIndex();

        els.statusText.innerText = 'Ready. Search for actors.';
        els.searchBox.disabled = false;
        els.searchBox.focus();
        
        const localMetaStr = localStorage.getItem('clipcast_metadata');
        if (localMetaStr) {
            const unixTs = JSON.parse(localMetaStr).compiled_at;
            const el = document.getElementById('dbTimestamp');
            if (el && unixTs) {
                el.innerText = `Database last updated: ${new Date(unixTs * 1000).toLocaleDateString()}`;
            }
        }

        await loadFromUrl();
    } catch (e) {
        els.statusText.innerText = 'Error loading engine.';
        console.error(e);
    }
}

// --- Search Logic ---
let debounceTimer;
let activeQueryId = 0;
els.searchBox.addEventListener('input', (e) => {
    clearTimeout(debounceTimer);
    els.searchSpinner.style.display = 'none';
    activeQueryId++;
    const query = e.target.value.trim();
    if (query.length < 2) {
        els.autocompleteList.innerHTML = '';
        return;
    }
    debounceTimer = setTimeout(() => searchActors(query, activeQueryId), 700);
});

async function searchActors(query, queryId) {
    if (queryId !== activeQueryId) return;
    els.searchSpinner.style.display = 'block';
    try {
        const isIdSearch = query.toLowerCase().startsWith('nm');
        const safeQuery = query.replace(/'/g, "''");
        let q;
        if (isIdSearch) {
            q = `SELECT nconst, primaryName, popularityScore FROM read_parquet('${OPFS_DIR}/${SEARCH_INDEX}') WHERE nconst ILIKE '${safeQuery}%' ORDER BY popularityScore DESC LIMIT 50`;
        } else {
            q = `SELECT nconst, primaryName, popularityScore, jaro_winkler_similarity(lower(primaryName), lower('${safeQuery}')) AS jw_score, (pow(jaro_winkler_similarity(lower(primaryName), lower('${safeQuery}')), 3) * log10(COALESCE(popularityScore, 0) + 50)) AS final_score FROM read_parquet('${OPFS_DIR}/${SEARCH_INDEX}') ORDER BY final_score DESC LIMIT 50`;
        }
        const result = await conn.query(q);
        if (queryId !== activeQueryId) return;
        if (els.searchBox.value.trim().length < 2) return;
        const rows = result.toArray().map(r => r.toJSON());
        renderAutocomplete(rows);
    } catch (e) {
        console.error("Search error:", e);
    } finally {
        if (queryId === activeQueryId) els.searchSpinner.style.display = 'none';
    }
}

function renderAutocomplete(rows) {
    els.autocompleteList.innerHTML = '';
    const visibleRows = rows.filter(row => !selectedActors.has(row.nconst));
    visibleRows.forEach(row => {
        const div = document.createElement('div');
        div.className = 'autocomplete-item';
        div.innerHTML = `
            <div class="actor-avatar-placeholder" id="avatar-${row.nconst}">
                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>
            </div>
            <div class="actor-info">
                <div class="actor-header">
                    <span class="actor-name">${row.primaryName}</span>
                    <span class="actor-id">${row.nconst.toLowerCase()}</span>
                </div>
                <div class="actor-prof">
                    <span class="prof-list" id="prof-${row.nconst}">...</span>
                </div>
            </div>
            <a href="https://www.imdb.com/name/${row.nconst}/" target="_blank" class="external-link" title="Open IMDb Profile">
                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path><polyline points="15 3 21 3 21 9"></polyline><line x1="10" y1="14" x2="21" y2="3"></line>
                </svg>
            </a>
        `;
        div.onclick = (e) => {
            if (e.target.closest('.external-link')) return;
            addActorChip(row.nconst, row.primaryName);
            els.searchBox.value = '';
            els.autocompleteList.innerHTML = '';
        };
        els.autocompleteList.appendChild(div);
    });

    if (visibleRows.length > 0) fetchActorDetails(visibleRows.map(r => r.nconst));
}

async function fetchActorDetails(nconsts) {
    const placeholders = nconsts.map(id => `'${id}'`).join(', ');
    try {
        const q = `SELECT nconst, primaryProfession, profile_path, movieCount FROM read_parquet('${ACTOR_DETAILS}') WHERE nconst IN (${placeholders})`;
        const result = await conn.query(q);
        const details = result.toArray().map(r => r.toJSON());
        details.forEach(d => {
            const prof = document.getElementById(`prof-${d.nconst}`);
            if (prof) prof.innerHTML = `${(d.primaryProfession || '').replace(/,/g, ', ')} &bull; <b>${d.movieCount} movies</b>`;
            if (d.profile_path) {
                const avatar = document.getElementById(`avatar-${d.nconst}`);
                if (avatar) avatar.innerHTML = `<img src="${TMDB_IMG}/w45${d.profile_path}" loading="lazy" class="actor-photo" alt="" onerror="this.parentElement.innerHTML='<svg width=20 height=20 viewBox=0_0_24_24 fill=none stroke=currentColor stroke-width=1.5><path_d=M20_21v-2a4_4_0_0_0-4-4H8a4_4_0_0_0-4_4v2/><circle_cx=12_cy=7_r=4/></svg>'">`;
            }
        });
    } catch (e) {}
}

function addActorChip(nconst, name) {
    selectedActors.set(nconst, { name });
    updateUrlState();
    renderChips();
    runIntersectionQuery();
}

function removeActorChip(nconst) {
    selectedActors.delete(nconst);
    updateUrlState();
    renderChips();
    if (selectedActors.size > 0) {
        runIntersectionQuery();
    } else {
        els.movieResults.innerHTML = '<div class="empty-state">Add two or more actors above to find their shared movies.</div>';
    }
}

function renderChips() {
    els.chipsContainer.innerHTML = '';
    selectedActors.forEach((data, nconst) => {
        const chip = document.createElement('div');
        chip.className = 'chip';
        chip.innerHTML = `
            <a href="https://www.imdb.com/name/${nconst}/" target="_blank" class="chip-avatar-link" title="Open IMDb Profile">
                <div id="chip-avatar-${nconst}" class="chip-avatar-wrapper">
                    <svg class="chip-avatar" style="padding:16px;" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>
                </div>
                <div class="chip-avatar-overlay">
                    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path><polyline points="15 3 21 3 21 9"></polyline><line x1="10" y1="14" x2="21" y2="3"></line></svg>
                </div>
            </a>
            <div style="display: flex; align-items: center; justify-content: center; gap: 4px;">
                <span style="font-size: 0.95rem; font-weight: 600; line-height: 1.2;">${data.name}</span>
            </div>
            <span class="close" onclick="removeActorChip('${nconst}')">&times;</span>
        `;
        els.chipsContainer.appendChild(chip);
    });
    
    if (selectedActors.size > 0) fetchChipPhotos(Array.from(selectedActors.keys()));
}

async function fetchChipPhotos(nconsts) {
    const placeholders = nconsts.map(id => `'${id}'`).join(', ');
    try {
        const q = `SELECT nconst, profile_path FROM read_parquet('${ACTOR_DETAILS}') WHERE nconst IN (${placeholders})`;
        const result = await conn.query(q);
        const photos = Object.fromEntries(result.toArray().map(r => r.toJSON()).map(r => [r.nconst, r.profile_path]));
        for (const [nconst, profile_path] of Object.entries(photos)) {
            if (profile_path) {
                const wrapper = document.getElementById(`chip-avatar-${nconst}`);
                if (wrapper) wrapper.innerHTML = `<img src="${TMDB_IMG}/w185${profile_path}" class="chip-avatar" alt="">`;
            }
        }
    } catch {}
}

async function runIntersectionQuery() {
    if (selectedActors.size === 0) return;
    els.movieResults.innerHTML = '<div class="loading">Finding shared movies...</div>';
    const actorIds = Array.from(selectedActors.keys());
    const types = [];
    if (els.filterMovies.checked) types.push("'movie'", "'tvMovie'");
    if (els.filterTV.checked) types.push("'tvSeries'", "'tvMiniSeries'");
    const typeFilter = types.length ? types.join(", ") : "''";

    let ctes = [];
    for (let i = 0; i < actorIds.length; i++) {
        ctes.push(`a${i} AS (SELECT string_split(movies, ',') as movies_list FROM read_parquet('${ACTOR_DETAILS}') WHERE nconst = '${actorIds[i]}')`);
    }

    let intersectLogic = 'a0.movies_list';
    for (let i = 1; i < actorIds.length; i++) {
        intersectLogic = `list_intersect(${intersectLogic}, a${i}.movies_list)`;
    }
    
    const crossJoinClause = actorIds.map((_, i) => `a${i}`).join(' CROSS JOIN ');

    const q = `
        WITH ${ctes.join(', ')}
        SELECT 
            b.tconst, b.primaryTitle, b.startYear, b.genres, b.rating, b.titleType, b.poster_path
        FROM read_parquet('${MOVIES}') b
        JOIN (
            SELECT unnest(${intersectLogic}) as shared_tconst
            FROM ${crossJoinClause}
        ) sub ON b.tconst = sub.shared_tconst
        WHERE b.titleType IN (${typeFilter})
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

let currentMovies = [];
let sortCol = 'startYear';
let sortAsc = false;
let filterText = '';

function renderMovies(movies, timeMs) {
    if (movies) currentMovies = movies;
    if (currentMovies.length === 0) {
        els.movieResults.innerHTML = '<div class="no-results">No shared movies found.</div>';
        return;
    }
    if (timeMs) console.log(`Found ${currentMovies.length} movies in ${timeMs.toFixed(0)} ms.`);
    
    let filtered = currentMovies;
    if (filterText) {
        const ft = filterText.toLowerCase();
        filtered = currentMovies.filter(m => (m.primaryTitle && m.primaryTitle.toLowerCase().includes(ft)) || (m.genres && m.genres.toLowerCase().includes(ft)));
    }
    
    filtered.sort((a, b) => {
        let valA = a[sortCol], valB = b[sortCol];
        if (sortCol === 'startYear') { valA = valA ? parseInt(valA) : 0; valB = valB ? parseInt(valB) : 0; }
        else if (sortCol === 'rating') { valA = valA ? parseFloat(valA) : 0; valB = valB ? parseFloat(valB) : 0; }
        else { valA = valA ? valA.toString().toLowerCase() : ''; valB = valB ? valB.toString().toLowerCase() : ''; }
        
        if (valA < valB) return sortAsc ? -1 : 1;
        if (valA > valB) return sortAsc ? 1 : -1;
        return 0;
    });

    const thClass = (col) => sortCol === col ? (sortAsc ? 'sort-asc' : 'sort-desc') : '';

    let html = `
        <div class="table-toolbar">
            <input type="text" id="tableFilter" placeholder="Filter by Title or Genre..." value="${filterText}">
            <div class="result-count">${filtered.length} ${filtered.length === 1 ? 'Movie' : 'Movies'}</div>
        </div>
        <div class="table-responsive">
            <table class="movie-table">
                <thead>
                    <tr>
                        <th class="poster-col"></th>
                        <th onclick="sortTable('primaryTitle')" class="${thClass('primaryTitle')}">Title</th>
                        <th onclick="sortTable('startYear')" class="${thClass('startYear')}">Year</th>
                        <th onclick="sortTable('genres')" class="${thClass('genres')}">Genres</th>
                        <th onclick="sortTable('rating')" class="${thClass('rating')}">Rating</th>
                    </tr>
                </thead>
                <tbody>
    `;
    
    if (filtered.length === 0) {
        html += `<tr><td colspan="5" class="no-results">No matches for filter.</td></tr>`;
    } else {
        filtered.forEach(m => {
            const ratingStr = m.rating ? `<svg width="14" height="14" viewBox="0 0 24 24" fill="#fbbf24" style="vertical-align: -2px; margin-right: 4px;"><path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/></svg>${m.rating.toFixed(1)}` : '-';
            const posterHtml = m.poster_path ? `<img src="${TMDB_IMG}/w92${m.poster_path}" loading="lazy" class="movie-poster-thumb" alt="">` : '<div class="poster-placeholder"></div>';
            html += `
                <tr>
                    <td class="poster-cell" data-tconst="${m.tconst}">${posterHtml}</td>
                    <td>
                        <a href="https://www.imdb.com/title/${m.tconst}/" target="_blank" class="table-title-link">${m.primaryTitle}</a>
                        ${m.titleType && m.titleType.includes('tv') ? '<span class="tv-badge">TV</span>' : ''}
                    </td>
                    <td>${m.startYear || '-'}</td>
                    <td><span class="genre-tag">${(m.genres || '-').replace(/,/g, ', ')}</span></td>
                    <td class="rating-cell">${ratingStr}</td>
                </tr>
            `;
        });
    }
    
    html += `</tbody></table></div>`;
    els.movieResults.innerHTML = html;
    
    const filterInput = document.getElementById('tableFilter');
    if (filterInput) {
        filterInput.addEventListener('input', (e) => {
            filterText = e.target.value;
            renderMovies();
            const newFilter = document.getElementById('tableFilter');
            if (newFilter) {
                newFilter.focus();
                newFilter.setSelectionRange(filterText.length, filterText.length);
            }
        });
    }
}

window.sortTable = function(col) {
    if (sortCol === col) sortAsc = !sortAsc;
    else { sortCol = col; sortAsc = (col === 'primaryTitle'); }
    renderMovies();
};

initFilters();
initDB();
window.removeActorChip = removeActorChip;
