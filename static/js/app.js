import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/+esm';

let db, conn;
const selectedActors = new Map(); // nconst -> { name }

const PARQUET_FILES = ['basics.parquet', 'principals.parquet', 'names.parquet'];
const OPFS_DIR = 'clipcast-data';

const els = {
    searchBox: document.getElementById('searchBox'),
    searchSpinner: document.getElementById('searchSpinner'),
    autocompleteList: document.getElementById('autocompleteList'),
    chipsContainer: document.getElementById('chipsContainer'),
    movieResults: document.getElementById('movieResults'),
    statusText: document.getElementById('statusText'),
    offlineBtn: document.getElementById('offlineBtn'),
    offlineBtnText: document.getElementById('offlineBtnText'),
    downloadToast: document.getElementById('downloadToast'),
    toastProgressBar: document.getElementById('toastProgressBar'),
    toastProgressText: document.getElementById('toastProgressText'),
    toastCancelBtn: document.getElementById('toastCancelBtn'),
};

// Safely construct URLs regardless of root or subdirectory hosting
let basePath = window.location.pathname;
if (!basePath.endsWith('/')) basePath = basePath.substring(0, basePath.lastIndexOf('/') + 1);
const getParquetUrl = (filename) => `${window.location.origin}${basePath}static/data/${filename}`;

// --- Source tracking: 'http' or 'opfs' ---
let dataSource = 'http';

function getQueryUrl(filename) {
    if (dataSource === 'opfs') {
        // After registerFileBuffer('clipcast-data/names.parquet', buf),
        // DuckDB references it by that exact registered name — no opfs:// prefix.
        return `${OPFS_DIR}/${filename}`;
    }
    return getParquetUrl(filename);
}

// --- DuckDB Init ---
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

        // Check if OPFS cache is already populated
        const opfsReady = await checkOPFS();
        if (opfsReady) {
            await registerOPFSWithDuckDB();
            dataSource = 'opfs';
            els.statusText.innerText = '⚡ Offline Mode — Ready.';
            hideOfflineButton();
        } else {
            els.statusText.innerText = 'Ready. Search for actors.';
            await loadOfflineButtonSize();
        }

        els.searchBox.disabled = false;
        els.searchBox.focus();
    } catch (e) {
        els.statusText.innerText = 'Error loading engine.';
        console.error(e);
    }
}

// --- OPFS Helpers ---
async function checkOPFS() {
    try {
        const root = await navigator.storage.getDirectory();
        const dir = await root.getDirectoryHandle(OPFS_DIR, { create: false });
        for (const f of PARQUET_FILES) {
            await dir.getFileHandle(f, { create: false });
        }
        return true;
    } catch {
        return false;
    }
}

async function loadOfflineButtonSize() {
    try {
        const res = await fetch(getParquetUrl('metadata.json'));
        const meta = await res.json();
        const mb = (meta.total / 1024 / 1024).toFixed(0);
        els.offlineBtnText.innerText = `⚡ Download Offline Engine for Speed (${mb} MB)`;
        els.offlineBtn.style.display = 'inline-flex';
    } catch {
        // metadata.json missing, show button with unknown size
        els.offlineBtnText.innerText = '⚡ Download Offline Engine for Speed';
        els.offlineBtn.style.display = 'inline-flex';
    }
}

function hideOfflineButton() {
    els.offlineBtn.style.display = 'none';
}

// --- Download Logic ---
let downloadAbortController = null;

els.offlineBtn.addEventListener('click', startOfflineDownload);
els.toastCancelBtn.addEventListener('click', cancelOfflineDownload);

async function startOfflineDownload() {
    hideOfflineButton();
    els.downloadToast.classList.add('visible');
    downloadAbortController = new AbortController();

    try {
        const root = await navigator.storage.getDirectory();
        const dir = await root.getDirectoryHandle(OPFS_DIR, { create: true });

        let totalDownloaded = 0;
        let grandTotal = 0;

        // Fetch metadata for accurate progress
        try {
            const meta = await fetch(getParquetUrl('metadata.json')).then(r => r.json());
            grandTotal = meta.total;
        } catch { grandTotal = 0; }

        for (const filename of PARQUET_FILES) {
            if (downloadAbortController.signal.aborted) break;

            const response = await fetch(getParquetUrl(filename), {
                signal: downloadAbortController.signal
            });
            const contentLength = parseInt(response.headers.get('Content-Length') || '0');
            if (grandTotal === 0) grandTotal += contentLength;

            const reader = response.body.getReader();
            const fileHandle = await dir.getFileHandle(filename, { create: true });
            const writable = await fileHandle.createWritable();

            while (true) {
                if (downloadAbortController.signal.aborted) {
                    await writable.close();
                    break;
                }
                const { done, value } = await reader.read();
                if (done) break;
                await writable.write(value);
                totalDownloaded += value.byteLength;
                updateToastProgress(totalDownloaded, grandTotal);
            }
            await writable.close();
        }

        if (!downloadAbortController.signal.aborted) {
            // Register OPFS files with DuckDB
            await registerOPFSWithDuckDB();
            dataSource = 'opfs';
            els.statusText.innerText = '⚡ Offline Mode — Ready.';
            els.downloadToast.classList.remove('visible');
            console.log('Offline mode activated. All searches now run locally.');
        }
    } catch (e) {
        if (e.name !== 'AbortError') {
            console.error('Download failed:', e);
            els.toastProgressText.innerText = 'Download failed. Try again.';
        }
    }
}

async function registerOPFSWithDuckDB() {
    const root = await navigator.storage.getDirectory();
    const dir = await root.getDirectoryHandle(OPFS_DIR, { create: false });
    for (const filename of PARQUET_FILES) {
        const fileHandle = await dir.getFileHandle(filename, { create: false });
        const file = await fileHandle.getFile();
        const buffer = await file.arrayBuffer();
        await db.registerFileBuffer(`${OPFS_DIR}/${filename}`, new Uint8Array(buffer));
    }
}

function cancelOfflineDownload() {
    if (downloadAbortController) {
        downloadAbortController.abort();
    }
    els.downloadToast.classList.remove('visible');
    // Clean up partial OPFS files
    navigator.storage.getDirectory().then(root => {
        root.removeEntry(OPFS_DIR, { recursive: true }).catch(() => {});
    });
    // Restore the button
    loadOfflineButtonSize();
}

function updateToastProgress(downloaded, total) {
    if (total > 0) {
        const pct = Math.min(100, (downloaded / total) * 100);
        els.toastProgressBar.style.width = `${pct.toFixed(1)}%`;
        els.toastProgressText.innerText = `${(downloaded / 1024 / 1024).toFixed(1)} / ${(total / 1024 / 1024).toFixed(0)} MB`;
    } else {
        els.toastProgressText.innerText = `${(downloaded / 1024 / 1024).toFixed(1)} MB downloaded...`;
    }
}

// --- Search ---
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
    debounceTimer = setTimeout(() => searchActors(query), 300);
});

async function searchActors(query) {
    try {
        const isIdSearch = query.toLowerCase().startsWith('nm');
        const safeQuery = query.replace(/'/g, "''");
        let q;
        if (isIdSearch) {
            q = `
                SELECT nconst, primaryName, birthYear, primaryProfession, movieCount, popularityScore 
                FROM read_parquet('${getQueryUrl('names.parquet')}')
                WHERE nconst ILIKE '${safeQuery}%'
                ORDER BY popularityScore DESC, movieCount DESC
                LIMIT 50
            `;
        } else {
            q = `
                SELECT 
                    nconst, primaryName, birthYear, primaryProfession, movieCount, popularityScore,
                    jaro_winkler_similarity(lower(primaryName), lower('${safeQuery}')) AS jw_score,
                    (pow(jaro_winkler_similarity(lower(primaryName), lower('${safeQuery}')), 3) * log10(COALESCE(popularityScore, 0) + 50)) AS final_score
                FROM read_parquet('${getQueryUrl('names.parquet')}')
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
        if (selectedActors.has(row.nconst)) return;
        const div = document.createElement('div');
        div.className = 'autocomplete-item';
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
            if (e.target.closest('.external-link')) return;
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
        els.movieResults.innerHTML = '<div class="empty-state">Add two or more actors above to find their shared movies.</div>';
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
        FROM read_parquet('${getQueryUrl('basics.parquet')}') b
        JOIN (
            SELECT tconst
            FROM read_parquet('${getQueryUrl('principals.parquet')}')
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

initDB();
window.removeActorChip = removeActorChip;
