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

// --- OPFS Helpers & Versioning ---
let remoteMetadata = null;
let filesToDownload = [];
let totalBytesToDownload = 0;

async function fetchMetadata() {
    try {
        const res = await fetch(getParquetUrl('metadata.json'));
        remoteMetadata = await res.json();
    } catch {
        remoteMetadata = null;
    }
}

function updateFooterTimestamp(unixTs, isOffline) {
    const el = document.getElementById('dbTimestamp');
    if (el && unixTs) {
        const date = new Date(unixTs * 1000);
        const prefix = isOffline ? '⚡ Offline Database active' : 'Database last updated';
        el.innerText = `${prefix}: ${date.toLocaleDateString()} (Updates monthly)`;
    }
}

async function checkOPFS() {
    filesToDownload = [];
    totalBytesToDownload = 0;
    
    try {
        const root = await navigator.storage.getDirectory();
        const dir = await root.getDirectoryHandle(OPFS_DIR, { create: false });
        
        const localMetaStr = localStorage.getItem('clipcast_metadata');
        const localMeta = localMetaStr ? JSON.parse(localMetaStr) : null;
        
        if (!remoteMetadata) return false;
        
        let allValid = true;
        for (const f of PARQUET_FILES) {
            try {
                await dir.getFileHandle(f, { create: false });
                if (!localMeta || !localMeta.files || !localMeta.files[f] || localMeta.files[f].hash !== remoteMetadata.files[f].hash) {
                    allValid = false;
                    filesToDownload.push(f);
                    totalBytesToDownload += remoteMetadata.files[f].size;
                }
            } catch {
                allValid = false;
                filesToDownload.push(f);
                totalBytesToDownload += remoteMetadata.files[f].size;
            }
        }
        
        // Cleanup orphaned files
        if (!allValid) {
            for await (const [name, handle] of dir.entries()) {
                if (!PARQUET_FILES.includes(name)) {
                    await dir.removeEntry(name).catch(()=>{});
                }
            }
        }
        
        return allValid;
    } catch {
        if (remoteMetadata) {
            filesToDownload = [...PARQUET_FILES];
            totalBytesToDownload = remoteMetadata.total_bytes || 0;
        }
        return false;
    }
}

// --- DuckDB Init ---
async function initDB() {
    els.statusText.innerText = 'Initializing Engine...';
    try {
        await fetchMetadata();
        
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

        const opfsReady = await checkOPFS();
        if (opfsReady) {
            await registerOPFSWithDuckDB();
            dataSource = 'opfs';
            els.statusText.innerText = '⚡ Offline Mode — Ready.';
            hideOfflineButton();
            const localMetaStr = localStorage.getItem('clipcast_metadata');
            if (localMetaStr) {
                updateFooterTimestamp(JSON.parse(localMetaStr).compiled_at, true);
            }
        } else {
            els.statusText.innerText = 'Ready. Search for actors.';
            if (remoteMetadata && remoteMetadata.compiled_at) {
                updateFooterTimestamp(remoteMetadata.compiled_at, false);
            }
            if (totalBytesToDownload > 0) {
                const mb = (totalBytesToDownload / 1024 / 1024).toFixed(0);
                const isUpdate = localStorage.getItem('clipcast_metadata') !== null;
                els.offlineBtnText.innerText = isUpdate 
                    ? `⚡ Update Offline Engine (${mb} MB)` 
                    : `⚡ Download Offline Engine for Speed (${mb} MB)`;
                els.offlineBtn.style.display = 'inline-flex';
            }
        }

        els.searchBox.disabled = false;
        els.searchBox.focus();
    } catch (e) {
        els.statusText.innerText = 'Error loading engine.';
        console.error(e);
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
        let grandTotal = totalBytesToDownload;

        for (const filename of filesToDownload) {
            if (downloadAbortController.signal.aborted) break;

            const response = await fetch(getParquetUrl(filename), {
                signal: downloadAbortController.signal
            });

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
            if (remoteMetadata) {
                localStorage.setItem('clipcast_metadata', JSON.stringify(remoteMetadata));
            }
            
            // Delete orphaned files again just to be safe
            for await (const [name, handle] of dir.entries()) {
                if (!PARQUET_FILES.includes(name)) {
                    await dir.removeEntry(name).catch(()=>{});
                }
            }
            
            await registerOPFSWithDuckDB();
            dataSource = 'opfs';
            els.statusText.innerText = '⚡ Offline Mode — Ready.';
            els.downloadToast.classList.remove('visible');
            if (remoteMetadata) updateFooterTimestamp(remoteMetadata.compiled_at, true);
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
    // Restore the button if there are files to download
    if (totalBytesToDownload > 0) {
        const mb = (totalBytesToDownload / 1024 / 1024).toFixed(0);
        const isUpdate = localStorage.getItem('clipcast_metadata') !== null;
        els.offlineBtnText.innerText = isUpdate 
            ? `⚡ Update Offline Engine (${mb} MB)` 
            : `⚡ Download Offline Engine for Speed (${mb} MB)`;
        els.offlineBtn.style.display = 'inline-flex';
    }
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
let activeQueryId = 0;
els.searchBox.addEventListener('input', (e) => {
    clearTimeout(debounceTimer);
    els.searchSpinner.style.display = 'none'; // Hide spinner while typing (debouncing)
    activeQueryId++; // Invalidate any running query
    
    const query = e.target.value.trim();
    if (query.length < 2) {
        els.autocompleteList.innerHTML = '';
        return;
    }
    debounceTimer = setTimeout(() => searchActors(query, activeQueryId), 700);
});

async function searchActors(query, queryId) {
    if (queryId !== activeQueryId) return; // Abort if another keystroke happened
    els.searchSpinner.style.display = 'block'; // Show spinner ONLY when request actually fires
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
        if (queryId !== activeQueryId) return;
        if (els.searchBox.value.trim().length < 2) return;
        const rows = result.toArray().map(r => r.toJSON());
        renderAutocomplete(rows);
    } catch (e) {
        console.error("Search error:", e);
    } finally {
        if (queryId === activeQueryId) {
            els.searchSpinner.style.display = 'none';
        }
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
            b.genres,
            b.rating
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
        filtered = currentMovies.filter(m => 
            (m.primaryTitle && m.primaryTitle.toLowerCase().includes(ft)) || 
            (m.genres && m.genres.toLowerCase().includes(ft))
        );
    }
    
    filtered.sort((a, b) => {
        let valA = a[sortCol];
        let valB = b[sortCol];
        
        if (sortCol === 'startYear') {
            valA = valA ? parseInt(valA) : 0;
            valB = valB ? parseInt(valB) : 0;
        } else if (sortCol === 'rating') {
            valA = valA ? parseFloat(valA) : 0;
            valB = valB ? parseFloat(valB) : 0;
        } else {
            valA = valA ? valA.toString().toLowerCase() : '';
            valB = valB ? valB.toString().toLowerCase() : '';
        }
        
        if (valA < valB) return sortAsc ? -1 : 1;
        if (valA > valB) return sortAsc ? 1 : -1;
        return 0;
    });

    const thClass = (col) => {
        if (sortCol === col) return sortAsc ? 'sort-asc' : 'sort-desc';
        return '';
    };

    let html = `
        <div class="table-toolbar">
            <input type="text" id="tableFilter" placeholder="Filter by Title or Genre..." value="${filterText}">
            <div class="result-count">${filtered.length} ${filtered.length === 1 ? 'Movie' : 'Movies'}</div>
        </div>
        <div class="table-responsive">
            <table class="movie-table">
                <thead>
                    <tr>
                        <th onclick="sortTable('primaryTitle')" class="${thClass('primaryTitle')}">Title</th>
                        <th onclick="sortTable('startYear')" class="${thClass('startYear')}">Year</th>
                        <th onclick="sortTable('genres')" class="${thClass('genres')}">Genres</th>
                        <th onclick="sortTable('rating')" class="${thClass('rating')}">Rating</th>
                    </tr>
                </thead>
                <tbody>
    `;
    
    if (filtered.length === 0) {
        html += `<tr><td colspan="4" class="no-results">No matches for filter.</td></tr>`;
    } else {
        filtered.forEach(m => {
            const ratingStr = m.rating ? `<svg width="14" height="14" viewBox="0 0 24 24" fill="#fbbf24" style="vertical-align: -2px; margin-right: 4px;"><path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/></svg>${m.rating.toFixed(1)}` : '-';
            html += `
                <tr>
                    <td>
                        <a href="https://www.imdb.com/title/${m.tconst}/" target="_blank" class="table-title-link">${m.primaryTitle}</a>
                    </td>
                    <td>${m.startYear || '-'}</td>
                    <td><span class="genre-tag">${(m.genres || '-').replace(/,/g, ', ')}</span></td>
                    <td class="rating-cell">${ratingStr}</td>
                </tr>
            `;
        });
    }
    
    html += `
                </tbody>
            </table>
        </div>
    `;
    
    els.movieResults.innerHTML = html;
    
    const filterInput = document.getElementById('tableFilter');
    if (filterInput) {
        filterInput.addEventListener('input', (e) => {
            filterText = e.target.value;
            renderMovies();
            // Restore focus
            const newFilter = document.getElementById('tableFilter');
            if (newFilter) {
                newFilter.focus();
                newFilter.setSelectionRange(filterText.length, filterText.length);
            }
        });
    }
}

window.sortTable = function(col) {
    if (sortCol === col) {
        sortAsc = !sortAsc;
    } else {
        sortCol = col;
        sortAsc = (col === 'primaryTitle'); // default title to asc, others desc
    }
    renderMovies();
};

initDB();
window.removeActorChip = removeActorChip;
