// Plant Encyclopedia Search Functionality

(function() {
    let searchData = [];
    let shardIndex = null;
    const shardCache = {};
    let searchInput = document.getElementById('search-input');
    let searchResults = document.getElementById('search-results');
    let searchToken = 0;

    // Determine base URL from current page
    function getBaseUrl() {
        const path = window.location.pathname;
        const depth = (path.match(/\//g) || []).length;
        // Adjust for different directory depths
        if (path.includes('/plant/') || path.includes('/family/') || path.includes('/genus/')) {
            return '..';
        }
        return '.';
    }

    function getShardKey(query) {
        const first = (query || '').trim().toLowerCase().charAt(0);
        return /[a-z]/.test(first) ? first : '_';
    }

    async function loadShard(baseUrl, key) {
        if (shardCache[key]) {
            return shardCache[key];
        }
        const response = await fetch(baseUrl + '/static/data/search-shard-' + key + '.json');
        if (!response.ok) {
            shardCache[key] = [];
            return shardCache[key];
        }
        shardCache[key] = await response.json();
        return shardCache[key];
    }

    // Load search index or fallback full file.
    async function loadSearchData() {
        try {
            const baseUrl = getBaseUrl();
            const indexResponse = await fetch(baseUrl + '/static/data/search-index.json');
            if (indexResponse.ok) {
                shardIndex = await indexResponse.json();
                return;
            }
            const response = await fetch(baseUrl + '/static/data/search-data.json');
            searchData = await response.json();
            shardCache['*'] = searchData;
        } catch (error) {
            console.error('Failed to load search data:', error);
        }
    }

    async function ensureDataForQuery(query) {
        const baseUrl = getBaseUrl();
        if (!query || query.length < 2) {
            searchData = [];
            return;
        }
        if (shardIndex) {
            const key = getShardKey(query);
            searchData = await loadShard(baseUrl, key);
            return;
        }
        if (shardCache['*']) {
            searchData = shardCache['*'];
            return;
        }
        try {
            const response = await fetch(baseUrl + '/static/data/search-data.json');
            searchData = await response.json();
            shardCache['*'] = searchData;
        } catch (error) {
            console.error('Failed to fetch fallback search data:', error);
            searchData = [];
        }
    }

    // Search function
    function search(query) {
        if (!query || query.length < 2) {
            return [];
        }

        const lowerQuery = query.toLowerCase();
        const results = [];

        for (const plant of searchData) {
            let score = 0;

            // Check canonical name
            if (plant.canonical_name && plant.canonical_name.toLowerCase().includes(lowerQuery)) {
                score += plant.canonical_name.toLowerCase().startsWith(lowerQuery) ? 100 : 50;
            }

            // Check common name
            if (plant.common_name && plant.common_name.toLowerCase().includes(lowerQuery)) {
                score += plant.common_name.toLowerCase().startsWith(lowerQuery) ? 80 : 40;
            }

            // Check all common names
            if (plant.common_names) {
                for (const name of plant.common_names) {
                    if (name.toLowerCase().includes(lowerQuery)) {
                        score += name.toLowerCase().startsWith(lowerQuery) ? 60 : 30;
                        break;
                    }
                }
            }

            // Check synonyms
            if (plant.synonyms) {
                for (const syn of plant.synonyms) {
                    if (syn.toLowerCase().includes(lowerQuery)) {
                        score += 20;
                        break;
                    }
                }
            }

            if (score > 0) {
                results.push({ ...plant, score });
            }
        }

        // Sort by score descending
        results.sort((a, b) => b.score - a.score);

        return results.slice(0, 10);
    }

    // Render search results
    function renderResults(results) {
        const baseUrl = getBaseUrl();

        if (results.length === 0) {
            searchResults.innerHTML = '<div class="no-results" style="padding: 1rem; color: #666;">No plants found</div>';
            return;
        }

        let html = '';
        for (const plant of results) {
            html += `
                <a href="${baseUrl}/plant/${plant.slug}.html">
                    <span class="scientific">${plant.display_name || plant.canonical_name || plant.scientific_name}</span>
                    ${(plant.display_common || plant.common_name) ? `<br><span class="common">${plant.display_common || plant.common_name}</span>` : ''}
                </a>
            `;
        }

        searchResults.innerHTML = html;
    }

    // Event handlers
    if (searchInput) {
        searchInput.addEventListener('input', async function(e) {
            searchToken += 1;
            const currentToken = searchToken;
            const query = e.target.value.trim();

            if (query.length < 2) {
                searchResults.classList.remove('active');
                return;
            }

            await ensureDataForQuery(query);
            if (currentToken !== searchToken) {
                return;
            }
            const results = search(query);
            renderResults(results);
            searchResults.classList.add('active');
        });

        searchInput.addEventListener('focus', function() {
            if (this.value.length >= 2) {
                searchResults.classList.add('active');
            }
        });

        // Close search results when clicking outside
        document.addEventListener('click', function(e) {
            if (!searchInput.contains(e.target) && !searchResults.contains(e.target)) {
                searchResults.classList.remove('active');
            }
        });

        // Keyboard navigation
        searchInput.addEventListener('keydown', function(e) {
            const items = searchResults.querySelectorAll('a');
            const activeItem = searchResults.querySelector('a:focus');
            let index = Array.from(items).indexOf(activeItem);

            if (e.key === 'ArrowDown') {
                e.preventDefault();
                if (index < items.length - 1) {
                    items[index + 1].focus();
                } else if (index === -1 && items.length > 0) {
                    items[0].focus();
                }
            } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                if (index > 0) {
                    items[index - 1].focus();
                } else if (index === 0) {
                    searchInput.focus();
                }
            } else if (e.key === 'Escape') {
                searchResults.classList.remove('active');
            }
        });
    }

    // Load search data on page load
    loadSearchData();

    // Expose for hero search
    window.plantSearch = async function(query) {
        await ensureDataForQuery(query);
        return search(query);
    };
})();
