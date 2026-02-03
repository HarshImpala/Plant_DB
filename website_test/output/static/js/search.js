// Plant Encyclopedia Search Functionality

(function() {
    let searchData = [];
    let searchInput = document.getElementById('search-input');
    let searchResults = document.getElementById('search-results');

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

    // Load search data
    async function loadSearchData() {
        try {
            const baseUrl = getBaseUrl();
            const response = await fetch(baseUrl + '/static/js/search-data.json');
            searchData = await response.json();
        } catch (error) {
            console.error('Failed to load search data:', error);
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
                    <span class="scientific">${plant.canonical_name || plant.scientific_name}</span>
                    ${plant.common_name ? `<br><span class="common">${plant.common_name}</span>` : ''}
                </a>
            `;
        }

        searchResults.innerHTML = html;
    }

    // Event handlers
    if (searchInput) {
        searchInput.addEventListener('input', function(e) {
            const query = e.target.value.trim();

            if (query.length < 2) {
                searchResults.classList.remove('active');
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
})();
