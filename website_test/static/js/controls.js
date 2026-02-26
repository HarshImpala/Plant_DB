// Theme and Language Controls

(function() {
    // ===== TRANSLATIONS =====
    const translations = {
        en: {
            // Header
            site_title: "Plant Encyclopedia",
            nav_home: "Home",
            nav_az_index: "A-Z Index",
            nav_families: "Families",
            nav_genera: "Genera",
            nav_map: "Map",
            nav_collections: "Collections",
            search_placeholder: "Search plants...",
            switch_language: "Switch language",
            toggle_dark_mode: "Toggle dark mode",
            language_modal_title: "Choose your language",
            language_modal_body: "Select a language to continue.",

            // Homepage
            hero_title: "Plant Encyclopedia",
            hero_description: "Explore our collection of plant species with detailed taxonomic information, distribution data, and more.",
            browse_title: "Browse Plants",
            browse_az_title: "A-Z Index",
            browse_az_desc: "Browse all plants alphabetically by scientific name",
            browse_family_title: "By Family",
            browse_family_desc: "plant families",
            browse_genus_title: "By Genus",
            browse_genus_desc: "genera",
            featured_title: "Featured Plants",

            // Plant page
            about: "About",
            classification: "Classification",
            family: "Family",
            genus: "Genus",
            external_links: "External Links",
            wfo_id: "WFO ID",
            wikipedia_en: "Wikipedia (EN)",
            wikipedia_hu: "Wikipedia (HU)",
            description: "Description",
            common_names: "Common Names",
            native_distribution: "Native Distribution",
            countries: "Countries",
            detailed_regions: "Detailed Regions",
            data_confidence: "Data confidence",
            toxicity_info: "Toxicity Information",
            toxicity_block_title: "Toxicity",
            toxicity_humans: "Humans",
            toxicity_cats_dogs: "Cats/Dogs",
            toxicity_status_toxic: "toxic",
            toxicity_status_not_toxic: "not toxic",
            toxicity_status_unknown: "unknown",
            toxicity_status_family_known_toxic: "family known toxic",
            garden_location: "Botanical Garden Location",
            synonyms: "Synonyms",
            show_synonyms: "Show synonyms",
            related_plants: "Related Plants",
            previous: "Previous",
            next: "Next",
            copy_scientific_name: "Copy scientific name",
            copied: "Copied!",
            image_source: "Image source",
            no_image_available: "No image available",
            machine_translated: "(machine translated)",
            english_fallback: "(English fallback)",
            view_on_map: "View this location on the map page",
            curator_comments: "Curator's Comments",
            toxicity_source: "Source",
            collection_label: "Collection",

            // A-Z Index
            az_title: "A-Z Plant Index",
            az_description: "Browse all plants alphabetically by scientific name.",
            filter_plants: "Filter plants",
            family_filter: "Family",
            genus_filter: "Genus",
            native_region: "Native region",
            all_families: "All families",
            all_genera: "All genera",
            all_regions: "All regions",
            has_toxicity: "Has toxicity info",
            has_image: "Has image",
            has_description: "Has description",
            reset_filters: "Reset",
            plants_shown: "plants shown",

            // Category pages
            families_title: "Plant Families",
            families_description: "Browse plants organized by botanical families.",
            genera_title: "Plant Genera",
            genera_description: "Browse plants organized by genera.",
            plants: "plants",
            plant: "plant",
            sort_by: "Sort by:",
            scientific_name: "Scientific name",
            common_name: "Common name",

            // Collections
            collections_title: "Collections",
            collections_intro: "Browse plants organised by themed collections within the botanical garden.",
            collections_empty: "No collections defined yet. Edit data/collections.json to add collections.",
            collection_count: "plant",
            collection_meta: "plant in this collection",

            // Map
            map_title: "Garden map",
            map_page_title: "Botanical Garden Map",
            map_subtitle: "Fovariosi Allat- es Novenykert - Fuveszkert, 2021",
            map_pdf_pages: "Garden map PDF pages",
            download_map_pdf: "Download Map PDF",
            stations_and_links: "Stations and Collection Links",
            stations_note: "Mapped from the garden station list (1-25). Collection page links are included where applicable.",
            find_by_location: "Find Plants by Garden Location",
            indexed_locations: "Indexed locations",
            mapped_plants: "Plants with mapped location",
            filter_locations: "Filter locations or plant names...",
            pages_label: "Pages",
            fit_width: "Fit Width",
            pdf_viewer_error: "Could not load inline PDF viewer.",
            open_in_new_tab: "Open in new tab",

            // 404
            page_not_found: "Page Not Found",
            page_not_found_lead: "This page seems to have wandered off into the undergrowth.",
            go_home: "Go Home",
            browse_az: "Browse A-Z",
            browse_families: "Browse Families",

            // Footer
            footer_text: "Plant Encyclopedia. Data sources: GBIF, World Flora Online, Wikipedia.",
            collection_statistics: "Collection Statistics",
            content_quality_queue: "Content Quality Queue",
            back_to_top: "Back to top",
            families_label: "Families",
            genera_label: "Genera",

            // Misc
            image_coming_soon: "Image coming soon",
            no_results: "No plants found",
            unknown_plant: "Unknown plant",
            adopt_me: "Adopt me!",
            total_plants: "Total Plants",
            with_photos: "With Photos",
            with_descriptions: "With Descriptions",
            with_distribution_data: "With Distribution Data",
            top_families_by_species: "Top Families by Species Count",
            data_quality_dashboard: "Data Quality Dashboard",
            overall_completeness: "Overall Completeness",
            quality_image: "Image",
            quality_description: "Description",
            quality_distribution: "Distribution",
            quality_wikipedia_url: "Wikipedia URL",
            quality_wfo_link: "WFO Link",
            quality_garden_location: "Garden Location",
            quality_toxicity_info: "Toxicity Info",
            plants_needing_attention: "Plants needing curator attention",
            missing_content_by_plant: "Missing Content by Plant",
            family_label: "Family",
            genus_label: "Genus",
            no_missing_content_items: "No missing-content items found.",
            missing_image: "image",
            missing_description: "description",
            missing_distribution: "distribution",
            missing_wikipedia: "wikipedia",
            missing_toxicity: "toxicity"
        },
        hu: {
            // Header
            site_title: "Növény Enciklopédia",
            nav_home: "Főoldal",
            nav_az_index: "A-Z Index",
            nav_families: "Családok",
            nav_genera: "Nemzetségek",
            nav_map: "Térkép",
            nav_collections: "Gyűjtemények",
            search_placeholder: "Növény keresése...",
            switch_language: "Nyelv váltása",
            toggle_dark_mode: "Sötét mód váltása",
            // Homepage
            hero_title: "Növény Enciklopédia",
            hero_description: "Fedezze fel növénygyűjteményünket részletes taxonómiai információkkal, elterjedési adatokkal és még sok mással.",
            browse_title: "Növények böngészése",
            browse_az_title: "A-Z Index",
            browse_az_desc: "Minden növény ábécé sorrendben tudományos név szerint",
            browse_family_title: "Család szerint",
            browse_family_desc: "növénycsalád",
            browse_genus_title: "Nemzetség szerint",
            browse_genus_desc: "nemzetség",
            featured_title: "Kiemelt növények",
            // Plant page
            about: "Leírás",
            classification: "Besorolás",
            family: "Család",
            genus: "Nemzetség",
            external_links: "Külső linkek",
            wfo_id: "WFO azonosító",
            wikipedia_en: "Wikipédia (EN)",
            wikipedia_hu: "Wikipédia (HU)",
            description: "Leírás",
            common_names: "Köznevek",
            native_distribution: "Természetes elterjedés",
            countries: "Országok",
            detailed_regions: "Részletes régiók",
            data_confidence: "Adat megbízhatóság",
            toxicity_info: "Mérgezési információk",
            toxicity_block_title: "Mérgezés",
            toxicity_humans: "Emberek",
            toxicity_cats_dogs: "Macskák/Kutyák",
            toxicity_status_toxic: "mérgező",
            toxicity_status_not_toxic: "nem mérgező",
            toxicity_status_unknown: "ismeretlen",
            toxicity_status_family_known_toxic: "családi szinten mérgező",
            garden_location: "Botanikus kerti elhelyezkedés",
            synonyms: "Szinonimák",
            show_synonyms: "Szinonimák mutatása",
            related_plants: "Kapcsolódó növények",
            previous: "Előző",
            next: "Következő",
            copy_scientific_name: "Tudományos név másolása",
            copied: "Másolva!",
            image_source: "Kép forrása",
            no_image_available: "Nincs elérhető kép",
            machine_translated: "(gépi fordítás)",
            english_fallback: "(angol tartalék)",
            view_on_map: "Megtekintés a térképen",
            curator_comments: "Kurátori megjegyzések",
            toxicity_source: "Forrás",
            collection_label: "Gyűjtemény",
            // A-Z Index
            az_title: "A-Z Növény Index",
            az_description: "Minden növény ábécé sorrendben tudományos név szerint.",
            filter_plants: "Növények szűrése",
            family_filter: "Család",
            genus_filter: "Nemzetség",
            native_region: "Őshonos régió",
            all_families: "Összes család",
            all_genera: "Összes nemzetség",
            all_regions: "Összes régió",
            has_toxicity: "Van mérgezési adat",
            has_image: "Van kép",
            has_description: "Van leírás",
            reset_filters: "Visszaállítás",
            plants_shown: "növény látható",
            // Category pages
            families_title: "Növénycsaládok",
            families_description: "Növények böngészése botanikai családok szerint.",
            genera_title: "Növény nemzetségek",
            genera_description: "Növények böngészése nemzetségek szerint.",
            plants: "növény",
            plant: "növény",
            sort_by: "Rendezés:",
            scientific_name: "Tudományos név",
            common_name: "Köznév",
            // Collections
            collections_title: "Gyűjtemények",
            collections_intro: "Böngésszen növények között a botanikus kert tematikus gyűjteményei szerint.",
            collections_empty: "Még nincs gyűjtemény megadva. Szerkessze a data/collections.json fájlt a gyűjtemények felvételéhez.",
            collection_count: "növény",
            collection_meta: "növény ebben a gyűjteményben",
            // Map
            map_title: "Kerti térkép",
            map_page_title: "Botanikus kert térképe",
            map_subtitle: "Fővárosi Állat- és Növénykert - Füvészkert, 2021",
            map_pdf_pages: "Kerti térkép PDF oldalak",
            download_map_pdf: "Térkép PDF letöltése",
            stations_and_links: "Állomások és gyűjtemény linkek",
            stations_note: "A kert állomáslistája (1-25) alapján. Ahol van, gyűjteményoldal linkkel.",
            find_by_location: "Növények keresése kerti helyszín szerint",
            indexed_locations: "Indexelt helyszínek",
            mapped_plants: "Térképezett növények",
            filter_locations: "Helyszínek vagy növénynevek szűrése...",
            pages_label: "Oldalak",
            fit_width: "Illesztés szélességre",
            pdf_viewer_error: "Nem sikerült betölteni a beágyazott PDF nézetet.",
            open_in_new_tab: "Megnyitás új lapon",
            // 404
            page_not_found: "Az oldal nem található",
            page_not_found_lead: "Úgy tűnik, ez az oldal eltévedt a bozótban.",
            go_home: "Főoldal",
            browse_az: "Böngészés A-Z",
            browse_families: "Családok böngészése",
            // Footer
            footer_text: "Növény Enciklopédia. Adatforrások: GBIF, World Flora Online, Wikipedia.",
            collection_statistics: "Gyűjtemény statisztika",
            content_quality_queue: "Tartalmi minőségi sor",
            back_to_top: "Vissza a tetejére",
            families_label: "Családok",
            genera_label: "Nemzetségek",
            // Misc
            image_coming_soon: "Kép hamarosan",
            no_results: "Nem található növény",
            unknown_plant: "Ismeretlen növény",
            adopt_me: "Fogadj örökbe!",
            total_plants: "Összes növény",
            with_photos: "Fotóval",
            with_descriptions: "Leírással",
            with_distribution_data: "Elterjedési adattal",
            top_families_by_species: "Legnagyobb családok fajszám szerint",
            data_quality_dashboard: "Adatminőségi áttekintő",
            overall_completeness: "Teljesség összesen",
            quality_image: "Kép",
            quality_description: "Leírás",
            quality_distribution: "Elterjedés",
            quality_wikipedia_url: "Wikipédia URL",
            quality_wfo_link: "WFO hivatkozás",
            quality_garden_location: "Kerti helyszín",
            quality_toxicity_info: "Mérgezési információ",
            plants_needing_attention: "Kurátori figyelmet igénylő növények",
            missing_content_by_plant: "Hiányzó tartalom növényenként",
            family_label: "Család",
            genus_label: "Nemzetség",
            no_missing_content_items: "Nincs hiányzó tartalmú elem.",
            missing_image: "kép",
            missing_description: "leírás",
            missing_distribution: "elterjedés",
            missing_wikipedia: "wikipédia",
            missing_toxicity: "mérgezés",
        }
    };

    // ===== THEME TOGGLE =====
    function initTheme() {
        const savedTheme = localStorage.getItem('theme') || 'dark';
        document.documentElement.setAttribute('data-theme', savedTheme);
        updateThemeButton(savedTheme);
    }

    function toggleTheme() {
        const currentTheme = document.documentElement.getAttribute('data-theme');
        const newTheme = currentTheme === 'light' ? 'dark' : 'light';

        document.documentElement.setAttribute('data-theme', newTheme);
        localStorage.setItem('theme', newTheme);
        updateThemeButton(newTheme);
    }

    function updateThemeButton(theme) {
        const lightIcon = document.querySelector('.light-icon');
        const darkIcon = document.querySelector('.dark-icon');

        if (lightIcon && darkIcon) {
            if (theme === 'dark') {
                lightIcon.style.display = 'none';
                darkIcon.style.display = 'inline';
            } else {
                lightIcon.style.display = 'inline';
                darkIcon.style.display = 'none';
            }
        }
    }

    // ===== LANGUAGE TOGGLE =====
    function initLanguage() {
        const savedLang = localStorage.getItem('lang');
        const lang = savedLang || 'en';
        document.documentElement.setAttribute('data-lang', lang);
        updateLanguageButton(lang);
        applyTranslations(lang);
        if (!savedLang) {
            showLanguageModal();
        }
    }

    function toggleLanguage() {
        const currentLang = document.documentElement.getAttribute('data-lang');
        const newLang = currentLang === 'en' ? 'hu' : 'en';

        document.documentElement.setAttribute('data-lang', newLang);
        localStorage.setItem('lang', newLang);
        updateLanguageButton(newLang);
        applyTranslations(newLang);
    }

    function showLanguageModal() {
        const modal = document.getElementById('language-modal');
        if (!modal) return;
        const focusables = Array.from(modal.querySelectorAll('button[data-lang]'));
        const previousActive = document.activeElement;

        function closeWithLang(lang) {
            document.documentElement.setAttribute('data-lang', lang);
            localStorage.setItem('lang', lang);
            updateLanguageButton(lang);
            applyTranslations(lang);
            modal.hidden = true;
            document.body.classList.remove('modal-open');
            if (previousActive && previousActive.focus) {
                previousActive.focus();
            }
        }

        modal.hidden = false;
        document.body.classList.add('modal-open');
        if (focusables[0]) focusables[0].focus();

        if (modal.dataset.bound) return;
        modal.dataset.bound = '1';

        focusables.forEach(btn => {
            btn.addEventListener('click', function() {
                const selected = btn.getAttribute('data-lang') || 'en';
                closeWithLang(selected);
            });
        });

        modal.addEventListener('keydown', function(e) {
            if (e.key === 'Escape') {
                e.preventDefault();
                const current = document.documentElement.getAttribute('data-lang') || 'en';
                closeWithLang(current);
                return;
            }
            if (e.key !== 'Tab') return;
            const items = focusables;
            if (items.length === 0) return;
            const first = items[0];
            const last = items[items.length - 1];
            if (e.shiftKey && document.activeElement === first) {
                e.preventDefault();
                last.focus();
            } else if (!e.shiftKey && document.activeElement === last) {
                e.preventDefault();
                first.focus();
            }
        });
    }

    function updateLanguageButton(lang) {
        const langLabel = document.querySelector('.lang-label');
        if (langLabel) {
            // Show the OTHER language as the option to switch to
            langLabel.textContent = lang === 'en' ? 'HU' : 'EN';
        }
    }

    function applyTranslations(lang) {
        const trans = translations[lang];
        if (!trans) return;

        // Translate elements with data-translate attribute
        document.querySelectorAll('[data-translate]').forEach(el => {
            const key = el.getAttribute('data-translate');
            if (trans[key]) {
                if (el.tagName === 'INPUT') {
                    el.placeholder = trans[key];
                } else {
                    el.textContent = trans[key];
                }
            }
        });
        // Translate title and aria-label attributes when requested
        document.querySelectorAll('[data-translate-title]').forEach(el => {
            const key = el.getAttribute('data-translate-title');
            if (trans[key]) {
                el.setAttribute('title', trans[key]);
            }
        });
        document.querySelectorAll('[data-translate-aria-label]').forEach(el => {
            const key = el.getAttribute('data-translate-aria-label');
            if (trans[key]) {
                el.setAttribute('aria-label', trans[key]);
            }
        });

        // Update HTML lang attribute
        document.documentElement.lang = lang;

        // Localize "Adopt me" destination by language
        const adoptUrl = lang === 'en'
            ? 'https://www.fuveszkert.org/adoption-program/'
            : 'https://www.fuveszkert.org/orokbefogadas/';
        document.querySelectorAll('.adopt-btn').forEach(el => {
            el.setAttribute('href', adoptUrl);
        });
    }

    // ===== INITIALIZATION =====
    function init() {
        // Initialize theme and language from localStorage
        initTheme();
        initLanguage();

        // Set up event listeners
        const themeToggle = document.getElementById('theme-toggle');
        const langToggle = document.getElementById('lang-toggle');

        if (themeToggle) {
            themeToggle.addEventListener('click', toggleTheme);
        }

        if (langToggle) {
            langToggle.addEventListener('click', toggleLanguage);
        }

        initBackToTop();
        initSearchShortcut();
    }

    // ===== SEARCH SHORTCUT =====
    function initSearchShortcut() {
        document.addEventListener('keydown', function(e) {
            if (e.key === '/' &&
                document.activeElement.tagName !== 'INPUT' &&
                document.activeElement.tagName !== 'TEXTAREA') {
                e.preventDefault();
                var searchInput = document.getElementById('search-input');
                if (searchInput) { searchInput.focus(); searchInput.select(); }
            }
        });
    }

    // ===== BACK TO TOP =====
    function initBackToTop() {
        const btn = document.getElementById('back-to-top');
        if (!btn) return;

        window.addEventListener('scroll', function() {
            btn.classList.toggle('visible', window.scrollY > 300);
        }, { passive: true });

        btn.addEventListener('click', function() {
            window.scrollTo({ top: 0, behavior: 'smooth' });
        });
    }

    // Run on DOM ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    // Expose translation function for dynamic content
    window.getTranslation = function(key) {
        const lang = document.documentElement.getAttribute('data-lang') || 'en';
        return translations[lang][key] || key;
    };
})();
