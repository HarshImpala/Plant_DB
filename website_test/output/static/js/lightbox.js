// Plant Encyclopedia - Image Lightbox

(function() {
    var overlay, lightboxImg;

    function buildOverlay() {
        overlay = document.createElement('div');
        overlay.className = 'lightbox-overlay';
        overlay.innerHTML =
            '<div class="lightbox-content">' +
                '<button class="lightbox-close" aria-label="Close">&times;</button>' +
                '<img class="lightbox-img" src="" alt="">' +
            '</div>';
        document.body.appendChild(overlay);

        lightboxImg = overlay.querySelector('.lightbox-img');

        overlay.addEventListener('click', function(e) {
            if (e.target === overlay) close();
        });
        overlay.querySelector('.lightbox-close').addEventListener('click', close);
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape' && overlay.classList.contains('active')) close();
        });
    }

    function open(src, alt) {
        lightboxImg.src = src;
        lightboxImg.alt = alt || '';
        overlay.classList.add('active');
        document.body.style.overflow = 'hidden';
    }

    function close() {
        overlay.classList.remove('active');
        document.body.style.overflow = '';
        lightboxImg.src = '';
    }

    function attachToImages() {
        document.querySelectorAll('.plant-image:not(.placeholder) img').forEach(function(img) {
            img.classList.add('zoomable');
            img.addEventListener('click', function() { open(img.src, img.alt); });
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', function() { buildOverlay(); attachToImages(); });
    } else {
        buildOverlay();
        attachToImages();
    }
})();
