/**
 * ScrapIQ frontend: theme persistence, loading overlay, scroll-to-top,
 * animated counters, clipboard (with fallback), and form validation.
 */

(function () {
    var html = document.documentElement;
    var themeToggle = document.getElementById("themeToggle");
    var themeIcon = document.getElementById("themeIcon");
    var scrollBtn = document.getElementById("scrollTopBtn");
    var loading = document.getElementById("globalLoading");
    var scrapeForm = document.getElementById("scrapeForm");

    var THEME_KEY = "scrapiq-theme";

    function safeSetTheme(mode) {
        try {
            localStorage.setItem(THEME_KEY, mode);
        } catch (e) {
            /* storage disabled */
        }
    }

    function applyTheme(mode) {
        var next = mode === "dark" ? "dark" : "light";
        html.setAttribute("data-bs-theme", next);
        if (themeIcon) {
            themeIcon.className = next === "dark" ? "bi bi-sun-fill" : "bi bi-moon-stars";
        }
        safeSetTheme(next);
    }

    function readStoredTheme() {
        try {
            return localStorage.getItem(THEME_KEY);
        } catch (e) {
            return null;
        }
    }

    function initTheme() {
        var stored = readStoredTheme();
        if (stored === "dark" || stored === "light") {
            applyTheme(stored);
            return;
        }
        var prefersDark =
            window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
        applyTheme(prefersDark ? "dark" : "light");
    }

    if (themeToggle) {
        themeToggle.addEventListener("click", function () {
            var current = html.getAttribute("data-bs-theme") === "dark" ? "dark" : "light";
            applyTheme(current === "dark" ? "light" : "dark");
        });
    }

    function toggleScrollBtn() {
        if (!scrollBtn) return;
        if (window.scrollY > 320) {
            scrollBtn.classList.add("show");
        } else {
            scrollBtn.classList.remove("show");
        }
    }

    window.addEventListener("scroll", toggleScrollBtn, { passive: true });
    if (scrollBtn) {
        scrollBtn.addEventListener("click", function () {
            window.scrollTo({ top: 0, behavior: "smooth" });
        });
    }

    function setLoading(on) {
        if (!loading) return;
        loading.classList.toggle("d-none", !on);
        loading.setAttribute("aria-hidden", on ? "false" : "true");
    }

    /* BFCache: hide spinner when user returns via Back/Forward */
    window.addEventListener("pageshow", function (ev) {
        if (ev.persisted) {
            setLoading(false);
        }
    });

    if (scrapeForm) {
        scrapeForm.addEventListener("submit", function (evt) {
            if (!scrapeForm.checkValidity()) {
                evt.preventDefault();
                evt.stopPropagation();
                scrapeForm.classList.add("was-validated");
                return;
            }
            scrapeForm.classList.add("was-validated");
            setLoading(true);
        });
    }

    function initScrapeOptionsUi() {
        var cat = document.getElementById("categorySelect");
        var fashionBox = document.getElementById("fashionExtras");
        if (!cat || !fashionBox) return;
        function syncFashion() {
            if (cat.value === "Fashion") {
                fashionBox.classList.remove("d-none");
            } else {
                fashionBox.classList.add("d-none");
                var cbs = fashionBox.querySelectorAll('input[type="checkbox"]');
                cbs.forEach(function (cb) {
                    cb.checked = false;
                });
            }
        }
        cat.addEventListener("change", syncFashion);
        syncFashion();
    }

    function animateCounters() {
        var counters = document.querySelectorAll(".counter");
        counters.forEach(function (el) {
            var target = parseInt(el.getAttribute("data-target") || "0", 10);
            if (isNaN(target)) target = 0;
            var duration = 900;
            var start = performance.now();

            function frame(now) {
                var t = Math.min(1, (now - start) / duration);
                var eased = 1 - Math.pow(1 - t, 3);
                var value = Math.round(target * eased);
                el.textContent = value.toLocaleString();
                if (t < 1) requestAnimationFrame(frame);
            }
            requestAnimationFrame(frame);
        });
    }

    function copyText(text) {
        if (navigator.clipboard && window.isSecureContext) {
            return navigator.clipboard.writeText(text);
        }
        return new Promise(function (resolve, reject) {
            var ta = document.createElement("textarea");
            ta.value = text;
            ta.setAttribute("readonly", "");
            ta.setAttribute("aria-hidden", "true");
            ta.style.position = "fixed";
            ta.style.left = "-9999px";
            ta.style.top = "0";
            document.body.appendChild(ta);
            ta.focus();
            ta.select();
            try {
                var ok = document.execCommand("copy");
                document.body.removeChild(ta);
                if (ok) resolve();
                else reject(new Error("copy"));
            } catch (err) {
                try {
                    document.body.removeChild(ta);
                } catch (e2) {
                    /* ignore */
                }
                reject(err);
            }
        });
    }

    function setupCopyAll() {
        var btn = document.getElementById("copyAllBtn");
        if (!btn || typeof window.SCRAPIQ_PLAIN === "undefined") return;

        btn.addEventListener("click", function () {
            var text = window.SCRAPIQ_PLAIN || "";
            var old = btn.innerHTML;

            function success() {
                btn.innerHTML = '<i class="bi bi-check2 me-1" aria-hidden="true"></i>Copied';
                btn.classList.remove("btn-outline-primary");
                btn.classList.add("btn-success");
                setTimeout(function () {
                    btn.innerHTML = old;
                    btn.classList.add("btn-outline-primary");
                    btn.classList.remove("btn-success");
                }, 1600);
            }

            function fail() {
                btn.innerHTML = '<i class="bi bi-clipboard-x me-1" aria-hidden="true"></i>Use Ctrl+C';
                setTimeout(function () {
                    btn.innerHTML = old;
                }, 2200);
            }

            var p = copyText(text);
            if (p && typeof p.then === "function") {
                p.then(success).catch(fail);
            } else {
                fail();
            }
        });
    }

    document.addEventListener("DOMContentLoaded", function () {
        initTheme();
        toggleScrollBtn();
        setupCopyAll();
        initScrapeOptionsUi();
        if (window.__SCRAPIQ_RESULT__) {
            animateCounters();
        }
    });
})();
