(function () {
    // ------------------- Helpers -------------------
    function getOrCreateCookie(name, days = 365) {
        const existing = document.cookie.split('; ').find(row => row.startsWith(name + '='));
        if (existing) return existing.split('=')[1];
        const id = crypto.randomUUID();
        document.cookie = `${name}=${id}; path=/; max-age=${days * 86400}`;
        return id;
    }

    function getOrCreateSessionId() {
        let sid = sessionStorage.getItem("session_id");
        if (!sid) {
            sid = crypto.randomUUID();
            sessionStorage.setItem("session_id", sid);
        }
        return sid;
    }

    function getUTMParams() {
        const p = new URLSearchParams(window.location.search);
        return {
            utm_source: p.get("utm_source"),
            utm_medium: p.get("utm_medium"),
            utm_campaign: p.get("utm_campaign"),
            utm_term: p.get("utm_term"),
            utm_content: p.get("utm_content"),
        };
    }

    // ------------------- FIRST TOUCH LOCK -------------------
    function persistFirstTouchContext(context) {
        if (!localStorage.getItem("first_touch_context")) {
            localStorage.setItem("first_touch_context", JSON.stringify({
                ...context,
                ts: new Date().toISOString()
            }));
        }
    }

    function getFirstTouchContext() {
        try {
            return JSON.parse(localStorage.getItem("first_touch_context"));
        } catch {
            return null;
        }
    }

    // ------------------- Referrer Intelligence -------------------
    function getReferrerContext() {
        const ref = document.referrer || null;
        let refHost = null;

        try {
            if (ref) refHost = new URL(ref).hostname.toLowerCase();
        } catch {}

        const ua = navigator.userAgent.toLowerCase();

        return {
            referrer_url: ref,
            referrer_host: refHost,
            is_social_referrer:
                refHost?.includes("instagram") ||
                refHost?.includes("facebook") ||
                refHost?.includes("tiktok") ||
                refHost?.includes("snapchat") ||
                refHost?.includes("twitter") ||
                refHost?.includes("linkedin") ||
                false,

            is_search_referrer:
                refHost?.includes("google") ||
                refHost?.includes("bing") ||
                refHost?.includes("yahoo") ||
                false,

            in_app_browser_hint:
                ua.includes("instagram") ||
                ua.includes("fbav") ||
                ua.includes("tiktok") ||
                ua.includes("snapchat"),

            user_agent: navigator.userAgent
        };
    }

    // ------------------- Landing Context -------------------
    function getLandingContext() {
        const path = location.pathname.toLowerCase();

        return {
            landing_url: location.href,
            landing_path: path,
            is_product_landing: path.includes("/products/"),
            is_collection_landing: path.includes("/collections/"),
            is_homepage: path === "/" || path === ""
        };
    }

    // ------------------- Source Inference + FIRST TOUCH -------------------
    function inferImmediateSource(utm, refCtx) {
        if (utm.utm_source) {
            return {
                source: utm.utm_source,
                medium: utm.utm_medium || "paid",
                campaign: utm.utm_campaign || "n/a",
                attribution_type: "explicit_utm"
            };
        }

        if (refCtx.referrer_host) {
            return {
                source: refCtx.referrer_host,
                medium: refCtx.is_search_referrer
                    ? "organic"
                    : refCtx.is_social_referrer
                    ? "social"
                    : "referral",
                campaign: "n/a",
                attribution_type: "referrer"
            };
        }

        return {
            source: "direct",
            medium: "none",
            campaign: "n/a",
            attribution_type: "direct_unverified"
        };
    }

    function getVisitorInfo() {
        if (window.customer) {
            return {
                Customer_ID: window.customer.id,
                Customer_Name: window.customer.name || null,
                Customer_Email: window.customer.email || null,
                Customer_Mobile: window.customer.mobile || null
            };
        }
        return {};
    }

    // ------------------- Tracking -------------------
    const BACKEND_URL = "https://testing-within.onrender.com";

    function sendTrackingEvent(type, details = {}) {
        const utm = getUTMParams();
        const refCtx = getReferrerContext();
        const landingCtx = getLandingContext();

        persistFirstTouchContext({
            utm,
            referrer: refCtx,
            landing: landingCtx
        });

        const firstTouchContext = getFirstTouchContext();
        const inferred = inferImmediateSource(utm, refCtx);

        const payload = {
            visitor_id: getOrCreateCookie("visitor_id"),
            session_id: getOrCreateSessionId(),

            event_type: type,
            event_details: details,

            page_url: location.href,
            store_url: location.origin,

            utm_params: utm,
            traffic_source: inferred,

            first_touch_context: firstTouchContext,

            client_info: {
                language: navigator.language,
                timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
                platform: navigator.platform,
                screen_resolution: `${screen.width}x${screen.height}`,
                device_memory: navigator.deviceMemory || null,
            },

            visitor_info: getVisitorInfo(),
            timestamp: new Date().toISOString()
        };

        fetch(`${BACKEND_URL}/save_tracking/`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        }).catch(() => {});
    }

    // ------------------- Base Events -------------------
    window.addEventListener("load", () => sendTrackingEvent("pageview"));
    window.addToCartEvent = p => sendTrackingEvent("add_to_cart", p || {});
    window.addToWishlist = pid => sendTrackingEvent("add_to_wishlist", { product_id: pid });
    window.purchaseEvent = p => sendTrackingEvent("purchase", p || {});
})();