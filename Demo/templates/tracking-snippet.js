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

    function inferSource(utm, referrer) {
        if (utm.utm_source) {
            return {
                source: utm.utm_source,
                medium: utm.utm_medium || "paid",
                campaign: utm.utm_campaign || "n/a",
                attribution_type: "explicit_utm"
            };
        }
        if (referrer) {
            try {
                const h = new URL(referrer).hostname.toLowerCase();
                if (h.includes("instagram")) return { source: "instagram", medium: "social", campaign: "organic", attribution_type: "referrer" };
                if (h.includes("facebook"))  return { source: "facebook",  medium: "social", campaign: "organic", attribution_type: "referrer" };
                if (h.includes("tiktok"))    return { source: "tiktok",    medium: "social", campaign: "organic", attribution_type: "referrer" };
                if (h.includes("snapchat"))  return { source: "snapchat",  medium: "social", campaign: "organic", attribution_type: "referrer" };
                if (h.includes("google")) return { source: "google", medium: "organic", campaign: "n/a", attribution_type: "referrer_confirmed" };
            } catch {}
        }
        return { source: "direct", medium: "none", campaign: "n/a", attribution_type: "direct_confirmed" };
    }

    function persistFirstTouch(source) {
        if (!localStorage.getItem("first_touch_attribution")) {
            localStorage.setItem("first_touch_attribution", JSON.stringify({
                ...source,
                ts: new Date().toISOString()
            }));
        }
    }

    function getPersistedFirstTouch() {
        try {
            return JSON.parse(localStorage.getItem("first_touch_attribution"));
        } catch {
            return null;
        }
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

    // ------------------- First Touch Identification -------------------
    function identifyFirstTouch() {
        const utm = getUTMParams();
        const referrer = document.referrer || null;
        const landingPath = location.pathname.toLowerCase();

        // Infer source
        const inferred = inferSource(utm, referrer);

        // Persist first-touch if not yet stored
        persistFirstTouch(inferred);

        // Get stored first-touch
        const firstTouch = getPersistedFirstTouch() || {};

        // Return enriched first-touch context
        return {
            ...firstTouch,
            landing_url: location.href,
            landing_path: landingPath,
            is_product_landing: landingPath.includes("/products/"),
            is_collection_landing: landingPath.includes("/collections/"),
            is_homepage: landingPath === "/" || landingPath === ""
        };
    }

    // ------------------- Tracking -------------------
    const BACKEND_URL = "https://testing-within.onrender.com";

    function sendTrackingEvent(type, details = {}) {
        const utm = getUTMParams();
        const referrer = document.referrer || null;

        const inferred = inferSource(utm, referrer);
        const firstTouchContext = identifyFirstTouch();

        const payload = {
            visitor_id: getOrCreateCookie("visitor_id"),
            session_id: getOrCreateSessionId(),
            store_url: location.origin,
            page_url: location.href,
            referrer,

            event_type: type,
            event_details: details,

            utm_params: utm,
            traffic_source: inferred,
            first_touch_context: firstTouchContext,

            client_info: {
                user_agent: navigator.userAgent,
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

    // ------------------- Purchase Interception -------------------
    (function interceptPurchaseEvent() {
        if (typeof window.sendPurchaseEvent !== "function") return;
        const originalSendPurchaseEvent = window.sendPurchaseEvent;

        window.sendPurchaseEvent = function (payload) {
            try {
                const order = payload?.order || (payload && payload.id ? payload : null);

                if (order && order.id) {
                    const orderInfo = {
                        order_id: order.id,
                        customer_id: window.customer?.id || null,
                        customer_name: window.customer?.name || null,
                        customer_email: window.customer?.email || null,
                        customer_mobile: window.customer?.mobile || null,
                        order_total: Number(order.order_total) || null,
                        order_total_string: order.order_total_string || null,
                        currency: order.currency_code || "SAR",
                        issue_date: order.issue_date || null,
                        payment_method_name: order.payment?.method?.name || null,
                        products: Array.isArray(order.products)
                            ? order.products.map(p => ({
                                  product_id: p.id || p.product_id || null,
                                  name: p.name || null,
                                  sku: p.sku || null,
                                  price: Number(p.sale_price ?? p.price) || null,
                                  quantity: Number(p.quantity) || 1
                              }))
                            : [],
                        products_count:
                            order.products_count ||
                            (Array.isArray(order.products) ? order.products.length : 0)
                    };

                    sendTrackingEvent("purchase", orderInfo);
                }

            } catch (err) {
                // silently fail
            }

            return originalSendPurchaseEvent.apply(this, arguments);
        };
    })();

})();
