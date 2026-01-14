(function() {
    // ------------------- Helper Functions -------------------
    function getOrCreateCookie(name, days = 365) {
        const existing = document.cookie.split('; ').find(row => row.startsWith(name + '='));
        if (existing) return existing.split('=')[1];
        const newId = crypto.randomUUID();
        const expires = new Date(Date.now() + days * 864e5).toUTCString();
        document.cookie = `${name}=${newId}; path=/; expires=${expires}`;
        return newId;
    }

    function getOrCreateSessionId() {
        return sessionStorage.getItem('session_id') || (() => {
            const id = crypto.randomUUID();
            sessionStorage.setItem('session_id', id);
            return id;
        })();
    }

    function getUTMParams() {
        const params = new URLSearchParams(window.location.search);
        return {
            utm_source: params.get("utm_source"),
            utm_medium: params.get("utm_medium"),
            utm_campaign: params.get("utm_campaign"),
            utm_term: params.get("utm_term"),
            utm_content: params.get("utm_content"),
        };
    }

    function getReferrer() {
        return document.referrer || null;
    }

    function getClientInfo() {
        return {
            user_agent: navigator.userAgent,
            language: navigator.language,
            timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
            platform: navigator.platform,
            screen_resolution: `${screen.width}x${screen.height}`,
            device_memory: navigator.deviceMemory || null,
        };
    }

    function inferTrafficSource(utmParams, referrer) {
        if (utmParams.utm_source) return { source: utmParams.utm_source, medium: utmParams.utm_medium || 'unknown', campaign: utmParams.utm_campaign || 'unknown' };
        if (referrer) {
            const hostname = new URL(referrer).hostname.toLowerCase();
            const socialDomains = ['facebook.com','instagram.com','twitter.com','linkedin.com','tiktok.com','pinterest.com'];
            const searchDomains = ['google.com','bing.com','yahoo.com','duckduckgo.com'];
            if (socialDomains.some(d => hostname.includes(d))) return { source: 'social', medium: 'referral', campaign: 'organic' };
            if (searchDomains.some(d => hostname.includes(d))) return { source: 'search', medium: 'organic', campaign: 'n/a' };
            return { source: 'referral', medium: 'referral', campaign: 'n/a' };
        }
        return { source: 'direct', medium: 'none', campaign: 'n/a' };
    }

    function getVisitorId() {
        return getOrCreateCookie('visitor_id');
    }

    function getVisitorInfo() {
        if (window.customer && window.customer.id) {
            return { customer_id: window.customer.id, name: window.customer.name || null, email: window.customer.email || null, mobile: window.customer.mobile || null };
        }
        return {};
    }

    // ------------------- Tracking -------------------
    const BACKEND_URL = "https://testing-within.onrender.com";

    function sendTrackingEvent(eventType, eventDetails = {}) {
        try {
            const storeUrl = window.location.origin;
            if (!storeUrl) return;

            const utmParams = getUTMParams();
            const referrer = getReferrer();
            const traffic = inferTrafficSource(utmParams, referrer);

            const payload = {
                visitor_id: getVisitorId(),
                session_id: getOrCreateSessionId(),
                store_url: storeUrl,
                event_type: eventType,
                event_details: eventDetails,
                utm_params: utmParams,
                referrer: referrer,
                traffic_source: traffic,
                client_info: getClientInfo(),
                visitor_info: getVisitorInfo(),
                page_url: window.location.href,
                timestamp: new Date().toISOString()
            };

            fetch(`${BACKEND_URL}/save_tracking/`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload)
            }).catch(() => {});

        } catch (e) {
            // silently fail
        }
    }

    // ------------------- Base Events -------------------
    window.addEventListener("load", function () {
        sendTrackingEvent("pageview");
    });

    window.addToCartEvent = function (productInfo) {
        sendTrackingEvent("add_to_cart", productInfo || {});
    };

    window.addToWishlist = function (productId) {
        sendTrackingEvent("add_to_wishlist", { product_id: productId });
    };

    // Compatibility only — real capture via interceptor
    window.purchaseEvent = function (data) {
        sendTrackingEvent("purchase", data || {});
    };

    // ------------------- Purchase Interception -------------------
    (function interceptPurchaseEvent() {
        if (typeof window.sendPurchaseEvent !== "function") return;

        const originalSendPurchaseEvent = window.sendPurchaseEvent;

        window.sendPurchaseEvent = function (payload) {
            try {
                const order =
                    payload?.order ||
                    (payload && payload.id ? payload : null);

                if (order && order.id) {
                    const orderInfo = {
                        order_id: order.id,
                        customer_id: order.customer?.id || null,
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
