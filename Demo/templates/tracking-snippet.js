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
        const storeUrl = window.location.origin;
        if (!storeUrl) {
            console.warn('tracking.js: store_url not found');
            return;
        }
        const utmParams = getUTMParams();
        const referrer = getReferrer();
        const traffic = inferTrafficSource(utmParams, referrer);

        const trackingData = {
            // store_id: STORE_ID,
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
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(trackingData)
        }).catch(err => console.error('Tracking failed:', err));
    }

    // ------------------- Event Listener -------------------
    window.addEventListener('load', function() {
        sendTrackingEvent("pageview");
    });

    window.addToCartEvent = function(productInfo) {
        console.log("🛒 Add to Cart detected:", productInfo);
        sendTrackingEvent("add_to_cart", productInfo);
    };

    window.addToWishlist = function(productId) {
        console.log("🛒 Add to Wishlist detected:", productId);
        sendTrackingEvent("add_to_wishlist", productId);
    };

    // ------------------- Purchase Tracking -------------------
    (function () {
        if (typeof window.sendPurchaseEvent !== "function") return;

        const originalSendPurchaseEvent = window.sendPurchaseEvent;

        window.sendPurchaseEvent = function (payload) {
            try {
            const { order } = payload || {};

            if (order && order.id) {
                const orderInfo = {
                order_id: order.id,
                customer_id: order.customer?.id || "Unknown",
                order_total: order.order_total,
                order_total_string: order.order_total_string,
                currency: order.currency_code || "SAR",
                issue_date: order.issue_date,
                payment_method_name: order.payment?.method?.name,
                products_name: order.products?.map(p => p.name).join(", "),
                products_count: order.products?.length || 0
                };

                console.log("✅ Purchase detected (hooked):", orderInfo);
                sendTrackingEvent("purchase", orderInfo);
            }
            } catch (err) {
            console.warn("Purchase hook failed:", err);
            }

            return originalSendPurchaseEvent.apply(this, arguments);
        };
    })();

    // Run once after page load
    window.addEventListener('load', sendPurchaseIfExists);
})();
