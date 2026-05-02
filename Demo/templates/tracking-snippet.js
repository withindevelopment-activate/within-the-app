// (function () {
//     var fpScript = document.createElement("script");
//     fpScript.src = "https://cdn.jsdelivr.net/npm/@fingerprintjs/fingerprintjs@3/dist/fp.min.js";
//     fpScript.async = true;
//     document.head.appendChild(fpScript);
// })();

(function () {
    // ------------------- Helpers -------------------

    function getAllCookies() {
        return document.cookie.split("; ").reduce((acc, c) => {
            const [k, ...rest] = c.split("=");
            acc[k] = decodeURIComponent(rest.join("="));
            return acc;
        }, {});
    }

    function parseGAClientId(ga) {
        if (!ga) return null;
        const parts = ga.split(".");
        if (parts.length >= 4) {
            return parts[2] + "." + parts[3];
        }
        return null;
    }

    function parseFBP(fbp) {
        if (!fbp) return null;
        const parts = fbp.split(".");
        return parts.length >= 4 ? parts[3] : null;
    }

    function parseTikTok(tt) {
        if (!tt) return null;
        return tt;
    }

    function parsePosthog(cookie) {
        try {
            return JSON.parse(cookie);
        } catch {
            return null;
        }
    }

    function parseJSONCookie(cookie) {
        try {
            return JSON.parse(decodeURIComponent(cookie));
        } catch {
            return null;
        }
    }

    function extractAnalyticsCookies() {
        const cookies = getAllCookies();

        // Define the list of keys we are already handling explicitly
        const specifiedKeys = [
            "visitor_id", "custom_visitor_id", "ajs_anonymous_id",
            "_ga", "_fbp", "_fbc", "_ttp", "_scid",
            "ttcsid", "_sctr", "_mz_utm","track_utms"
        ];

        const posthogKey = Object.keys(cookies).find(k =>
            k.startsWith("ph_") && k.endsWith("_posthog")
        );
        
        if (posthogKey) specifiedKeys.push(posthogKey);

        // Filter the cookies to find the "leftovers"
        const otherCookies = {};
        Object.keys(cookies).forEach(key => {
            if (!specifiedKeys.includes(key)) {
                otherCookies[key] = cookies[key];
            }
        });

        const posthog = posthogKey ? parsePosthog(cookies[posthogKey]) : null;

        const mzutm = cookies["_mz_utm"]
            ? parseJSONCookie(cookies["_mz_utm"])
            : null;
        
        const trackutms = cookies["track_utms"]
            ? parseJSONCookie(cookies["track_utms"])
            : null;

        return {
            visitor_ids: {
                visitor_id: cookies["visitor_id"] || null,
                custom_visitor_id: cookies["custom_visitor_id"] || null,
                segment_anonymous_id: cookies["ajs_anonymous_id"] || null
            },

            track_utms: {
                utm_source: trackutms?.utm_source || null,
                utm_medium: trackutms?.utm_medium || null,
                utm_campaign: trackutms?.utm_campaign || null,
                utm_term: trackutms?.utm_term || null,
                utm_content: trackutms?.utm_content || null,
            },

            analytics_ids: {
                ga_client_id: parseGAClientId(cookies["_ga"]),
                fb_browser_id: parseFBP(cookies["_fbp"]),
                fb_click_id: cookies["_fbc"] || null,
                tiktok_browser_id: parseTikTok(cookies["_ttp"]),
                snapchat_browser_id: cookies["_scid"] || null
            },

            session_ids: {
                tiktok_session: cookies["ttcsid"] || null,
                session_timestamp: cookies["_sctr"] || null
            },

            attribution: {
                mz_utm: mzutm,
            },

            posthog_identity: {
                device_id: posthog?.$device_id || null,
                distinct_id: posthog?.distinct_id || null
            },

            // Any cookie not specified above ends up here
            other: otherCookies
        };
    }

    // ------------------- Secure Platform Identity Engine -------------------
    function detectPlatform() {
        const ua = navigator.userAgent.toLowerCase();
        const params = new URLSearchParams(window.location.search);

        if (ua.includes("instagram")) return "instagram";
        if (ua.includes("fbav") || ua.includes("fban") || ua.includes("facebook")) return "facebook";
        if (ua.includes("musical_ly") || ua.includes("tiktok")) return "tiktok";
        if (ua.includes("snapchat")) return "snapchat";
        if (params.has("gclid") || ua.includes("youtube")) return "google";
        if (ua.includes("twitter") || ua.includes("x.com")) return "x";
        if (ua.includes("linkedin")) return "linkedin";
        if (ua.includes("pinterest")) return "pinterest";
        if (ua.includes("reddit")) return "reddit";
        if (ua.includes("whatsapp")) return "whatsapp";
        if (ua.includes("telegram")) return "telegram";

        return "web";
    }

    function getPlatformPrefix(platform) {
        const prefixes = {
            meta: "meta_",
            instagram: "insta_",
            facebook: "fb_",
            tiktok: "tiktok_",
            snapchat: "snap_",
            google: "google_",
            x: "x_",
            linkedin: "li_",
            pinterest: "pin_",
            reddit: "rdt_",
            whatsapp: "wa_",
            telegram: "tg_",
            web: "web_"
        };
        return prefixes[platform] || "web_";
    }

    /**
     * Generates a cryptographically strong unique ID.
     * Falls back to a high-entropy random string if crypto.randomUUID is unavailable.
     */
    function generateSecureID(prefix) {
        if (typeof crypto !== 'undefined' && crypto.randomUUID) {
            return prefix + crypto.randomUUID();
        }
        // Fallback for older browsers
        const timestamp = Date.now().toString(36);
        const randomBits = Math.random().toString(36).substring(2, 15);
        return `${prefix}${timestamp}-${randomBits}`;
    }

    function getPrefixStrength(prefix) {
        const strength = {
            meta_: 100,
            insta_: 100,
            fb_: 100,
            tiktok_: 100,
            snap_: 100,
            google_: 80,
            x_: 70,
            li_: 60,
            pin_: 55,
            rdt_: 50,
            wa_: 40,
            tg_: 35,
            web_: 10
        };

        return strength[prefix] || 0;
    }

    function extractPrefix(deviceId) {
        if (!deviceId) return null;

        const i = deviceId.indexOf("_");
        if (i === -1) return null;

        return deviceId.substring(0, i + 1);
    }

    function sanitizeDeviceId(rawId) {
        if (!rawId) return null;
        
        // Explicitly catch string versions of booleans or invalid placeholders
        const invalid = ["true", "false", "null", "undefined", "none"];
        if (invalid.includes(String(rawId).toLowerCase())) return null;

        // Ensure it contains an underscore to be considered a valid prefixed ID
        if (typeof rawId === "string" && rawId.includes("_")) {
            return rawId;
        }
        
        return null;
    }

    function resolveDeviceId(localId, incomingId) {
        if (!localId) return incomingId; 
        if (!incomingId) return localId;

        const localPrefix = extractPrefix(localId);
        const incomingPrefix = extractPrefix(incomingId);

        const localStrength = getPrefixStrength(localPrefix);
        const incomingStrength = getPrefixStrength(incomingPrefix);

        if (incomingStrength >= localStrength) return incomingId
    
        return localId;
    }
    let _cachedPlatformIdentity = null;
    let _memorySessionId = null;

    function getOrCreateDeviceIdentity() {
        if (_cachedPlatformIdentity) return _cachedPlatformIdentity;
        // PRIVACY SAFETY: Respect "Do Not Track" and "Global Privacy Control"
        const isPrivacyEnabled = navigator.doNotTrack === "1" || navigator.globalPrivacyControl === true;
        
        if (isPrivacyEnabled) {
            console.info("Identity generation skipped: User has privacy protections enabled.");
            return null; 
        }

        const params = new URLSearchParams(window.location.search);

        let platform = detectPlatform();

        /* 1️⃣ PRIORITY: URL parameter */
        const incomingId = sanitizeDeviceId(params.get("sleecid"));
        const localIdRaw = localStorage.getItem("device_id");
        const localId = sanitizeDeviceId(localIdRaw);

        let deviceId = resolveDeviceId(localId, incomingId);

        if (!deviceId) {
            const prefix = getPlatformPrefix(platform);
            deviceId = generateSecureID(prefix);
        }

        /* Persist */
        try {
            localStorage.setItem("device_id", deviceId);
            localStorage.setItem("device_platform", platform);
        } catch (e) {
            console.warn("LocalStorage unavailable");
        }

        // Initialize Identity Object
        const ids = {
            device_id: deviceId,
            device_platform: platform,
            meta_device_id: null,
            tiktok_device_id: null,
            snapchat_device_id: null,
            google_device_id: null,
        };

        // Map platform-specific IDs for easier tracking integration
        const platformMap = {
            instagram: "meta_device_id",
            facebook: "meta_device_id",
            tiktok: "tiktok_device_id",
            snapchat: "snapchat_device_id",
            google: "google_device_id",
        };

        const specificKey = platformMap[platform];

        if (specificKey) {
            ids[specificKey] = deviceId;
            localStorage.setItem(specificKey, deviceId);
        }

        _cachedPlatformIdentity = ids;
        return ids;
    }

    function appendSleeid(deviceId) {
        const url = new URL(location.href);
        if (!url.searchParams.has("sleecid")) {
            url.searchParams.set("sleecid", deviceId);
            history.replaceState({}, "", url.toString());
        }
    }

    const platformIdentity = getOrCreateDeviceIdentity();
    if (platformIdentity?.device_id) {
        appendSleeid(platformIdentity.device_id);
    }
    // ------------------- Fingerprint Identifiers -------------------
    // let fingerprintPromise = null;

    // function getFingerprint() {
    //     if (fingerprintPromise) return fingerprintPromise;

    //     fingerprintPromise = new Promise((resolve) => {
    //         if (!window.FingerprintJS) {
    //             resolve(null);
    //             return;
    //         }

    //         FingerprintJS.load()
    //             .then(fp => fp.get())
    //             .then(result => {
    //                 resolve({
    //                     visitor_id: result.visitorId,
    //                     confidence: result.confidence?.score || null
    //                 });
    //             })
    //             .catch(() => resolve(null));
    //     });

    //     return fingerprintPromise;
    // }
    // ------------------- Fingerprint End -------------------
    function generateUUID() {
        if (typeof crypto !== "undefined" && crypto.randomUUID) {
            return crypto.randomUUID();
        }
        return Date.now().toString(36) + "-" + Math.random().toString(36).substring(2, 15);
    }

    function getOrCreateCookie(name, days = 365) {
        const existing = document.cookie.split('; ').find(row => row.startsWith(name + '='));
        if (existing) return existing.split('=')[1];
        const id = generateUUID();
        document.cookie = `${name}=${id}; path=/; max-age=${days * 86400}`;
        return id;
    }

    function getOrCreateSessionId() {
        try {
            let sid = sessionStorage.getItem("session_id");
            if (!sid) {
                sid = generateUUID(); // FIX #7: use safe UUID wrapper
                sessionStorage.setItem("session_id", sid);
            }
            _memorySessionId = sid;
            return sid;
        } catch {
            if (!_memorySessionId) {
                _memorySessionId = generateUUID();
            }
            return _memorySessionId;
        }
    }

    function getUTMParams() {
        const up = new URLSearchParams(window.location.search);
        
        // Safely parse referrer search params
        let ur;
        try {
            ur = new URLSearchParams(document.referrer ? new URL(document.referrer).search : '');
        } catch (e) {
            ur = new URLSearchParams(); // Fallback for invalid referrer URLs
        }

        // Logic Fix: Fallback to 'ur' if 'up' is empty or missing utm_source
        const p = up.get("utm_source") ? up : ur;

        const formatValue = (val) => {
            if (!val) return null;
            return val.replace(/\+/g, ' ');
        };

        return {
            utm_source: formatValue(p.get("utm_source")),
            utm_medium: formatValue(p.get("utm_medium")),
            utm_campaign: formatValue(p.get("utm_campaign")),
            utm_term: formatValue(p.get("utm_term")),
            utm_content: formatValue(p.get("utm_content")),
        };
    }

    function setUTMCookie() {
        const params = new URLSearchParams(window.location.search);
        const refParams = new URLSearchParams(document.referrer ? new URL(document.referrer).search : '');
        const utms = ["utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content"];

        // ---------------------------
        // 1. Get incoming UTMs
        // ---------------------------
        let incoming = {};
        utms.forEach(key => {
            const value = params.get(key);
            const refValue = refParams.get(key);
            if (value) {
                incoming[key] = value.toLowerCase();
            } else if (refValue) {
                incoming[key] = refValue.toLowerCase();
            }
        });

        if (!incoming.utm_source) return; // nothing to process

        // ---------------------------
        // 2. Get existing cookie
        // ---------------------------
        const cookies = getAllCookies();

        let existing = null;

        if (cookies["track_utms"]) {
            try {
                existing = JSON.parse(cookies["track_utms"]);
            } catch {
                existing = null;
            }
        }

        const existingSource = existing?.utm_source || null;
        const incomingSource = incoming.utm_source;

        // ---------------------------
        // 3. Apply your logic
        // ---------------------------
        let shouldOverride = false;

        if (!existingSource) {
            shouldOverride = true;
        } else if (existingSource === "google" && incomingSource !== "direct") {
            shouldOverride = true;
        } else if (existingSource !== "google" && incomingSource === "google") {
            shouldOverride = false;
        } else if (existingSource !== "google" && incomingSource !== "google") {
            shouldOverride = true;
        }

        if (!shouldOverride) return;

        // ---------------------------
        // 4. Save cookie
        // ---------------------------
        const d = new Date();
        d.setTime(d.getTime() + (30 * 24 * 60 * 60 * 1000));

        document.cookie = `track_utms=${JSON.stringify(incoming)};expires=${d.toUTCString()};path=/;SameSite=Lax`;
    }

    // Run this on every page load
    setUTMCookie();

    function getUTMFromCookie() {
        const cookies = getAllCookies();
        let savedData = null;

        if (cookies["track_utms"]) {
            try {
                savedData = JSON.parse(cookies["track_utms"]);
            } catch {
                savedData = null;
            }
        }

        const formatValue = (val) => {
            if (!val || typeof val !== 'string') return null;
            return val.replace(/\+/g, ' ');
        };

        return {
            utm_source: formatValue(savedData?.utm_source),
            utm_medium: formatValue(savedData?.utm_medium),
            utm_campaign: formatValue(savedData?.utm_campaign),
            utm_term: formatValue(savedData?.utm_term),
            utm_content: formatValue(savedData?.utm_content),
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
        const urlUtms = getUTMParams();
        const cookieUtms = getUTMFromCookie();
        const utm = urlUtms.utm_source ? urlUtms : cookieUtms;
        const referrer = document.referrer || null;
        const landingPath = location.pathname.toLowerCase();

        // Infer source
        const inferred = inferSource(utm, referrer);

        // Persist first-touch if not yet stored
        persistFirstTouch(inferred);

        // Get stored first-touch
        const firstTouch = getPersistedFirstTouch() || {};

        // --- enrich with referrer intelligence for backend extraction ---
        const refHost = (() => { try { return referrer ? new URL(referrer).hostname.toLowerCase() : null } catch { return null } })();
        const isSocialRef = refHost ? ["instagram","facebook","tiktok","snapchat","twitter","linkedin"].some(s => refHost.includes(s)) : false;
        const isSearchRef = refHost ? ["google","bing","yahoo"].some(s => refHost.includes(s)) : false;

        // Return enriched first-touch context
        return {
            ...firstTouch,
            referrer_url: referrer || "",
            referrer_host: refHost,
            is_social_referrer: isSocialRef,
            is_search_referrer: isSearchRef,

            landing_url: location.href,
            landing_path: landingPath,
            is_product_landing: landingPath.includes("/products/"),
            is_collection_landing: landingPath.includes("/collections/"),
            is_homepage: landingPath === "/" || landingPath === ""
        };
    }

    // ------------------- Tracking -------------------
    const BACKEND_URL = "https://testing-within.onrender.com";
    const _eventQueue = [];
    let _isFlushing = false;

    function flushQueue() {
        if (_isFlushing || _eventQueue.length === 0) return;
        _isFlushing = true;

        const item = _eventQueue[0];

        fetch(`${BACKEND_URL}/save_tracking/`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(item.payload)
        })
        .then(res => {
            console.log("Tracking response:", res.status);
            _eventQueue.shift(); // remove successfully sent item
            _isFlushing = false;
            if (_eventQueue.length > 0) flushQueue(); // continue draining
        })
        .catch(err => {
            console.error("Tracking error:", err);
            item.attempts = (item.attempts || 0) + 1;
            _isFlushing = false;

            if (item.attempts >= MAX_RETRIES) {
                console.warn("Dropping event after", MAX_RETRIES, "failed attempts:", item.payload.event_type);
                _eventQueue.shift();
                if (_eventQueue.length > 0) flushQueue();
                return;
            }

            const delay = Math.pow(2, item.attempts - 1) * 1000;
            setTimeout(flushQueue, delay);
        });
    }

    function debounce(fn, wait) {
        let timer;
        return function (...args) {
            clearTimeout(timer);
            timer = setTimeout(() => fn.apply(this, args), wait);
        };
    }

    function buildPayload(type, details) {
        const urlUtms = getUTMParams();
        const cookieUtms = getUTMFromCookie();
        const utm = urlUtms.utm_source ? urlUtms : cookieUtms;
        const referrer = document.referrer || null;

        const inferred = inferSource(utm, referrer);
        const firstTouchContext = identifyFirstTouch();

        const identity = getOrCreateDeviceIdentity() || {};
        const cookieIntel = extractAnalyticsCookies();

        return {
            visitor_id: getOrCreateCookie("visitor_id"),
            cookie_id: cookieIntel,
            session_id: getOrCreateSessionId(),
            store_url: location.origin,
            page_url: location.href,
            referrer,

            event_type: type,
            event_details: details,

            device_id: identity.device_id,
            sleecid: identity.device_id,
            meta_device_id: identity.meta_device_id,
            tiktok_device_id: identity.tiktok_device_id,
            snapchat_device_id: identity.snapchat_device_id,
            google_device_id: identity.google_device_id,

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
    }

    function sendTrackingEvent(type, details = {}) {
        const payload = buildPayload(type, details);

        _eventQueue.push({ payload, attempts: 0 });
        flushQueue();
    }

    // wrap UI-triggered events in a debounce so rapid button clicks don't
    // send duplicate events or flood the backend within the same user gesture
    const _debouncedAddToCart = debounce((p) => sendTrackingEvent("add_to_cart", p || {}), 300);
    const _debouncedAddToWishlist = debounce((pid) => sendTrackingEvent("add_to_wishlist", { product_id: pid }), 300);


    
    // function sendTrackingEvent(type, details = {}) {
    //     setUTMCookie();
    //     const urlUtms = getUTMParams();
    //     const cookieUtms = getUTMFromCookie();
    //     const utm = urlUtms.utm_source ? urlUtms : cookieUtms;
    //     const referrer = document.referrer || null;

    //     const inferred = inferSource(utm, referrer);
    //     const firstTouchContext = identifyFirstTouch();

    //     // const fingerprint = await getFingerprint(); 

    //     const platformIdentity = getOrCreateDeviceIdentity() || {};
    //     const cookieIntel = extractAnalyticsCookies();

    //     const payload = {
    //         visitor_id: getOrCreateCookie("visitor_id"),
    //         cookie_id: cookieIntel,
    //         session_id: getOrCreateSessionId(),
    //         store_url: location.origin,
    //         page_url: location.href,
    //         referrer,

    //         event_type: type,
    //         event_details: details,

    //         // fingerprint_id: fingerprint?.visitor_id || null,
    //         // fingerprint_confidence: fingerprint?.confidence || null,

    //         device_id: platformIdentity.device_id,
    //         sleecid: platformIdentity.device_id,

    //         meta_device_id: platformIdentity.meta_device_id,
    //         tiktok_device_id: platformIdentity.tiktok_device_id,
    //         snapchat_device_id: platformIdentity.snapchat_device_id,
    //         google_device_id: platformIdentity.google_device_id,

    //         utm_params: utm,
    //         traffic_source: inferred,
    //         first_touch_context: firstTouchContext,

    //         client_info: {
    //             user_agent: navigator.userAgent,
    //             language: navigator.language,
    //             timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    //             platform: navigator.platform,
    //             screen_resolution: `${screen.width}x${screen.height}`,
    //             device_memory: navigator.deviceMemory || null,
    //         },

    //         visitor_info: getVisitorInfo(),
    //         timestamp: new Date().toISOString()
    //     };

    //     fetch(`${BACKEND_URL}/save_tracking/`, {
    //         method: "POST",
    //         headers: { "Content-Type": "application/json" },
    //         body: JSON.stringify(payload)
    //     })
    //     .then(res => {
    //         console.log("Tracking response:", res.status);
    //         return res.text();
    //     })
    //     .then(data => console.log("Response body:", data))
    //     .catch(err => console.error("Tracking error:", err));
    // }


    // ------------------- Base Events -------------------
    window.addEventListener("load", () => sendTrackingEvent("pageview"));
    window.addToCartEvent = p => sendTrackingEvent("add_to_cart", p || {});
    window.addToWishlist = pid => sendTrackingEvent("add_to_wishlist", { product_id: pid });
    window.purchaseEvent = p => sendTrackingEvent("purchase", p || {});
    
    // ------------------- Purchase Interception -------------------
    (function interceptPurchaseEvent() {
        let _originalSendPurchaseEvent = window.sendPurchaseEvent || null;

        function wrappedSendPurchaseEvent(payload) {
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
            } catch {
                // silently fail — never block the original purchase flow
            }

            if (typeof _originalSendPurchaseEvent === "function") {
                return _originalSendPurchaseEvent.apply(this, arguments);
            }
        }

        // FIX #15: define a property setter so that if the host page does:
        //   window.sendPurchaseEvent = function(...) { ... }
        // after this script runs, our setter fires and wraps it automatically
        Object.defineProperty(window, "sendPurchaseEvent", {
            get() { return wrappedSendPurchaseEvent; },
            set(fn) {
                // FIX #15: store the newly assigned function as the original to call through
                _originalSendPurchaseEvent = fn;
            },
            configurable: true
        });
    })();

    (function() {
        const events = [
            { zid: 'zidPurchaseEventTracking', name: 'purchase' },
            { zid: 'zidAddCartEventTracking', name: 'add_to_cart' },
            { zid: 'zidViewContentEventTracking', name: 'view_content' },
            { zid: 'zidInitiateCheckoutEventTracking', name: 'begin_checkout' }
        ];

        function patch() {
            events.forEach(item => {
                const original = window[item.zid];
                
                // Check if it's a function and we haven't patched it yet
                if (typeof original === 'function' && !original._isPatched) {
                    
                    // Create the wrapper
                    const wrapped = function() {
                        console.log(`%c [Success] Intercepted ${item.name}`, "color: #00ff00; font-weight: bold;");
                        
                        // Extract the data (usually the second argument in Zid)
                        const data = arguments[1] || {};

                        if (typeof window.sendTrackingEvent === 'function') {
                            window.sendTrackingEvent(item.name, data);
                        }

                        // Run the original Zid logic
                        return original.apply(this, arguments);
                    };

                    // Mark it so we don't wrap it twice
                    wrapped._isPatched = true;

                    try {
                        // Try simple assignment first
                        window[item.zid] = wrapped;
                    } catch (e) {
                        // If assignment fails because it's read-only, we are at a dead end 
                        // without modifying the source injection.
                        console.warn(`Could not patch ${item.zid}: Property is read-only.`);
                    }
                }
            });
        }

        // Run immediately and then every 1s for 5s to catch late loads
        patch();
        let count = 0;
        const inv = setInterval(() => {
            patch();
            if (++count > 5) clearInterval(inv);
        }, 1000);
    })();

})();
