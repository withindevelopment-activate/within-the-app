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
        if (window.customer && window.customer.id) return window.customer.id;
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

    function sendTrackingData(eventType = 'pageview', eventDetails = {}) {
        //const storeUrl = "{{ store_url }}";
        const storeUrl = window.location.origin;
        console.log('IN THE TRACKING SNIPPET FILE THE STORE_URL IS:', storeUrl);
        if (!storeUrl) {
            console.warn('tracking.js: store_url not found in sessionStorage');
            return;
        }
        const utmParams = getUTMParams();
        const referrer = getReferrer();
        const traffic = inferTrafficSource(utmParams, referrer);

        const trackingData = {
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

        fetch(`${BACKEND_URL}/save_tracking`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(trackingData)
        }).catch(err => console.error('Tracking failed:', err));
    }

    // ------------------- Event Listeners -------------------
    function setupListeners() {
        window.addEventListener('load', () => sendTrackingData('pageview'));

        document.addEventListener('click', event => {
            const target = event.target;
            sendTrackingData('click', {
                tag: target.tagName,
                id: target.id || null,
                classes: target.className || null,
                text: target.innerText ? target.innerText.substring(0,50) : null
            });
        });

        document.addEventListener('submit', event => {
            const target = event.target;
            sendTrackingData('form_submit', {
                form_id: target.id || null,
                form_classes: target.className || null,
                action: target.action || null
            });
        });

        let lastScrollPercent = 0;
        window.addEventListener('scroll', () => {
            const scrollTop = document.documentElement.scrollTop || document.body.scrollTop;
            const scrollHeight = document.documentElement.scrollHeight - document.documentElement.clientHeight;
            const scrollPercent = Math.round((scrollTop / scrollHeight) * 100);
            if (scrollPercent - lastScrollPercent >= 25) {
                lastScrollPercent = scrollPercent;
                sendTrackingData('scroll', { percent: scrollPercent });
            }
        });
    }

    setupListeners();
})();
