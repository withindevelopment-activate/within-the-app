document.addEventListener("DOMContentLoaded", function () {
    const dropdownButtons = document.querySelectorAll(".dropbtn");
    dropdownButtons.forEach(button => {
        button.addEventListener("click", function (e) {
        // Toggle this one
            const dropdownContent = button.nextElementSibling;
            dropdownContent.classList.toggle("show");
        });
    });

    // Close dropdown if clicking outside
    window.addEventListener("click", function (e) {
        if (!e.target.matches('.dropbtn')) {
            document.querySelectorAll(".dropdown-content").forEach(content => {
                content.classList.remove("show");
            });
        }
    });

    document.querySelectorAll('.dropdown').forEach(dropdown => {
    const button = dropdown.querySelector('.dropbtn');
    const links = dropdown.querySelectorAll('.dropdown-content a');
    const dropdownButtons = document.querySelectorAll(".dropbtn");
    const currentPath = window.location.pathname;

    let foundActive = false;

    links.forEach(link => {
      const linkPath = new URL(link.href).pathname;

      if (linkPath === currentPath) {
        link.classList.add('active');
        const dropdownContent = button.nextElementSibling;
        dropdownContent.classList.add("show");
        foundActive = true;
      } else {
        link.classList.remove('active');
      }
    });

    // Toggle "active" class on the button if any link is active
    if (foundActive) {
      button.classList.add('active');
    } else {
      button.classList.remove('active');
    }
  });


  // Load FingerprintJS dynamically
  const fpScript = document.createElement('script');
  fpScript.src = "https://cdn.jsdelivr.net/npm/@fingerprintjs/fingerprintjs@3/dist/fp.min.js";
  fpScript.onload = async () => {
    const TRACKING_ENDPOINT = "https://testing-within.onrender.com/save-tracking/";

    function getOrCreateCookie(name, days = 365) {
      const existing = document.cookie.split('; ').find(row => row.startsWith(name + '='));
      if (existing) return existing.split('=')[1];
      const newId = crypto.randomUUID();
      const expires = new Date(Date.now() + days * 864e5).toUTCString();
      document.cookie = `${name}=${newId}; path=/; expires=${expires}`;
      return newId;
    }

    function getUTMParams() {
      const params = new URLSearchParams(window.location.search);
      return {
        utm_source: params.get("utm_source"),
        utm_medium: params.get("utm_medium"),
        utm_campaign: params.get("utm_campaign"),
        utm_term: params.get("utm_term"),
        utm_content: params.get("utm_content")
      };
    }

    function getClientInfo() {
      return {
        user_agent: navigator.userAgent,
        language: navigator.language,
        timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
        platform: navigator.platform,
        screen_resolution: `${screen.width}x${screen.height}`,
        device_memory: navigator.deviceMemory || null
      };
    }

    function getReferrer() {
      return document.referrer || null;
    }

    // Initialize FingerprintJS and collect tracking data
    const fp = await FingerprintJS.load();
    const result = await fp.get();

    const payload = {
      visitor_id: result.visitorId,
      fingerprint_components: result.components,
      custom_cookie_id: getOrCreateCookie("custom_visitor_id"),
      session_id: getOrCreateCookie("session_id", 1),
      referrer: getReferrer(),
      ...getUTMParams(),
      ...getClientInfo()
    };

    console.log("Tracking payload:", payload);

    fetch(TRACKING_ENDPOINT, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    }).catch(err => console.error("Tracking error:", err));
  };
  document.body.appendChild(fpScript);

});


$(document).ready(function () {
        // $('.table-class').DataTable({
        //     paging: false,
        //     ordering: false,
        //     info: false,
        //     retrieve: true,
        //     destroy: true,
        //     autoWidth: false
        // });
        
        // To allow search in the drop-downs
        $('select.form-select').select2({
        theme: 'bootstrap-5',
        placeholder: 'اختر',
        allowClear: true
        });

    });