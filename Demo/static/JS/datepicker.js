// Date Range Picker Logic
(function () {
    function startOfDay(d) { const x = new Date(d); x.setHours(0,0,0,0); return x; }
    function addDays(d, n) { const x = new Date(d); x.setDate(x.getDate() + n); return x; }
    function addMonths(d, n) { const x = new Date(d); x.setMonth(x.getMonth() + n); return x; }
    function startOfMonth(d) { return new Date(d.getFullYear(), d.getMonth(), 1); }
    function endOfMonth(d) { return new Date(d.getFullYear(), d.getMonth() + 1, 0); }
    function sameDay(a, b) { return a && b && a.toDateString() === b.toDateString(); }
    function fmt(d) { return d ? d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" }) : ""; }
    function fmtInput(d) {
    if (!d) return "";
    const y = d.getFullYear(), m = String(d.getMonth()+1).padStart(2,"0"), day = String(d.getDate()).padStart(2,"0");
    return `${y}-${m}-${day}`;
    }
    function monthLabel(d) { return d.toLocaleDateString("en-US", { month: "long", year: "numeric" }); }
    const WEEKDAYS = ["S","M","T","W","T","F","S"];

    const today = startOfDay(new Date());
    const MIN_DATE = null;
    const MAX_DATE = new Date();

    const PRESETS = [
    { label: "Today", range: () => [today, today] },
    { label: "Yesterday", range: () => [addDays(today,-1), addDays(today,-1)] },
    { label: "Last 7 days", range: () => [addDays(today,-6), addDays(today,-1)] },
    { label: "Last 14 days", range: () => [addDays(today,-13), addDays(today,-1)] },
    { label: "Last 30 days", range: () => [addDays(today,-29), addDays(today,-1)] },
    { label: "Last 90 days", range: () => [addDays(today,-89), addDays(today,-1)] },
    { label: "This month", range: () => [startOfMonth(today), today] },
    { label: "Last month", range: () => { const lm = addMonths(today, -1); return [startOfMonth(lm), endOfMonth(lm)]; } },
    ];


    let appliedStart = initialFrom ? new Date(initialFrom + "T00:00:00") : null;
    let appliedEnd = initialTo ? new Date(initialTo + "T00:00:00") : null;

    let draftStart = appliedStart;
    let draftEnd = appliedEnd;
    let activePreset = null;
    let pickingStart = true;
    let leftMonth = startOfMonth(addMonths(appliedEnd || today, -1));

    const wrapper = document.getElementById("drpWrapper");
    const trigger = document.getElementById("drpTrigger");
    const triggerLabel = document.getElementById("drpTriggerLabel");
    const hiddenFrom = document.getElementById("hidden_order_date_from");
    const hiddenTo = document.getElementById("hidden_order_date_to");

    const panelHTML = `
    <div class="drp-panel" id="drpPanel">
        <div class="drp-presets" id="drpPresets"></div>
        <div class="drp-body">
        <div class="drp-inputs">
            <div class="drp-field"><label>From</label><input type="date" id="drpFromInput" /></div>
            <div class="drp-arrow">&#8594;</div>
            <div class="drp-field"><label>To</label><input type="date" id="drpToInput" /></div>
            <div class="drp-daycount" id="drpDayCount"></div>
        </div>
        <div class="drp-nav">
            <button id="drpPrevMonth" type="button" aria-label="Previous month"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="15 18 9 12 15 6"></polyline></svg></button>
            <span>Navigate months</span>
            <button id="drpNextMonth" type="button" aria-label="Next month"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 18 15 12 9 6"></polyline></svg></button>
        </div>
        <div class="drp-months" id="drpMonths">
            <div class="drp-month" id="drpMonthLeft"></div>
            <div class="drp-month" id="drpMonthRight"></div>
        </div>
        <div class="drp-footer">
            <span class="drp-footer-range" id="drpFooterRange"></span>
            <div class="drp-footer-actions">
            <button class="drp-btn drp-btn-cancel" id="drpCancel" type="button">Cancel</button>
            <button class="drp-btn drp-btn-apply" id="drpApply" type="button">Apply</button>
            </div>
        </div>
        </div>
    </div>`;
    document.body.insertAdjacentHTML('beforeend', panelHTML);

    const panel = document.getElementById("drpPanel");
    const presetsEl = document.getElementById("drpPresets");
    const fromInput = document.getElementById("drpFromInput");
    const toInput = document.getElementById("drpToInput");
    const dayCountEl = document.getElementById("drpDayCount");
    const monthLeftEl = document.getElementById("drpMonthLeft");
    const monthRightEl = document.getElementById("drpMonthRight");
    const footerRangeEl = document.getElementById("drpFooterRange");
    const applyBtn = document.getElementById("drpApply");
    const cancelBtn = document.getElementById("drpCancel");
    const prevMonthBtn = document.getElementById("drpPrevMonth");
    const nextMonthBtn = document.getElementById("drpNextMonth");

    function renderTrigger() {
    if (appliedStart && appliedEnd) {
        triggerLabel.textContent = fmt(appliedStart) + " \u2013 " + fmt(appliedEnd);
    } else {
        triggerLabel.textContent = "Select date range";
    }
    }

    function renderPresets() {
    presetsEl.innerHTML = "";
    PRESETS.forEach(p => {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "drp-preset-btn" + (activePreset === p.label ? " active" : "");
        btn.textContent = p.label;
        btn.addEventListener("click", () => applyPreset(p));
        presetsEl.appendChild(btn);
    });
    const customBtn = document.createElement("button");
    customBtn.type = "button";
    customBtn.className = "drp-preset-btn" + (activePreset === null ? " active" : "");
    customBtn.textContent = "Custom";
    customBtn.addEventListener("click", () => { activePreset = null; renderPresets(); });
    presetsEl.appendChild(customBtn);
    }

    function applyPreset(preset) {
    const [s, e] = preset.range();
    draftStart = s; draftEnd = e; activePreset = preset.label; pickingStart = true;
    leftMonth = startOfMonth(addMonths(e, -1));
    renderAll();
    }

    function buildMonthEl(container, monthDate) {
    container.innerHTML = "";
    const title = document.createElement("div");
    title.className = "drp-month-title";
    title.textContent = monthLabel(monthDate);
    container.appendChild(title);
    const weekdaysEl = document.createElement("div");
    weekdaysEl.className = "drp-weekdays";
    WEEKDAYS.forEach(w => { const s = document.createElement("span"); s.textContent = w; weekdaysEl.appendChild(s); });
    container.appendChild(weekdaysEl);
    const daysEl = document.createElement("div");
    daysEl.className = "drp-days";
    const first = startOfMonth(monthDate), last = endOfMonth(monthDate);
    const leadDays = first.getDay(), totalCells = Math.ceil((leadDays + last.getDate()) / 7) * 7;
    for (let i = 0; i < totalCells; i++) {
        const dayNum = i - leadDays + 1;
        const date = new Date(first.getFullYear(), first.getMonth(), dayNum);
        const inMonth = dayNum >= 1 && dayNum <= last.getDate();
        const cell = document.createElement("div");
        cell.className = "drp-day-cell";
        if (inMonth) {
        const isStart = draftStart && sameDay(date, draftStart);
        const isEnd = draftEnd && sameDay(date, draftEnd);
        const isEdge = isStart || isEnd;
        const rangeEndRef = draftEnd || null;
        const within = draftStart && rangeEndRef && date > startOfDay(draftStart) && date < startOfDay(rangeEndRef);
        if (within || isEdge) {
            const bg = document.createElement("div");
            bg.className = "drp-day-range-bg" + (isStart && draftEnd ? " edge-start" : "") + (isEnd && draftStart ? " edge-end" : "");
            if (!(within) && isEdge && draftStart && draftEnd && sameDay(draftStart, draftEnd)) { bg.style.display = "none"; }
            cell.appendChild(bg);
        }
        const disabled = (MIN_DATE && date < startOfDay(MIN_DATE)) || (MAX_DATE && date > startOfDay(MAX_DATE));
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "drp-day-btn" + (isEdge ? " selected" : "") + (sameDay(date, new Date()) ? " today" : "");
        btn.textContent = date.getDate();
        btn.disabled = disabled;
        btn.addEventListener("click", () => handlePick(date));
        cell.appendChild(btn);
        } else {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "drp-day-btn empty";
        btn.disabled = true;
        cell.appendChild(btn);
        }
        daysEl.appendChild(cell);
    }
    container.appendChild(daysEl);
    }

    function renderMonths() {
    const rightMonth = addMonths(leftMonth, 1);
    buildMonthEl(monthLeftEl, leftMonth);
    buildMonthEl(monthRightEl, rightMonth);
    }

    function handlePick(date) {
    activePreset = null;
    if (pickingStart || (draftStart && draftEnd) || date < draftStart) {
        draftStart = date; draftEnd = null; pickingStart = false;
    } else {
        draftEnd = date; pickingStart = true;
    }
    renderAll();
    }

    function handleManualInput(which, value) {
    if (!value) return;
    const parsed = startOfDay(new Date(value + "T00:00:00"));
    activePreset = null;
    if (which === "start") {
        draftStart = parsed;
        if (draftEnd && parsed > draftEnd) draftEnd = null;
        leftMonth = startOfMonth(parsed);
    } else {
        draftEnd = parsed;
        if (draftStart && parsed < draftStart) draftStart = parsed;
        leftMonth = startOfMonth(addMonths(parsed, -1));
    }
    renderAll();
    }

    fromInput.addEventListener("change", (e) => handleManualInput("start", e.target.value));
    toInput.addEventListener("change", (e) => handleManualInput("end", e.target.value));
    prevMonthBtn.addEventListener("click", () => { leftMonth = addMonths(leftMonth, -1); renderMonths(); });
    nextMonthBtn.addEventListener("click", () => { leftMonth = addMonths(leftMonth, 1); renderMonths(); });

    applyBtn.addEventListener("click", () => {
    const s = draftStart;
    const e = draftEnd || draftStart;
    appliedStart = s < e ? s : e;
    appliedEnd = s < e ? e : s;
    hiddenFrom.value = fmtInput(appliedStart);
    hiddenTo.value = fmtInput(appliedEnd);
    renderTrigger();
    closePanel();
    });

    cancelBtn.addEventListener("click", closePanel);

    function renderInputsAndFooter() {
    fromInput.value = fmtInput(draftStart);
    toInput.value = fmtInput(draftEnd);
    fromInput.max = fmtInput(draftEnd || MAX_DATE);
    if (MIN_DATE) fromInput.min = fmtInput(MIN_DATE);
    toInput.min = fmtInput(draftStart);
    toInput.max = fmtInput(MAX_DATE);
    if (draftStart && draftEnd) {
        const days = Math.round((draftEnd - draftStart) / 86400000) + 1;
        dayCountEl.textContent = days + (days > 1 ? " days" : " day");
    } else {
        dayCountEl.textContent = "";
    }
    footerRangeEl.textContent = (draftStart ? fmt(draftStart) : "Start date") + " \u2013 " + (draftEnd ? fmt(draftEnd) : "End date");
    applyBtn.disabled = !draftStart;
    }

    function renderAll() { renderPresets(); renderMonths(); renderInputsAndFooter(); }

    function openPanel() {
    draftStart = appliedStart; draftEnd = appliedEnd;
    leftMonth = startOfMonth(addMonths(appliedEnd || today, -1));
    pickingStart = true;
    renderAll();

    const rect = trigger.getBoundingClientRect();
    panel.style.top = `${rect.bottom + window.scrollY + 8}px`;
    panel.style.left = `${rect.left + window.scrollX}px`;

    panel.classList.add("open");
    }
    function closePanel() { panel.classList.remove("open"); }

    trigger.addEventListener("click", () => { panel.classList.contains("open") ? closePanel() : openPanel(); });
    document.addEventListener("mousedown", (e) => { if (!wrapper.contains(e.target) && !panel.contains(e.target)) closePanel(); });

    renderTrigger();
})();
