/**
 * WhatsApp Outreach Bot Dashboard - Frontend Controller
 * Handles SSE real-time streaming, queue rendering, Chrome controls,
 * template personalization preview, and campaign management.
 */

document.addEventListener("DOMContentLoaded", () => {
  // Application State
  const state = {
    engineState: "IDLE",
    isReady: false,
    stats: { total: 0, sent: 0, failed: 0, not_on_whatsapp: 0, pending: 0 },
    queue: [],
    currentFilter: "all",
    eventSource: null,
    leadFiles: [],
    selectedLeadFile: null
  };

  // DOM Elements - Navigation & Engine Status
  const engineStatusPill = document.getElementById("engineStatusPill");
  const engineStatusText = document.getElementById("engineStatusText");
  const btnLaunchBrowser = document.getElementById("btnLaunchBrowser");
  const btnCloseBrowser = document.getElementById("btnCloseBrowser");
  const qrAlertBanner = document.getElementById("qrAlertBanner");

  // DOM Elements - Stats
  const statTotal = document.getElementById("statTotal");
  const statSent = document.getElementById("statSent");
  const statRemaining = document.getElementById("statRemaining");
  const statFailed = document.getElementById("statFailed");
  const statFailedSub = document.getElementById("statFailedSub");
  const statProgressPercent = document.getElementById("statProgressPercent");
  const statValidSummary = document.getElementById("statValidSummary");
  const statCooldown = document.getElementById("statCooldown");
  const statCooldownSub = document.getElementById("statCooldownSub");

  // DOM Elements - Source Tabs & Inputs
  const sourceTabs = document.querySelectorAll(".source-tabs .tab-btn");
  const tabPanels = document.querySelectorAll(".tab-panel");
  const selectLeadFile = document.getElementById("selectLeadFile");
  const btnRefreshFiles = document.getElementById("btnRefreshFiles");
  const leadFileInfo = document.getElementById("leadFileInfo");
  const fileCountBadge = document.getElementById("fileCountBadge");
  const fileSizeBadge = document.getElementById("fileSizeBadge");
  const btnLoadSelectedFile = document.getElementById("btnLoadSelectedFile");

  const rawNumbersInput = document.getElementById("rawNumbersInput");
  const btnInsertSample = document.getElementById("btnInsertSample");
  const btnParsePasted = document.getElementById("btnParsePasted");

  const dropzone = document.getElementById("dropzone");
  const csvFileInput = document.getElementById("csvFileInput");
  const btnBrowseFile = document.getElementById("btnBrowseFile");
  const customFilePathInput = document.getElementById("customFilePathInput");
  const btnLoadCustomPath = document.getElementById("btnLoadCustomPath");
  const selectedFileInfo = document.getElementById("selectedFileInfo");
  const selectedFileNameBadge = document.getElementById("selectedFileNameBadge");
  const selectedFileCountBadge = document.getElementById("selectedFileCountBadge");
  const selectedFileStatusBadge = document.getElementById("selectedFileStatusBadge");

  const defaultCountryCode = document.getElementById("defaultCountryCode");
  const dispatchModeSelect = document.getElementById("dispatchModeSelect");

  // DOM Elements - Message Template & Preview
  const messageTemplate = document.getElementById("messageTemplate");
  const charCount = document.getElementById("charCount");
  const varTags = document.querySelectorAll(".var-tag");
  const previewText = document.getElementById("previewText");
  const delayMin = document.getElementById("delayMin");
  const delayMax = document.getElementById("delayMax");

  // DOM Elements - Campaign Action Bar
  const actionPulse = document.getElementById("actionPulse");
  const actionHeadline = document.getElementById("actionHeadline");
  const actionSub = document.getElementById("actionSub");
  const btnStartCampaign = document.getElementById("btnStartCampaign");
  const btnPauseCampaign = document.getElementById("btnPauseCampaign");
  const btnResumeCampaign = document.getElementById("btnResumeCampaign");
  const btnSkipCurrent = document.getElementById("btnSkipCurrent");
  const btnStopCampaign = document.getElementById("btnStopCampaign");
  const btnExportCSV = document.getElementById("btnExportCSV");
  const btnRetryFailed = document.getElementById("btnRetryFailed");
  const btnRetryFailedCount = document.getElementById("btnRetryFailedCount");
  const campaignProgressFill = document.getElementById("campaignProgressFill");
  const progressSummaryText = document.getElementById("progressSummaryText");
  const progressPercentText = document.getElementById("progressPercentText");

  // DOM Elements - Queue Table
  const queueTableBody = document.getElementById("queueTableBody");
  const filterBtns = document.querySelectorAll(".filter-btn");
  const filterAllCount = document.getElementById("filterAllCount");
  const filterPendingCount = document.getElementById("filterPendingCount");
  const filterSentCount = document.getElementById("filterSentCount");
  const filterFailedCount = document.getElementById("filterFailedCount");

  // DOM Elements - Terminal Logs
  const terminalLogs = document.getElementById("terminalLogs");
  const checkAutoScroll = document.getElementById("checkAutoScroll");
  const btnClearLogs = document.getElementById("btnClearLogs");

  // ==========================================
  // Initialization
  // ==========================================
  initSSE();
  fetchLeadFiles();
  setupEventListeners();
  updateMessagePreview();

  // ==========================================
  // Real-Time Event Stream (SSE)
  // ==========================================
  function initSSE() {
    if (state.eventSource) {
      state.eventSource.close();
    }

    state.eventSource = new EventSource("/api/stream");

    state.eventSource.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        handleStreamEvent(payload);
      } catch (err) {
        // Heartbeat or raw string
      }
    };

    state.eventSource.onerror = () => {
      console.warn("SSE connection interrupted. Reconnecting in 3s...");
      setTimeout(initSSE, 3000);
    };
  }

  function handleStreamEvent(payload) {
    const { event, data, timestamp } = payload;

    switch (event) {
      case "init_snapshot":
        updateEngineState(data.state, data.is_ready);
        if (data.queue && data.queue.length > 0) {
          state.queue = data.queue;
          state.stats = data.stats;
          renderQueueTable();
          updateStatsUI();
        }
        if (data.dispatch_mode) dispatchModeSelect.value = data.dispatch_mode;
        if (data.delay_min) delayMin.value = data.delay_min;
        if (data.delay_max) delayMax.value = data.delay_max;
        if (data.default_country_code) defaultCountryCode.value = data.default_country_code;
        if (data.message_template && data.message_template.trim()) {
          messageTemplate.value = data.message_template;
        }
        updateMessagePreview();
        break;

      case "state_change":
        updateEngineState(data.state);
        break;

      case "log":
        appendTerminalLog(data.timestamp, data.message, data.level);
        break;

      case "contact_progress":
        handleContactProgress(data.index, data.contact, data.stats);
        break;

      case "queue_updated":
        state.queue = data.queue;
        state.stats = data.stats;
        renderQueueTable();
        updateStatsUI();
        break;

      case "cooldown":
        statCooldown.textContent = `${data.remaining_seconds}s`;
        statCooldownSub.textContent = `Delay: ${data.total_delay}s`;
        actionSub.textContent = `Pacing delay active: next message in ${data.remaining_seconds}s...`;
        break;

      default:
        break;
    }
  }

  // ==========================================
  // Engine State & UI Update
  // ==========================================
  function updateEngineState(newState, isReady = null) {
    state.engineState = newState;
    if (isReady !== null) state.isReady = isReady;

    // Reset status pill classes
    engineStatusPill.className = "status-pill";

    switch (newState) {
      case "IDLE":
        engineStatusPill.classList.add("status-idle");
        engineStatusText.textContent = "Browser Offline";
        qrAlertBanner.style.display = "none";
        actionPulse.classList.remove("active");
        setControlButtonsState({ start: true, pause: false, resume: false, stop: false, skip: false });
        break;

      case "LAUNCHING":
        engineStatusPill.classList.add("status-running");
        engineStatusText.textContent = "Launching Chrome...";
        actionHeadline.textContent = "Starting Chrome on your screen...";
        actionPulse.classList.add("active");
        break;

      case "WAITING_QR":
        engineStatusPill.classList.add("status-waiting_qr");
        engineStatusText.textContent = "Scan WhatsApp QR";
        qrAlertBanner.style.display = "flex";
        actionHeadline.textContent = "Point phone at Chrome to scan QR Code";
        actionSub.textContent = "Once scanned, WhatsApp Web will automatically become Ready.";
        actionPulse.classList.add("active");
        break;

      case "READY":
        engineStatusPill.classList.add("status-ready");
        engineStatusText.textContent = "WhatsApp Ready";
        qrAlertBanner.style.display = "none";
        actionHeadline.textContent = "WhatsApp Web is connected & ready";
        actionSub.textContent = "Configure queue and click 'Start Sending Campaign'.";
        actionPulse.classList.remove("active");
        setControlButtonsState({ start: true, pause: false, resume: false, stop: false, skip: false });
        statCooldown.textContent = "Idle";
        break;

      case "RUNNING":
        engineStatusPill.classList.add("status-running");
        engineStatusText.textContent = "Sending Messages...";
        qrAlertBanner.style.display = "none";
        actionPulse.classList.add("active");
        setControlButtonsState({ start: false, pause: true, resume: false, stop: true, skip: true });
        break;

      case "PAUSED":
        engineStatusPill.classList.add("status-paused");
        engineStatusText.textContent = "Campaign Paused";
        actionHeadline.textContent = "Automation paused";
        actionSub.textContent = "Click Resume to continue sending.";
        actionPulse.classList.remove("active");
        setControlButtonsState({ start: false, pause: false, resume: true, stop: true, skip: false });
        break;

      case "STOPPED":
        engineStatusPill.classList.add("status-idle");
        engineStatusText.textContent = "Campaign Stopped";
        actionHeadline.textContent = "Campaign stopped.";
        actionPulse.classList.remove("active");
        setControlButtonsState({ start: true, pause: false, resume: false, stop: false, skip: false });
        statCooldown.textContent = "Idle";
        break;
    }
  }

  function setControlButtonsState({ start, pause, resume, stop, skip }) {
    btnStartCampaign.disabled = !start || state.queue.length === 0;
    if (start) {
      btnStartCampaign.innerHTML = `<svg viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg> Start Sending Campaign`;
    }
    btnPauseCampaign.disabled = !pause;
    btnPauseCampaign.style.display = resume ? "none" : "inline-flex";
    btnResumeCampaign.style.display = resume ? "inline-flex" : "none";
    btnStopCampaign.disabled = !stop;
    btnSkipCurrent.disabled = !skip;
  }

  function handleContactProgress(index, contact, stats) {
    if (state.queue[index]) {
      state.queue[index] = contact;
    }
    state.stats = stats;

    actionHeadline.textContent = `Controlling Chrome for: ${contact.name}`;
    actionSub.textContent = `Number: ${contact.clean_phone || contact.raw_phone} | Status: ${contact.status.toUpperCase()}`;

    updateStatsUI();
    updateTableRow(index, contact);
  }

  // ==========================================
  // UI & Stats Renderers
  // ==========================================
  function updateStatsUI() {
    const total = state.stats.total || state.queue.length;
    const sent = state.stats.sent || 0;
    const failed = (state.stats.failed || 0) + (state.stats.not_on_whatsapp || 0);
    const remaining = Math.max(0, total - (sent + failed));

    statTotal.textContent = total;
    statSent.textContent = sent;
    statRemaining.textContent = remaining;
    statFailed.textContent = failed;
    statFailedSub.textContent = `${state.stats.not_on_whatsapp || 0} not on WA, ${state.stats.failed || 0} err`;

    const percent = total > 0 ? Math.round(((sent + failed) / total) * 100) : 0;
    statProgressPercent.textContent = `${percent}% completed`;
    campaignProgressFill.style.width = `${percent}%`;
    progressSummaryText.textContent = `${sent + failed} / ${total} dispatched`;
    progressPercentText.textContent = `${percent}%`;

    const validCount = state.queue.filter(c => c.clean_phone).length;
    statValidSummary.textContent = `${validCount} valid WhatsApp format`;

    // Filter Badges
    filterAllCount.textContent = total;
    filterPendingCount.textContent = remaining;
    filterSentCount.textContent = sent;
    filterFailedCount.textContent = failed;
    if (btnRetryFailedCount) btnRetryFailedCount.textContent = failed;
    if (btnRetryFailed) btnRetryFailed.style.display = failed > 0 ? "inline-flex" : "none";

    btnStartCampaign.disabled = state.engineState === "RUNNING" || state.engineState === "PAUSED" || state.queue.length === 0;
    if (state.engineState !== "RUNNING" && state.engineState !== "PAUSED" && btnStartCampaign.textContent.includes("Starting")) {
      btnStartCampaign.innerHTML = `<svg viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg> Start Sending Campaign`;
    }
  }

  function renderQueueTable() {
    queueTableBody.innerHTML = "";

    if (state.queue.length === 0) {
      queueTableBody.innerHTML = `
        <tr class="empty-row">
          <td colspan="6">No numbers in queue. Import scraped leads or paste numbers above.</td>
        </tr>`;
      return;
    }

    state.queue.forEach((contact, idx) => {
      // Check filter
      if (!isRowVisibleUnderFilter(contact.status, state.currentFilter)) {
        return;
      }
      const tr = createTableRowElement(idx, contact);
      queueTableBody.appendChild(tr);
    });
  }

  function isRowVisibleUnderFilter(status, filter) {
    if (filter === "all") return true;
    if (filter === "pending" && (status === "pending" || status === "sending")) return true;
    if (filter === "sent" && status === "sent") return true;
    if (filter === "failed" && (status === "failed" || status === "not_on_whatsapp" || status === "skipped")) return true;
    return false;
  }

  function createTableRowElement(idx, contact) {
    const tr = document.createElement("tr");
    tr.id = `queue-row-${idx}`;
    if (contact.status === "sending") tr.classList.add("row-active");

    const badgeClass = getBadgeClassForStatus(contact.status);
    const statusLabel = getStatusLabel(contact.status);

    tr.innerHTML = `
      <td>${contact.id || idx + 1}</td>
      <td><strong>${escapeHtml(contact.name || "Customer")}</strong></td>
      <td>
        <code>${escapeHtml(contact.clean_phone || contact.raw_phone)}</code>
        ${!contact.clean_phone ? '<span style="color:#f87171;font-size:0.7rem;"> (Invalid)</span>' : ''}
      </td>
      <td><span style="color:var(--text-muted);font-size:0.75rem;">${escapeHtml(contact.category || contact.address || "-")}</span></td>
      <td><span class="badge-status ${badgeClass}">${statusLabel}</span></td>
      <td><span style="font-size:0.75rem;color:var(--text-dim);">${contact.sent_at || "-"}</span></td>
    `;
    return tr;
  }

  function updateTableRow(idx, contact) {
    const existingRow = document.getElementById(`queue-row-${idx}`);
    if (existingRow) {
      // Check if row is visible under the currently selected tab filter
      if (!isRowVisibleUnderFilter(contact.status, state.currentFilter)) {
        existingRow.style.display = "none";
        return;
      }
      existingRow.style.display = "";

      const badgeClass = getBadgeClassForStatus(contact.status);
      const statusLabel = getStatusLabel(contact.status);

      if (contact.status === "sending") {
        existingRow.classList.add("row-active");
      } else {
        existingRow.classList.remove("row-active");
      }

      const statusCell = existingRow.querySelector("td:nth-child(5)");
      if (statusCell) {
        statusCell.innerHTML = `<span class="badge-status ${badgeClass}">${statusLabel}</span>`;
      }
      const timeCell = existingRow.querySelector("td:nth-child(6)");
      if (timeCell && contact.sent_at) {
        timeCell.innerHTML = `<span style="font-size:0.75rem;color:var(--text-dim);">${contact.sent_at}</span>`;
      }
    }
  }

  function getBadgeClassForStatus(status) {
    switch (status) {
      case "sent": return "badge-sent";
      case "sending": return "badge-sending";
      case "not_on_whatsapp": return "badge-not_on_whatsapp";
      case "skipped": return "badge-failed";
      case "failed": return "badge-failed";
      default: return "badge-pending";
    }
  }

  function getStatusLabel(status) {
    switch (status) {
      case "sent": return "SENT ✓";
      case "sending": return "SENDING...";
      case "not_on_whatsapp": return "NOT ON WA";
      case "skipped": return "SKIPPED";
      case "failed": return "FAILED";
      default: return "PENDING";
    }
  }

  // ==========================================
  // Terminal Logs
  // ==========================================
  function appendTerminalLog(timestamp, message, level = "info") {
    if (!terminalLogs) return;
    const div = document.createElement("div");
    div.className = `log-line log-${level}`;
    div.innerHTML = `<span class="log-time">[${timestamp}]</span> ${escapeHtml(message)}`;
    terminalLogs.appendChild(div);

    if (checkAutoScroll && checkAutoScroll.checked) {
      terminalLogs.scrollTop = terminalLogs.scrollHeight;
    }
  }

  // ==========================================
  // Leads Files Loading & Parsing
  // ==========================================
  async function fetchLeadFiles() {
    try {
      const res = await fetch("/api/leads-files");
      const data = await res.json();
      state.leadFiles = data.files || [];

      selectLeadFile.innerHTML = "";
      if (state.leadFiles.length === 0) {
        selectLeadFile.innerHTML = '<option value="">No lead CSV files found</option>';
        leadFileInfo.style.display = "none";
        return;
      }

      state.leadFiles.forEach((f) => {
        const opt = document.createElement("option");
        opt.value = f.filepath;
        opt.textContent = `${f.filename} (${f.size_kb} KB - ${f.modified})`;
        selectLeadFile.appendChild(opt);
      });

      handleLeadFileSelection();
    } catch (err) {
      console.error("Error fetching lead files:", err);
    }
  }

  function handleLeadFileSelection() {
    const selectedPath = selectLeadFile.value;
    const file = state.leadFiles.find(f => f.filepath === selectedPath);
    if (file) {
      state.selectedLeadFile = file;
      fileSizeBadge.textContent = `${file.size_kb} KB`;
      fileCountBadge.textContent = file.filename;
      leadFileInfo.style.display = "flex";
    } else {
      leadFileInfo.style.display = "none";
    }
  }

  async function loadSelectedLeadFile() {
    if (!selectLeadFile.value) return;

    btnLoadSelectedFile.disabled = true;
    btnLoadSelectedFile.textContent = "Loading...";

    try {
      const res = await fetch("/api/leads-files/load", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          filepath: selectLeadFile.value,
          default_country_code: defaultCountryCode.value.trim() || "91"
        })
      });

      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || "Failed to load CSV");

      // Set items to engine queue
      await sendQueueToBackend(data.items);
      appendTerminalLog(new Date().toLocaleTimeString(), `Imported ${data.items.length} leads from ${state.selectedLeadFile?.filename || "file"}.`, "success");
    } catch (err) {
      alert(`Error loading file: ${err.message}`);
      appendTerminalLog(new Date().toLocaleTimeString(), `Error loading file: ${err.message}`, "error");
    } finally {
      btnLoadSelectedFile.disabled = false;
      btnLoadSelectedFile.textContent = "Import Into Queue";
    }
  }

  async function parsePastedNumbers() {
    const text = rawNumbersInput.value.trim();
    if (!text) {
      alert("Please paste or type at least one phone number.");
      return;
    }

    btnParsePasted.disabled = true;
    btnParsePasted.textContent = "Parsing numbers...";

    try {
      const res = await fetch("/api/parse-raw-numbers", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          raw_text: text,
          default_country_code: defaultCountryCode.value.trim() || "91"
        })
      });

      const data = await res.json();
      if (data.items.length === 0) {
        alert("No valid phone numbers found in input. Please check the format.");
        return;
      }

      await sendQueueToBackend(data.items);
      appendTerminalLog(new Date().toLocaleTimeString(), `Parsed ${data.items.length} numbers from text entry.`, "success");
    } catch (err) {
      alert(`Error parsing numbers: ${err.message}`);
    } finally {
      btnParsePasted.disabled = false;
      btnParsePasted.textContent = "Parse & Add to Queue";
    }
  }

  async function sendQueueToBackend(items) {
    const template = normalizeMessageText(messageTemplate.value).trim();
    const minD = parseInt(delayMin.value) || 7;
    const maxD = parseInt(delayMax.value) || 14;
    const mode = dispatchModeSelect.value;
    const cc = defaultCountryCode.value.trim() || "91";

    const res = await fetch("/api/campaign/queue", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        items: items,
        message_template: template,
        delay_min: minD,
        delay_max: maxD,
        dispatch_mode: mode,
        default_country_code: cc
      })
    });

    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || "Failed to configure queue");
    state.queue = data.queue;
    state.stats = data.stats;
    renderQueueTable();
    updateStatsUI();
  }

  // ==========================================
  // Campaign Controls
  // ==========================================
  async function startCampaign() {
    if (state.queue.length === 0) {
      alert("Queue is empty! Please load or paste phone numbers first.");
      return;
    }

    if (!messageTemplate.value.trim()) {
      alert("Please enter a message template before starting.");
      messageTemplate.focus();
      return;
    }

    try {
      btnStartCampaign.disabled = true;
      btnStartCampaign.textContent = "Starting...";

      const res = await fetch("/api/campaign/start", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          message_template: normalizeMessageText(messageTemplate.value).trim(),
          delay_min: parseInt(delayMin.value) || 7,
          delay_max: parseInt(delayMax.value) || 14,
          dispatch_mode: dispatchModeSelect.value,
          default_country_code: defaultCountryCode.value.trim() || "91"
        })
      });

      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || "Failed to start campaign");
      appendTerminalLog(new Date().toLocaleTimeString(), "Campaign automation launched! Controlling Chrome on your screen...", "info");
    } catch (err) {
      alert(`Failed to start campaign: ${err.message}`);
      btnStartCampaign.disabled = false;
      btnStartCampaign.innerHTML = `<svg viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg> Start Sending Campaign`;
    }
  }

  async function pauseCampaign() {
    try {
      const res = await fetch("/api/campaign/pause", { method: "POST" });
      if (!res.ok) throw new Error("Failed to pause campaign");
    } catch (err) {
      console.error(err);
      alert(`Error pausing campaign: ${err.message}`);
    }
  }

  async function resumeCampaign() {
    try {
      const res = await fetch("/api/campaign/resume", { method: "POST" });
      if (!res.ok) throw new Error("Failed to resume campaign");
    } catch (err) {
      console.error(err);
      alert(`Error resuming campaign: ${err.message}`);
    }
  }

  async function stopCampaign() {
    if (!confirm("Are you sure you want to stop the campaign?")) return;
    try {
      const res = await fetch("/api/campaign/stop", { method: "POST" });
      if (!res.ok) throw new Error("Failed to stop campaign");
    } catch (err) {
      console.error(err);
      alert(`Error stopping campaign: ${err.message}`);
    }
  }

  async function skipCurrentContact() {
    try {
      const res = await fetch("/api/campaign/skip", { method: "POST" });
      if (!res.ok) throw new Error("Failed to skip contact");
      appendTerminalLog(new Date().toLocaleTimeString(), "Skip signal sent. Advancing to next contact...", "warning");
    } catch (err) {
      console.error(err);
      alert(`Error skipping contact: ${err.message}`);
    }
  }

  // ==========================================
  // Browser Management
  // ==========================================
  async function launchBrowser() {
    btnLaunchBrowser.disabled = true;
    btnLaunchBrowser.textContent = "Launching...";
    try {
      const res = await fetch("/api/browser/launch", { method: "POST" });
      if (!res.ok) throw new Error("Server error launching browser");
      appendTerminalLog(new Date().toLocaleTimeString(), "Requested Chrome launch. Opening visible browser window...", "info");
    } catch (err) {
      alert(`Failed to launch browser: ${err.message}`);
    } finally {
      setTimeout(() => {
        btnLaunchBrowser.disabled = false;
        btnLaunchBrowser.innerHTML = `
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:16px;height:16px;">
            <circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="4"/>
            <line x1="21.17" y1="8" x2="12" y2="8"/><line x1="3.95" y1="6.06" x2="8.54" y2="14"/><line x1="10.88" y1="21.94" x2="15.46" y2="14"/>
          </svg> Launch Visible Chrome`;
      }, 3000);
    }
  }

  async function closeBrowser() {
    if (!confirm("Close the Chrome browser window?")) return;
    try {
      const res = await fetch("/api/browser/close", { method: "POST" });
      if (!res.ok) throw new Error("Failed to close browser");
    } catch (err) {
      console.error(err);
      alert(`Error closing browser: ${err.message}`);
    }
  }

  // ==========================================
  // Message Template & Personalization Preview
  // ==========================================
  function normalizeMessageText(text) {
    if (!text) return "";
    return text.replace(/\u2028/g, "\n").replace(/\u2029/g, "\n").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  }

  function updateMessagePreview() {
    const raw = normalizeMessageText(messageTemplate.value);
    charCount.textContent = raw.length;

    if (!raw.trim()) {
      previewText.textContent = "Hello! Your WhatsApp message will preview here...";
      return;
    }

    // Sample contact for live preview
    const sample = (state.queue && state.queue.length > 0) ? state.queue[0] : {
      name: "Smile Arts Dental Clinic",
      category: "Dental clinic",
      address: "Janakpuri, New Delhi",
      rating: "4.9",
      reviews: "120",
      website: "https://snenergylabs.com",
      raw_phone: "+91 98765 43210",
      clean_phone: "919876543210"
    };

    const name = (sample.name && String(sample.name).trim() && String(sample.name).toLowerCase() !== "nan") ? sample.name : "Customer";
    const phone = sample.clean_phone || sample.raw_phone || sample.phone || "+91 98765 43210";
    const address = (sample.address && String(sample.address).trim() && String(sample.address).toLowerCase() !== "nan") ? sample.address : "New Delhi";
    const category = (sample.category && String(sample.category).trim() && String(sample.category).toLowerCase() !== "nan") ? sample.category : "business";
    const rating = (sample.rating && String(sample.rating).trim() && String(sample.rating).toLowerCase() !== "nan") ? sample.rating : "4.9";
    const reviews = (sample.reviews && String(sample.reviews).trim() && String(sample.reviews).toLowerCase() !== "nan") ? sample.reviews : "120";
    const website = (sample.website && String(sample.website).trim() && String(sample.website).toLowerCase() !== "nan") ? sample.website : "https://snenergylabs.com";

    let rendered = raw
      .replace(/{name}/gi, name)
      .replace(/{business_name}/gi, name)
      .replace(/{clinic}/gi, name)
      .replace(/{category}/gi, category)
      .replace(/{address}/gi, address)
      .replace(/{city}/gi, address)
      .replace(/{rating}/gi, rating)
      .replace(/{reviews}/gi, reviews)
      .replace(/{website}/gi, website)
      .replace(/{phone}/gi, phone)
      .replace(/{phone_number}/gi, phone);

    previewText.textContent = normalizeMessageText(rendered);
  }

  function insertVariableAtCursor(tag) {
    const startPos = messageTemplate.selectionStart;
    const endPos = messageTemplate.selectionEnd;
    const currentVal = messageTemplate.value;

    messageTemplate.value = currentVal.substring(0, startPos) + tag + currentVal.substring(endPos, currentVal.length);
    messageTemplate.selectionStart = messageTemplate.selectionEnd = startPos + tag.length;
    messageTemplate.focus();

    updateMessagePreview();
  }

  // ==========================================
  // Event Listeners Setup
  // ==========================================
  function setupEventListeners() {
    // Tab switching
    sourceTabs.forEach((tab) => {
      tab.addEventListener("click", () => {
        sourceTabs.forEach(t => t.classList.remove("active"));
        tabPanels.forEach(p => p.classList.remove("active"));
        tab.classList.add("active");
        const targetPanel = document.getElementById(tab.dataset.tab);
        if (targetPanel) targetPanel.classList.add("active");
      });
    });

    // File selection
    selectLeadFile.addEventListener("change", handleLeadFileSelection);
    btnRefreshFiles.addEventListener("click", fetchLeadFiles);
    btnLoadSelectedFile.addEventListener("click", loadSelectedLeadFile);

    // Paste & parse
    btnParsePasted.addEventListener("click", parsePastedNumbers);
    btnInsertSample.addEventListener("click", () => {
      rawNumbersInput.value = `TEETH REHABs, 092103 70993\nGalaxy Dental, 081301 12781\nSmile Arts Dental Clinic, 099589 34479`;
    });

    // File Selection & Dropzone Upload
    if (btnBrowseFile) {
      btnBrowseFile.addEventListener("click", (e) => {
        e.stopPropagation();
        csvFileInput.click();
      });
    }

    if (btnLoadCustomPath) {
      btnLoadCustomPath.addEventListener("click", loadCustomFilePath);
    }

    if (customFilePathInput) {
      customFilePathInput.addEventListener("keydown", (e) => {
        if (e.key === "Enter") loadCustomFilePath();
      });
    }

    dropzone.addEventListener("click", () => csvFileInput.click());
    dropzone.addEventListener("dragover", (e) => {
      e.preventDefault();
      dropzone.style.borderColor = "var(--wa-emerald)";
      dropzone.style.background = "#f0fdf4";
    });
    dropzone.addEventListener("dragleave", () => {
      dropzone.style.borderColor = "";
      dropzone.style.background = "";
    });
    dropzone.addEventListener("drop", (e) => {
      e.preventDefault();
      dropzone.style.borderColor = "";
      dropzone.style.background = "";
      if (e.dataTransfer.files.length > 0) {
        handleUploadedFile(e.dataTransfer.files[0]);
      }
    });
    csvFileInput.addEventListener("change", (e) => {
      if (e.target.files.length > 0) {
        handleUploadedFile(e.target.files[0]);
      }
    });

    // Template variables
    varTags.forEach((tag) => {
      tag.addEventListener("click", () => insertVariableAtCursor(tag.dataset.tag));
    });
    messageTemplate.addEventListener("input", updateMessagePreview);

    // Filter Buttons
    filterBtns.forEach((btn) => {
      btn.addEventListener("click", () => {
        filterBtns.forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        state.currentFilter = btn.dataset.filter;
        renderQueueTable();
      });
    });

    // Browser Controls
    btnLaunchBrowser.addEventListener("click", launchBrowser);
    btnCloseBrowser.addEventListener("click", closeBrowser);

    // Campaign Controls
    btnStartCampaign.addEventListener("click", startCampaign);
    btnPauseCampaign.addEventListener("click", pauseCampaign);
    btnResumeCampaign.addEventListener("click", resumeCampaign);
    btnStopCampaign.addEventListener("click", stopCampaign);
    btnSkipCurrent.addEventListener("click", skipCurrentContact);

    // Export CSV
    btnExportCSV.addEventListener("click", () => {
      window.location.href = "/api/export-results";
    });

    // Retry Failed Numbers
    if (btnRetryFailed) {
      btnRetryFailed.addEventListener("click", async () => {
        try {
          btnRetryFailed.disabled = true;
          const res = await fetch("/api/campaign/retry-failed", { method: "POST" });
          const data = await res.json();
          if (data.stats) {
            state.stats = data.stats;
          }
          state.queue.forEach(c => {
            if (c.status === "failed" || c.status === "not_on_whatsapp" || c.status === "skipped") {
              c.status = "pending";
              c.error = null;
            }
          });
          renderQueueTable();
          updateStatsUI();
          appendTerminalLog(new Date().toLocaleTimeString(), `Reset ${data.reset_count || 0} failed numbers back to pending queue. Ready to resume!`, "success");
        } catch (err) {
          alert(`Error resetting failed contacts: ${err.message}`);
        } finally {
          btnRetryFailed.disabled = false;
        }
      });
    }

    // Clear Terminal Logs
    if (btnClearLogs) {
      btnClearLogs.addEventListener("click", () => {
        if (terminalLogs) terminalLogs.innerHTML = "";
      });
    }
  }

  async function handleUploadedFile(file) {
    const validExts = [".csv", ".xlsx", ".xls", ".json", ".txt"];
    const ext = "." + (file.name.split(".").pop() || "").toLowerCase();
    if (!validExts.includes(ext)) {
      alert(`Unsupported file format (${ext}). Please select a CSV (.csv), Excel (.xlsx/.xls), JSON (.json), or Text (.txt) lead file.`);
      return;
    }

    appendTerminalLog(new Date().toLocaleTimeString(), `Uploading & parsing selected file: ${file.name}...`, "info");

    const formData = new FormData();
    formData.append("file", file);
    formData.append("default_country_code", defaultCountryCode.value.trim() || "91");

    try {
      const res = await fetch("/api/upload-file", {
        method: "POST",
        body: formData
      });

      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || "Failed to parse file");

      if (data.items.length === 0) {
        alert("No valid phone numbers found in the selected file.");
        return;
      }

      await sendQueueToBackend(data.items);

      // Show selected file status badge
      if (selectedFileInfo) {
        selectedFileNameBadge.textContent = file.name;
        selectedFileCountBadge.textContent = `${data.items.length} Leads`;
        selectedFileInfo.style.display = "flex";
      }

      appendTerminalLog(new Date().toLocaleTimeString(), `Successfully loaded ${data.items.length} contacts from "${file.name}" into the queue!`, "success");
    } catch (err) {
      alert(`Error reading file: ${err.message}`);
      appendTerminalLog(new Date().toLocaleTimeString(), `Error loading file: ${err.message}`, "error");
    }
  }

  async function loadCustomFilePath() {
    const path = customFilePathInput.value.trim();
    if (!path) {
      alert("Please enter a file path (e.g. ~/Downloads/leads.csv, /Users/.../leads.xlsx, or leads.json)");
      return;
    }

    btnLoadCustomPath.disabled = true;
    btnLoadCustomPath.textContent = "Loading...";

    try {
      const res = await fetch("/api/leads-files/load", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          filepath: path,
          default_country_code: defaultCountryCode.value.trim() || "91"
        })
      });

      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || "Failed to load lead file from path");

      await sendQueueToBackend(data.items);

      if (selectedFileInfo) {
        selectedFileNameBadge.textContent = data.filename || path.split("/").pop();
        selectedFileCountBadge.textContent = `${data.items.length} Leads`;
        selectedFileInfo.style.display = "flex";
      }

      appendTerminalLog(new Date().toLocaleTimeString(), `Loaded ${data.items.length} contacts from "${path}".`, "success");
    } catch (err) {
      alert(`Error loading file path: ${err.message}`);
      appendTerminalLog(new Date().toLocaleTimeString(), `Error loading path: ${err.message}`, "error");
    } finally {
      btnLoadCustomPath.disabled = false;
      btnLoadCustomPath.textContent = "Load File";
    }
  }

  function escapeHtml(str) {
    if (!str) return "";
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }
});
