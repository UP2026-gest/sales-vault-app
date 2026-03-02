// Sales Vault – Opportunità + Fatture
// Salvataggio automatico in localStorage + sincronizzazione GitHub (shared storage)
// Popup promemoria per azioni scadute + fatture pianificate scadute (avvio + ogni 15 minuti)

const STORAGE_KEY = "sales_vault_v1"; // ✅ stesso key: retro-compatibile con backup e dati esistenti
const GH_CONFIG_KEY = "sales_vault_gh_config";

// ════════════════════════════════════════════════════════════
// GITHUB SYNC MODULE
// ════════════════════════════════════════════════════════════

const GH = {
  // Legge la configurazione dal localStorage di questo browser
  getConfig() {
    try {
      const raw = localStorage.getItem(GH_CONFIG_KEY);
      return raw ? JSON.parse(raw) : null;
    } catch { return null; }
  },

  saveConfig(cfg) {
    localStorage.setItem(GH_CONFIG_KEY, JSON.stringify(cfg));
  },

  clearConfig() {
    localStorage.removeItem(GH_CONFIG_KEY);
  },

  isConfigured() {
    const c = this.getConfig();
    return !!(c?.token && c?.owner && c?.repo && c?.path);
  },

  // Costruisce l'URL API per il file dati
  apiUrl() {
    const c = this.getConfig();
    if (!c) return null;
    return `https://api.github.com/repos/${c.owner}/${c.repo}/contents/${c.path}`;
  },

  // Scarica il file JSON dal repo GitHub. Restituisce { content, sha } o null.
  async pull() {
    const cfg = this.getConfig();
    if (!cfg) return null;
    try {
      const res = await fetch(this.apiUrl(), {
        headers: {
          "Authorization": `Bearer ${cfg.token}`,
          "Accept": "application/vnd.github+json",
          "X-GitHub-Api-Version": "2022-11-28",
        }
      });
      if (res.status === 404) return { content: null, sha: null }; // file non ancora creato
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.message || `HTTP ${res.status}`);
      }
      const data = await res.json();
      const decoded = JSON.parse(atob(data.content.replace(/\n/g, "")));
      return { content: decoded, sha: data.sha };
    } catch (e) {
      console.warn("GH pull error:", e);
      GH._lastError = e.message || String(e);
      return null;
    }
  },

  // Carica il JSON su GitHub (crea o aggiorna il file). sha richiesto per aggiornamento.
  async push(jsonData, sha) {
    const cfg = this.getConfig();
    if (!cfg) return false;
    try {
      const encoded = btoa(unescape(encodeURIComponent(JSON.stringify(jsonData, null, 2))));
      const body = {
        message: `Sales Vault sync ${new Date().toISOString()}`,
        content: encoded,
      };
      if (sha) body.sha = sha;

      const res = await fetch(this.apiUrl(), {
        method: "PUT",
        headers: {
          "Authorization": `Bearer ${cfg.token}`,
          "Accept": "application/vnd.github+json",
          "Content-Type": "application/json",
          "X-GitHub-Api-Version": "2022-11-28",
        },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.message || `HTTP ${res.status}`);
      }
      const result = await res.json();
      // Salva il nuovo SHA localmente per il prossimo push
      const newSha = result?.content?.sha;
      if (newSha) {
        const c = this.getConfig();
        c._lastSha = newSha;
        this.saveConfig(c);
      }
      return true;
    } catch (e) {
      console.warn("GH push error:", e);
      GH._lastError = e.message || String(e);
      return false;
    }
  },

  // Testa la connessione: prima GET (lettura), poi verifica permessi repo
  async testConnection(token, owner, repo, path) {
    try {
      // Step 1: verifica che il repo esista e il token sia valido
      const repoUrl = `https://api.github.com/repos/${owner}/${repo}`;
      const repoRes = await fetch(repoUrl, {
        headers: {
          "Authorization": `Bearer ${token}`,
          "Accept": "application/vnd.github+json",
          "X-GitHub-Api-Version": "2022-11-28",
        }
      });
      if (repoRes.status === 401) return { ok: false, message: "Token non valido o scaduto (401)" };
      if (repoRes.status === 403) return { ok: false, message: "Token non ha i permessi necessari (403)" };
      if (repoRes.status === 404) return { ok: false, message: `Repository "${owner}/${repo}" non trovato (404) — controlla username e nome repo` };
      if (!repoRes.ok) {
        const err = await repoRes.json().catch(() => ({}));
        return { ok: false, message: err.message || `HTTP ${repoRes.status}` };
      }

      // Step 2: verifica che il file esista o che il percorso sia accessibile
      const fileUrl = `https://api.github.com/repos/${owner}/${repo}/contents/${path}`;
      const fileRes = await fetch(fileUrl, {
        headers: {
          "Authorization": `Bearer ${token}`,
          "Accept": "application/vnd.github+json",
          "X-GitHub-Api-Version": "2022-11-28",
        }
      });
      // 200 = file esiste (read OK), 404 = file non ancora creato (va bene, verrà creato al primo salvataggio)
      if (fileRes.status === 200) return { ok: true, status: 200, message: "File trovato ✓" };
      if (fileRes.status === 404) return { ok: true, status: 404, message: "File non ancora creato — verrà creato al primo salvataggio ✓" };
      if (fileRes.status === 403) return { ok: false, message: "Token non ha il permesso Contents: Read and write (403)" };
      const err = await fileRes.json().catch(() => ({}));
      return { ok: false, message: err.message || `HTTP ${fileRes.status}` };
    } catch (e) {
      return { ok: false, message: `Errore di rete: ${e.message}` };
    }
  }
};

// SHA in memoria per il ciclo push/pull corrente
let _ghCurrentSha = null;

// Stato sync visibile in UI
function setGhStatus(msg, type = "info") {
  // type: "info" | "ok" | "error" | "syncing"
  const el = document.getElementById("ghStatus");
  if (!el) return;
  const icons = { info: "☁️", ok: "✅", error: "⚠️", syncing: "🔄" };
  el.textContent = `${icons[type] || "☁️"} ${msg}`;
  el.className = "ghStatus " + type;
}

// Scarica da GitHub e aggiorna il db locale (usato all'avvio e nel polling)
async function ghPullAndMerge(silent = false) {
  if (!GH.isConfigured()) return false;
  if (!silent) setGhStatus("Scaricamento dati dal server…", "syncing");
  const result = await GH.pull();
  if (!result) {
    const reason = GH._lastError ? ` (${GH._lastError})` : "";
    if (!silent) setGhStatus(`Impossibile connettersi a GitHub${reason}`, "error");
    return false;
  }
  if (result.content === null) {
    // File non ancora esistente su GitHub → primo avvio, faremo push
    _ghCurrentSha = null;
    if (!silent) setGhStatus("Nessun dato remoto — verrà creato al primo salvataggio", "info");
    return true;
  }
  _ghCurrentSha = result.sha;
  const remote = ensureDbShape(result.content);

  const localTs = db?.updatedAt || "";
  const remoteTs = remote?.updatedAt || "";
  const localHasData = (db?.opportunities?.length || 0) > 0;

  // Accetta i dati remoti se:
  // 1. Il PC locale non ha ancora dati (primo avvio su questo browser), OPPURE
  // 2. Il timestamp remoto è più recente di quello locale
  const shouldApplyRemote = !localHasData || remoteTs > localTs;

  if (shouldApplyRemote) {
    db = remote;
    localStorage.setItem(STORAGE_KEY, JSON.stringify(db));
    refreshOwnerSelects();
    refreshLeadDatalist();
    renderAll();
    if (!silent) setGhStatus(`Sincronizzato — ${fmtItDateTime(remoteTs)}`, "ok");
  } else {
    if (!silent) setGhStatus(`Già aggiornato — ${fmtItDateTime(localTs)}`, "ok");
  }
  return true;
}

// Carica il db su GitHub dopo ogni salvataggio locale
async function ghPush() {
  if (!GH.isConfigured()) return;
  setGhStatus("Salvataggio sul server…", "syncing");

  // Se non abbiamo uno SHA valido in memoria, fai prima un pull per ottenerlo
  if (!_ghCurrentSha) {
    const fresh = await GH.pull();
    if (fresh && fresh.sha) {
      _ghCurrentSha = fresh.sha;
    }
    // se fresh.content === null il file non esiste ancora: push di creazione, sha non serve
  }

  const ok = await GH.push(db, _ghCurrentSha);
  if (ok) {
    const cfg = GH.getConfig();
    _ghCurrentSha = cfg?._lastSha || _ghCurrentSha;
    setGhStatus(`Salvato sul server — ${new Date().toLocaleTimeString("it-IT")}`, "ok");
  } else {
    // Potrebbe essere un conflitto SHA: riscarica lo SHA corrente e riprova una volta
    setGhStatus("Risoluzione conflitto SHA…", "syncing");
    const fresh = await GH.pull();
    if (fresh && fresh.sha) {
      _ghCurrentSha = fresh.sha;
      const retry = await GH.push(db, _ghCurrentSha);
      if (retry) {
        const cfg = GH.getConfig();
        _ghCurrentSha = cfg?._lastSha || _ghCurrentSha;
        setGhStatus(`Salvato sul server (retry) — ${new Date().toLocaleTimeString("it-IT")}`, "ok");
        return;
      }
    }
    setGhStatus("Errore nel salvataggio su GitHub — riproverò al prossimo salvataggio", "error");
  }
}

// Polling ogni 2 minuti: scarica aggiornamenti da altri utenti in background
let _ghPollInterval = null;
function startGhPolling() {
  if (_ghPollInterval) clearInterval(_ghPollInterval);
  _ghPollInterval = setInterval(async () => {
    if (!GH.isConfigured()) return;
    // Non fare pull se il form ha modifiche non salvate (per non disturbare l'utente)
    if (isFormDirty()) return;
    await ghPullAndMerge(false); // non-silent: aggiorna sempre la UI con lo stato corrente
  }, 30 * 1000); // ogni 30 secondi: sync quasi in tempo reale
}

// ── UI Setup Guidato ──────────────────────────────────────────────────────────

function buildGhSetupModal() {
  const cfg = GH.getConfig() || {};
  return `
    <div style="max-width:520px;">
      <p style="margin:0 0 12px; color:#444; font-size:14px;">
        Collega Sales Vault a un file su un repository GitHub privato.
        I dati saranno condivisi automaticamente tra tutti gli utenti che usano lo stesso repository.
      </p>

      <div style="background:#f0f7ff; border:1px solid #b8d4f5; border-radius:8px; padding:10px 12px; margin-bottom:14px; font-size:13px; color:#1a4a7a;">
        <b>Come configurare (una volta sola):</b><br>
        1. Crea un account su <a href="https://github.com" target="_blank">github.com</a> (gratis)<br>
        2. Crea un repository <b>privato</b> (es. <code>sales-vault-data</code>)<br>
        3. Vai su <b>Settings → Developer settings → Personal access tokens → Fine-grained tokens</b><br>
        4. Crea un token con accesso al repo: permesso <b>Contents: Read and write</b><br>
        5. Condividi token + nome repo con i colleghi (ognuno lo inserisce sul proprio PC)
      </div>

      <label style="display:block; margin-bottom:10px; font-size:14px;">
        <span style="display:block; margin-bottom:4px; font-weight:600;">Token GitHub (Personal Access Token)</span>
        <input id="ghToken" type="password" placeholder="github_pat_..." value="${cfg.token || ""}"
          style="width:100%; box-sizing:border-box; padding:8px; border:1px solid #ccc; border-radius:8px; font-size:14px; font-family:monospace;" />
      </label>

      <label style="display:block; margin-bottom:10px; font-size:14px;">
        <span style="display:block; margin-bottom:4px; font-weight:600;">Username GitHub (owner del repo)</span>
        <input id="ghOwner" type="text" placeholder="es. mario-rossi" value="${cfg.owner || ""}"
          style="width:100%; box-sizing:border-box; padding:8px; border:1px solid #ccc; border-radius:8px; font-size:14px;" />
      </label>

      <label style="display:block; margin-bottom:10px; font-size:14px;">
        <span style="display:block; margin-bottom:4px; font-weight:600;">Nome repository</span>
        <input id="ghRepo" type="text" placeholder="es. sales-vault-data" value="${cfg.repo || ""}"
          style="width:100%; box-sizing:border-box; padding:8px; border:1px solid #ccc; border-radius:8px; font-size:14px;" />
      </label>

      <label style="display:block; margin-bottom:14px; font-size:14px;">
        <span style="display:block; margin-bottom:4px; font-weight:600;">Percorso file nel repo</span>
        <input id="ghPath" type="text" placeholder="data/sales-vault.json" value="${cfg.path || "data/sales-vault.json"}"
          style="width:100%; box-sizing:border-box; padding:8px; border:1px solid #ccc; border-radius:8px; font-size:14px;" />
      </label>

      <div id="ghTestResult" style="min-height:24px; font-size:13px; margin-bottom:10px;"></div>

      <div style="display:flex; gap:8px; flex-wrap:wrap;">
        <button id="ghTestBtn" type="button" style="width:auto; padding:8px 16px;">🔌 Testa connessione</button>
        <button id="ghSaveConfigBtn" type="button" style="width:auto; padding:8px 16px; background:#1a7a3a; color:white; border-color:#1a7a3a;">✅ Salva configurazione</button>
        ${GH.isConfigured() ? `<button id="ghDisconnectBtn" type="button" style="width:auto; padding:8px 16px;" class="danger">Disconnetti</button>` : ""}
      </div>
    </div>
  `;
}

function openGhSetupModal() {
  ui.modalBody.innerHTML = buildGhSetupModal();
  ui.modal.classList.remove("hidden");

  document.getElementById("ghTestBtn")?.addEventListener("click", async () => {
    const token = document.getElementById("ghToken").value.trim();
    const owner = document.getElementById("ghOwner").value.trim();
    const repo = document.getElementById("ghRepo").value.trim();
    const path = document.getElementById("ghPath").value.trim();
    const res = document.getElementById("ghTestResult");
    res.textContent = "🔄 Connessione in corso…";
    const r = await GH.testConnection(token, owner, repo, path);
    if (r.ok) {
      res.innerHTML = `<span style="color:green;">✅ Connessione riuscita! ${r.status === 404 ? "(file non ancora esistente — verrà creato)" : "(file trovato)"}</span>`;
    } else {
      res.innerHTML = `<span style="color:red;">❌ Errore: ${r.message}</span>`;
    }
  });

  document.getElementById("ghSaveConfigBtn")?.addEventListener("click", async () => {
    const token = document.getElementById("ghToken").value.trim();
    const owner = document.getElementById("ghOwner").value.trim();
    const repo = document.getElementById("ghRepo").value.trim();
    const path = document.getElementById("ghPath").value.trim();
    if (!token || !owner || !repo || !path) {
      alert("Compila tutti i campi.");
      return;
    }
    GH.saveConfig({ token, owner, repo, path });
    closeModal();
    setGhStatus("Configurazione salvata — connessione in corso…", "syncing");
    await ghPullAndMerge(false);
    startGhPolling();
    renderBackupInfo();
  });

  document.getElementById("ghDisconnectBtn")?.addEventListener("click", () => {
    if (!confirm("Disconnettere GitHub Sync? I dati locali rimarranno intatti.")) return;
    GH.clearConfig();
    _ghCurrentSha = null;
    if (_ghPollInterval) clearInterval(_ghPollInterval);
    setGhStatus("Non configurato — modalità locale", "info");
    closeModal();
    renderBackupInfo();
  });
}

const ENUM = {
  status: ["(tutti)", "aperta", "sospesa", "chiusa vinta", "chiusa persa", "abbandonata"],
  statusValues: ["aperta", "sospesa", "chiusa vinta", "chiusa persa", "abbandonata"],

  phase: ["(tutte)",
    "contatto iniziale",
    "proposta inviata",
    "proposta accettata informalmente - mandare CTR",
    "proposta accettata - attesa CTR",
    "conseguita - non fatturata",
    "conseguita - fatturata",
    "persa"
  ],
  phaseValues: [
    "contatto iniziale",
    "proposta inviata",
    "proposta accettata informalmente - mandare CTR",
    "proposta accettata - attesa CTR",
    "conseguita - non fatturata",
    "conseguita - fatturata",
    "persa"
  ],

  product: ["(tutti)",
    "docente presenza",
    "docente online",
    "traduzione",
    "interpretariato",
    "blended presenza",
    "blended online",
    "piattaforma",
    "da definire",
    "altro"
  ],
  productValues: [
    "docente presenza",
    "docente online",
    "traduzione",
    "interpretariato",
    "blended presenza",
    "blended online",
    "piattaforma",
    "da definire",
    "altro"
  ],
  probability: ["10%", "50%", "90%", "100%"],
};

const ui = {
  status: document.getElementById("status"),
  backupInfo: document.getElementById("backupInfo"),
  newOppBtn: document.getElementById("newOppBtn"),
  exportBtn: document.getElementById("exportBtn"),
  importBtn: document.getElementById("importBtn"),
  exportCsvBtn: document.getElementById("exportCsvBtn"),
  importFile: document.getElementById("importFile"),
leadList: document.getElementById("leadList"),

  q: document.getElementById("q"),
  statusFilter: document.getElementById("statusFilter"),
  phaseFilter: document.getElementById("phaseFilter"),
  productFilter: document.getElementById("productFilter"),
  ownerFilter: document.getElementById("ownerFilter"),
  manageOwnersBtn: document.getElementById("manageOwnersBtn"),
  dueFilter: document.getElementById("dueFilter"),
invPlannedFilter: document.getElementById("invPlannedFilter"),
invIssuedFilter: document.getElementById("invIssuedFilter"),

  kpiBox: document.getElementById("kpiBox"),
  oppList: document.getElementById("oppList"),
oppCounter: document.getElementById("oppCounter"),

lead: document.getElementById("lead"),
 
oppForm: document.getElementById("oppForm"),
dirtyHint: document.getElementById("dirtyHint"),
leadContactName: document.getElementById("leadContactName"),
leadPhone: document.getElementById("leadPhone"),
leadEmail: document.getElementById("leadEmail"),
  createdAt: document.getElementById("createdAt"),
  owner: document.getElementById("owner"),
  oppName: document.getElementById("oppName"),
  oppStatus: document.getElementById("oppStatus"),
  oppPhase: document.getElementById("oppPhase"),
  product: document.getElementById("product"),
  probability: document.getElementById("probability"),
  valueExpected: document.getElementById("valueExpected"),
  serviceCost: document.getElementById("serviceCost"),
  nextAction: document.getElementById("nextAction"),
  nextActionDate: document.getElementById("nextActionDate"),
  notes: document.getElementById("notes"),

  // Fatture
  invStatus: document.getElementById("invStatus"),
  invPlannedDate: document.getElementById("invPlannedDate"),
  invPlannedAmount: document.getElementById("invPlannedAmount"),

  invNumber: document.getElementById("invNumber"),
  invDate: document.getElementById("invDate"),
  invAmount: document.getElementById("invAmount"),

  addInvBtn: document.getElementById("addInvBtn"),
  invList: document.getElementById("invList"),
  calcBox: document.getElementById("calcBox"),

  saveBtn: document.getElementById("saveBtn"),
  deleteBtn: document.getElementById("deleteBtn"),
  fileInfo: document.getElementById("fileInfo"),

  modal: document.getElementById("modal"),
  modalBody: document.getElementById("modalBody"),
  modalCloseBtn: document.getElementById("modalCloseBtn"),
  ghSyncBtn: document.getElementById("ghSyncBtn"),
};

function nowIso(){ return new Date().toISOString(); }
function pad2(n){ return String(n).padStart(2,"0"); }
function todayStr(){
  const d = new Date();
  return `${d.getFullYear()}-${pad2(d.getMonth()+1)}-${pad2(d.getDate())}`;
}
function uid(){ return `${Date.now()}_${Math.random().toString(16).slice(2)}`; }

function toNum(v){
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function truthyStr(v){ return String(v ?? "").trim(); }

function escapeHtml(s){
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

let db = loadDb();
let currentOppId = null;

let formSnapshot = "";
let dirtyBypass = false; // usato per import/azioni che non devono chiedere doppia conferma

function getFormState(){
  // Prendiamo solo campi opportunità + campi rubrica visibili,
  // non includiamo inv input temporanei perché non sono parte dell’opportunità finché non fai "Aggiungi riga".
  return {
    lead: ui.lead?.value || "",
    createdAt: ui.createdAt?.value || "",
    name: ui.oppName?.value || "",
    status: ui.oppStatus?.value || "",
    phase: ui.oppPhase?.value || "",
    product: ui.product?.value || "",
    probability: ui.probability?.value || "",
    valueExpected: ui.valueExpected?.value || "",
    serviceCost: ui.serviceCost?.value || "",
    nextAction: ui.nextAction?.value || "",
    nextActionDate: ui.nextActionDate?.value || "",
    notes: ui.notes?.value || "",

    // rubrica lead (non "fotografata" nell'opportunità ma è una modifica utente reale)
    leadContactName: ui.leadContactName?.value || "",
    leadPhone: ui.leadPhone?.value || "",
    leadEmail: ui.leadEmail?.value || "",
  };
}

function setFormSnapshot(){
  formSnapshot = JSON.stringify(getFormState());
}

function isFormDirty(){
  if(dirtyBypass) return false;
  return JSON.stringify(getFormState()) !== formSnapshot;
}

function updateDirtyHint(){
  if(!ui.dirtyHint) return;
  ui.dirtyHint.style.display = isFormDirty() ? "block" : "none";
}

function confirmIfDirty(message = "Hai modifiche non salvate. Vuoi uscire senza salvare?"){
  return !isFormDirty() || confirm(message);
}

function defaultDb(){
  return {
    version: 2,
    updatedAt: nowIso(),
    opportunities: [],
    leads: [],

    // ✅ elenco controllato dei commerciali (anti-refusi)
    salespeople: ["Renato", "Clizia", "Jelena"],

    // ✅ metadati backup
    meta: {
      activeBackupName: "(dati locali: nessun import)",
      activeBackupImportedAt: "",
      activeBackupCreatedAt: "",
      latestBackupSeenAt: "", // max(import/export) visto su questo dispositivo
lastBackupExportedAt: "",
    },
  };
}

function normalizeSalespeopleList(list){
  return [...new Set((list || [])
    .map(x => String(x ?? "").trim())
    .filter(Boolean))]
    .sort((a,b) => a.localeCompare(b, "it"));
}

function ensureDbShape(x){
  const d = x && typeof x === "object" ? x : defaultDb();
  if(!Array.isArray(d.opportunities)) d.opportunities = [];
  if(!Array.isArray(d.leads)) d.leads = [];

  if(!Array.isArray(d.salespeople) || d.salespeople.length === 0){
    d.salespeople = defaultDb().salespeople;
  }
  d.salespeople = normalizeSalespeopleList(d.salespeople);

  if(!d.meta || typeof d.meta !== "object") d.meta = defaultDb().meta;
  if(typeof d.meta.activeBackupName !== "string") d.meta.activeBackupName = defaultDb().meta.activeBackupName;
  if(typeof d.meta.activeBackupImportedAt !== "string") d.meta.activeBackupImportedAt = "";
  if(typeof d.meta.activeBackupCreatedAt !== "string") d.meta.activeBackupCreatedAt = "";
  if(typeof d.meta.latestBackupSeenAt !== "string") d.meta.latestBackupSeenAt = "";
  return d;
}

function loadDb(){
  try{
    const raw = localStorage.getItem(STORAGE_KEY);
    if(!raw) return defaultDb();
    const parsed = JSON.parse(raw);
    if(!parsed || !Array.isArray(parsed.opportunities)) return defaultDb();
    return ensureDbShape(parsed);
  }catch{
    return defaultDb();
  }
}

function refreshOwnerSelects(){
  const owners = normalizeSalespeopleList(db.salespeople);
  db.salespeople = owners;

  // filtro: (tutti) + elenco
  if(ui.ownerFilter){
    setSelectOptions(ui.ownerFilter, ["(tutti)", ...owners]);
  }

  // form: obbligatorio -> solo elenco
  if(ui.owner){
    ui.owner.innerHTML = "";
    for(const v of owners){
      const opt = document.createElement("option");
      opt.value = v; opt.textContent = v;
      ui.owner.appendChild(opt);
    }
  }
}

function fmtItDateTime(iso){
  try{ return iso ? new Date(iso).toLocaleString("it-IT") : ""; }catch{ return ""; }
}

function renderBackupInfo(){
  if(!ui.backupInfo) return;

  if (GH.isConfigured()) {
    const cfg = GH.getConfig();
    ui.backupInfo.classList.remove("warn");
    ui.backupInfo.textContent = `☁️ GitHub Sync attivo — ${cfg.owner}/${cfg.repo} → ${cfg.path}`;
    return;
  }

  const name = db?.meta?.activeBackupName || "(dati locali: nessun import)";
  const importedAt = db?.meta?.activeBackupImportedAt
    ? ` • Importato: ${fmtItDateTime(db.meta.activeBackupImportedAt)}`
    : "";
  const createdAt = db?.meta?.activeBackupCreatedAt
    ? ` • Creato: ${fmtItDateTime(db.meta.activeBackupCreatedAt)}`
    : "";

  ui.backupInfo.classList.remove("warn");

  const needsImport = !db?.meta?.activeBackupImportedAt;
  const reminder = needsImport
    ? "\u2139\ufe0f Promemoria: premi \"Importa backup\" per ripristinare i dati, se stai lavorando da un file. "
    : "";

  ui.backupInfo.textContent = `${reminder}Backup in uso: ${name}${importedAt}${createdAt}`;
}
function saveDb(){
  refreshLeadDatalist();
  db.version = 2;
  db.updatedAt = nowIso();
  localStorage.setItem(STORAGE_KEY, JSON.stringify(db));
  ui.status.textContent = `Salvato automaticamente • Ultimo salvataggio: ${new Date().toLocaleString("it-IT")}`;
  renderBackupInfo();
  // ✅ GitHub Sync: carica su GitHub dopo ogni salvataggio locale
  if (GH.isConfigured()) {
    ghPush(); // fire-and-forget
  }
}

function setSelectOptions(selectEl, arr){
  selectEl.innerHTML = "";
  for(const v of arr){
    const opt = document.createElement("option");
    opt.value = v === "(tutti)" || v === "(tutte)" ? "" : v;
    opt.textContent = v;
    selectEl.appendChild(opt);
  }
}

function initSelects(){
  setSelectOptions(ui.statusFilter, ENUM.status);
  setSelectOptions(ui.phaseFilter, ENUM.phase);
  setSelectOptions(ui.productFilter, ENUM.product);

  // commerciali
  refreshOwnerSelects();

  ui.oppStatus.innerHTML = "";
  for(const v of ENUM.statusValues){
    const opt = document.createElement("option");
    opt.value = v; opt.textContent = v;
    ui.oppStatus.appendChild(opt);
  }

  ui.oppPhase.innerHTML = "";
  for(const v of ENUM.phaseValues){
    const opt = document.createElement("option");
    opt.value = v; opt.textContent = v;
    ui.oppPhase.appendChild(opt);
  }

  ui.product.innerHTML = "";
  for(const v of ENUM.productValues){
    const opt = document.createElement("option");
    opt.value = v; opt.textContent = v;
    ui.product.appendChild(opt);
  }

  ui.probability.innerHTML = "";
  for(const v of ENUM.probability){
    const opt = document.createElement("option");
    opt.value = v; opt.textContent = v;
    ui.probability.appendChild(opt);
  }
}

function normalizeInvoice(x){
  // retro-compat: se esistono number/date -> emessa, altrimenti pianificata
  const hasIssuedData = !!String(x?.number ?? "").trim() || !!String(x?.date ?? "").trim();
  const inferred = x?.status ? x.status : (hasIssuedData ? "emessa" : "pianificata");
  const status = (inferred === "pianificata" || inferred === "emessa") ? inferred : "emessa";

  return {
    id: x?.id || uid(),
    status, // "pianificata" | "emessa"

    // pianificata
    plannedDate: x?.plannedDate || "",
    plannedAmount: toNum(x?.plannedAmount),

    // emessa
    number: x?.number || "",
    date: x?.date || "",
    amount: toNum(x?.amount),

    createdAt: x?.createdAt || nowIso(),
    updatedAt: x?.updatedAt || nowIso(),
  };
}

function normalizeOpp(o){
  const inv = Array.isArray(o.invoices) ? o.invoices : [];
  const owners = normalizeSalespeopleList(db?.salespeople);
  const rawOwner = String(o.owner ?? "").trim();
  const safeOwner = rawOwner && owners.includes(rawOwner) ? rawOwner : (owners[0] || "");
  return {
    id: o.id || uid(),
    lead: o.lead || "",
    createdAt: o.createdAt || todayStr(),
    owner: safeOwner,
    name: o.name || "",
    status: o.status || "aperta",
    phase: o.phase || "contatto iniziale",
    product: o.product || "da definire",
    valueExpected: toNum(o.valueExpected),
    probability: o.probability || "50%",
    nextAction: o.nextAction || "",
    nextActionDate: o.nextActionDate || "",
    notes: o.notes || "",
serviceCost: toNum(o.serviceCost),
invoices: inv.map(normalizeInvoice),
createdAtTs: o.createdAtTs || nowIso(),
updatedAt: o.updatedAt || nowIso(),
  };
}

function normLeadKey(name){
  return String(name || "").trim().toLowerCase();
}

function getLeadByName(name){
  const key = normLeadKey(name);
  if(!key) return null;
  return (db.leads || []).find(l => normLeadKey(l.name) === key) || null;
}

function upsertLeadFromInputs(){
  const name = ui.lead.value.trim();
  if(!name) return;

  if(!Array.isArray(db.leads)) db.leads = [];

  const existing = getLeadByName(name);
  const payload = {
    name,
    contactName: ui.leadContactName.value.trim(),
    phone: ui.leadPhone.value.trim(),
    email: ui.leadEmail.value.trim(),
    updatedAt: nowIso(),
    createdAt: existing?.createdAt || nowIso(),
  };

  if(existing){
    Object.assign(existing, payload);
  }else{
    db.leads.push(payload);
  }
}

function fillLeadContactFields(name){
  const lead = getLeadByName(name);
  ui.leadContactName.value = lead?.contactName || "";
  ui.leadPhone.value = lead?.phone || "";
  ui.leadEmail.value = lead?.email || "";
}

function refreshLeadDatalist(){
  if(!ui.leadList) return;
  ui.leadList.innerHTML = "";
  const items = (db.leads || [])
    .map(l => l.name)
    .filter(Boolean)
    .sort((a,b) => a.localeCompare(b, "it"));

  for(const name of items){
    const opt = document.createElement("option");
    opt.value = name;
    ui.leadList.appendChild(opt);
  }
}

function oppToForm(o){
  currentOppId = o.id;
  ui.lead.value = o.lead;
fillLeadContactFields(o.lead);
  ui.createdAt.value = o.createdAt;
  if(ui.owner) ui.owner.value = o.owner || (db.salespeople?.[0] || "");
  ui.oppName.value = o.name;
  ui.oppStatus.value = o.status;
  ui.oppPhase.value = o.phase;
  ui.product.value = o.product;
  ui.valueExpected.value = o.valueExpected || "";
  ui.probability.value = o.probability;
  ui.nextAction.value = o.nextAction;
  ui.nextActionDate.value = o.nextActionDate || "";
  ui.notes.value = o.notes;
  ui.serviceCost.value = o.serviceCost || "";

  renderInvoices(o.invoices);
  renderCalcBox(o);

  ui.deleteBtn.disabled = false;
  ui.fileInfo.textContent = `ID opportunità: ${o.id}`;
setFormSnapshot();
updateDirtyHint();
}

function formToOpp(){
  const existing = db.opportunities.find(x => x.id === currentOppId);
  const inv = existing?.invoices || [];
  return normalizeOpp({
    id: currentOppId || uid(),
    lead: ui.lead.value.trim(),
    createdAt: ui.createdAt.value,
    owner: ui.owner?.value || (db.salespeople?.[0] || ""),
    name: ui.oppName.value.trim(),
    status: ui.oppStatus.value,
    phase: ui.oppPhase.value,
    product: ui.product.value,
    valueExpected: ui.valueExpected.value,
    probability: ui.probability.value,
    nextAction: ui.nextAction.value.trim(),
    nextActionDate: ui.nextActionDate.value,
    notes: ui.notes.value.trim(),
    serviceCost: ui.serviceCost.value,
    invoices: inv,
    createdAtTs: existing?.createdAtTs || nowIso(),
    updatedAt: nowIso(),
  });
}

function resetInvoiceInputs(){
  ui.invPlannedDate.value = "";
  ui.invPlannedAmount.value = "";
  ui.invNumber.value = "";
  ui.invDate.value = "";
  ui.invAmount.value = "";
}

function newOpp(){
  currentOppId = null;
  ui.oppForm.reset();
  ui.createdAt.value = todayStr();
  if(ui.owner) ui.owner.value = db.salespeople?.[0] || "";
  ui.oppStatus.value = "aperta";
  ui.oppPhase.value = "contatto iniziale";
  ui.product.value = "da definire";
  ui.probability.value = "50%";
  ui.deleteBtn.disabled = true;

  if(ui.invStatus) ui.invStatus.value = "pianificata";
  resetInvoiceInputs();
setFormSnapshot();
updateDirtyHint();

  ui.invList.textContent = "Nessuna riga fattura.";
  ui.invList.classList.add("muted");
  ui.calcBox.textContent = "";
  ui.fileInfo.textContent = "";
}

function totalIssued(o){
  return (o.invoices || [])
    .filter(x => x.status === "emessa")
    .reduce((s,x) => s + toNum(x.amount), 0);
}

function totalPlanned(o){
  return (o.invoices || [])
    .filter(x => x.status === "pianificata")
    .reduce((s,x) => s + toNum(x.plannedAmount), 0);
}

function renderCalcBox(o){
  const issued = totalIssued(o);
  const planned = totalPlanned(o);
  const cost = toNum(o.serviceCost);

  const mol = issued - cost;
  const molPct = issued > 0 ? (mol / issued) * 100 : 0;

  const issuedFlag = issued > 0 ? "SÌ" : "NO";
  const plannedFlag = planned > 0 ? "SÌ" : "NO";

  ui.calcBox.innerHTML =
    `<div><b>Fatturata (emessa)?</b> ${issuedFlag}</div>` +
    `<div><b>Fatturato emesso</b>: € ${issued.toFixed(2)}</div>` +
    `<div><b>Fatture pianificate</b>: € ${planned.toFixed(2)} (${plannedFlag})</div>` +
    `<div><b>Costo servizio</b>: € ${cost.toFixed(2)}</div>` +
    `<div><b>MOL (su emesso)</b>: € ${mol.toFixed(2)}</div>` +
    `<div><b>% MOL su emesso</b>: ${molPct.toFixed(1)}%</div>`;
}

function invoiceSortKey(inv){
  const d = inv.status === "pianificata" ? (inv.plannedDate || "") : (inv.date || "");
  return d || inv.createdAt || "";
}

function renderInvoices(invoices){
  ui.invList.innerHTML = "";
  if(!invoices || invoices.length === 0){
    ui.invList.textContent = "Nessuna riga fattura.";
    ui.invList.classList.add("muted");
    return;
  }
  ui.invList.classList.remove("muted");

  const sorted = [...invoices].map(normalizeInvoice).sort((a,b) => invoiceSortKey(b).localeCompare(invoiceSortKey(a)));

  for(const inv of sorted){
    const div = document.createElement("div");
    div.className = "item";

    const left = document.createElement("div");
    const s = document.createElement("strong");

    if(inv.status === "pianificata"){
      const when = inv.plannedDate || "-";
      s.textContent = `📅 Pianificata — € ${toNum(inv.plannedAmount).toFixed(2)}`;
      const meta = document.createElement("div");
      meta.className = "meta";
      meta.textContent = `Data prevista: ${when} • ID: ${inv.id}`;
      left.appendChild(s);
      left.appendChild(meta);
    }else{
      s.textContent = `${inv.number || "(senza numero)"} — € ${toNum(inv.amount).toFixed(2)}`;
      const meta = document.createElement("div");
      meta.className = "meta";
      meta.textContent = `Data: ${inv.date || "-"} • ID: ${inv.id}`;
      left.appendChild(s);
      left.appendChild(meta);
    }

    const right = document.createElement("div");
    right.style.display = "flex";
    right.style.gap = "6px";

    if(inv.status === "pianificata"){
      const mark = document.createElement("button");
      mark.type = "button";
      mark.textContent = "Segna come emessa";
      mark.addEventListener("click", () => markPlannedAsIssued(inv.id));
      right.appendChild(mark);
    }

    const del = document.createElement("button");
    del.type = "button";
    del.textContent = "🗑";
    del.addEventListener("click", () => deleteInvoice(inv.id));
    right.appendChild(del);

    div.appendChild(left);
    div.appendChild(right);
    ui.invList.appendChild(div);
  }
}

function clearNextActionReminder(idx){
  if(idx === undefined || idx === null) return;
  if(!db.opportunities[idx]) return;

  // Azzeriamo la data della prossima azione commerciale
  db.opportunities[idx].nextActionDate = "";

  // Se il form è aperto, aggiorniamo anche il campo a video
  if(ui.nextActionDate){
    ui.nextActionDate.value = "";
  }
}

// ✅ Se esiste una fattura (pianificata o emessa) consideriamo chiusa la parte commerciale:
// annulliamo il reminder eliminando la data della prossima azione commerciale.
function clearNextActionDateForOpp(idx){
  if(idx === undefined || idx === null) return;
  const opp = db.opportunities?.[idx];
  if(!opp) return;

  const hasAnyInvoice = Array.isArray(opp.invoices) && opp.invoices.length > 0;
  if(!hasAnyInvoice) return;

  opp.nextActionDate = "";
  if(ui.nextActionDate) ui.nextActionDate.value = "";
}

function addInvoice(){
  if(!currentOppId){
    alert("Salva prima l’opportunità (almeno una volta), poi aggiungi le fatture.");
    return;
  }

  const kind = ui.invStatus?.value || "emessa";

  const idx = db.opportunities.findIndex(x => x.id === currentOppId);
  if(idx === -1) return;

  // assicura array fatture e normalizza
  db.opportunities[idx].invoices = (db.opportunities[idx].invoices || []).map(normalizeInvoice);

  if(kind === "pianificata"){
    const plannedDate = ui.invPlannedDate.value;
    const plannedAmount = toNum(ui.invPlannedAmount.value);

    if(!plannedDate && plannedAmount === 0){
      alert("Per una fattura pianificata compila almeno Data prevista o Importo previsto.");
      return;
    }

    db.opportunities[idx].invoices.push(normalizeInvoice({
      id: uid(),
      status: "pianificata",
      plannedDate,
      plannedAmount,
      createdAt: nowIso(),
      updatedAt: nowIso(),
    }));

    // pulizia campi pianificata
    ui.invPlannedDate.value = "";
    ui.invPlannedAmount.value = "";
  } else {
    const number = ui.invNumber.value.trim();
    const date = ui.invDate.value;
    const amount = toNum(ui.invAmount.value);

    if(!number && !date && amount === 0){
      alert("Compila almeno uno tra numero, data o importo.");
      return;
    }

    db.opportunities[idx].invoices.push(normalizeInvoice({
      id: uid(),
      status: "emessa",
      number,
      date,
      amount,
      createdAt: nowIso(),
      updatedAt: nowIso(),
    }));

    // pulizia campi emessa
    ui.invNumber.value = "";
    ui.invDate.value = "";
    ui.invAmount.value = "";
  }

  // aggiorna subito lista fatture + calcoli
  clearNextActionDateForOpp(idx);
  renderInvoices(db.opportunities[idx].invoices);

  saveDb();
  renderAll();

  const fresh = db.opportunities.find(x => x.id === currentOppId);
  if(fresh) renderCalcBox(normalizeOpp(fresh));
}

function deleteInvoice(invId){
  const idx = db.opportunities.findIndex(x => x.id === currentOppId);
  if(idx === -1) return;

  db.opportunities[idx].invoices = (db.opportunities[idx].invoices || [])
    .map(normalizeInvoice)
    .filter(x => x.id !== invId);

  saveDb();
  renderAll();
}

function markPlannedAsIssued(invId){
  const idx = db.opportunities.findIndex(x => x.id === currentOppId);
  if(idx === -1) return;

  const opp = normalizeOpp(db.opportunities[idx]);
  const inv = (opp.invoices || []).find(x => x.id === invId);
  if(!inv) return;

  const suggestedAmount = toNum(inv.plannedAmount).toFixed(2);

  const num = prompt("Numero fattura (es. 12/2026):", "");
  if(num === null) return;

  const dt = prompt("Data fattura (YYYY-MM-DD):", todayStr());
  if(dt === null) return;

  const amtStr = prompt("Importo fatturato (€):", suggestedAmount);
  if(amtStr === null) return;
  const amt = toNum(amtStr);

  const newInvoices = opp.invoices.map(x => {
    if(x.id !== invId) return x;
    return normalizeInvoice({
      ...x,
      status: "emessa",
      number: num.trim(),
      date: (dt || "").trim(),
      amount: amt,
      updatedAt: nowIso(),
    });
  });

  db.opportunities[idx] = normalizeOpp({ ...opp, invoices: newInvoices, updatedAt: nowIso() });

  clearNextActionDateForOpp(idx);

  saveDb();
  renderAll();
}

function saveOpp(e){
  e.preventDefault();

  // ✅ salva/aggiorna rubrica lead prima di salvare l'opportunità
  upsertLeadFromInputs();

  const o = formToOpp();
  if(!o.createdAt || !o.name){
    alert("Compila almeno Data creazione e Nome opportunità.");
    return;
  }

  const idx = db.opportunities.findIndex(x => x.id === o.id);
  if(idx === -1){
    db.opportunities.push(o);
  }else{
    // preserva fatture esistenti
    o.invoices = db.opportunities[idx].invoices || [];
    db.opportunities[idx] = o;
  }
  
// ✅ Se esiste almeno una fattura (pianificata o emessa), annulla il reminder della prossima azione
  const savedIdx = db.opportunities.findIndex(x => x.id === o.id);
  clearNextActionDateForOpp(savedIdx);

  currentOppId = o.id;
  ui.deleteBtn.disabled = false;
  ui.fileInfo.textContent = `ID opportunità: ${o.id}`;

  saveDb();
  refreshLeadDatalist();
  renderAll();
setFormSnapshot();
updateDirtyHint();
}

function deleteOpp(){
  if(!currentOppId) return;
  const ok = confirm("Vuoi eliminare definitivamente questa opportunità?");
  if(!ok) return;
  db.opportunities = db.opportunities.filter(x => x.id !== currentOppId);
  saveDb();
  renderAll();
  newOpp();
}

function matchesDueFilter(o){
  const f = ui.dueFilter.value;
  if(f === "all") return true;

  const d = o.nextActionDate || "";
  if(!d) return false;

  const today = todayStr();

  if(f === "overdue") return d < today;
  if(f === "today") return d === today;

  if(f === "next7"){
    const t = new Date(today);
    const d7 = new Date(t.getTime() + 7*24*60*60*1000);
    const max = `${d7.getFullYear()}-${pad2(d7.getMonth()+1)}-${pad2(d7.getDate())}`;
    return d >= today && d <= max;
  }

  return true;
}

/* === FILTRO FATTURE PIANIFICATE === */

function addDays(iso, days){
  const d = new Date(iso);
  d.setDate(d.getDate() + days);
  return `${d.getFullYear()}-${pad2(d.getMonth()+1)}-${pad2(d.getDate())}`;
}

function matchesPlannedInvoiceFilter(o){
  const f = ui.invPlannedFilter?.value || "all";
  if(f === "all") return true;

  const plannedDates = (o.invoices || [])
    .map(normalizeInvoice)
    .filter(i => i.status === "pianificata" && i.plannedDate)
    .map(i => i.plannedDate);

  if(plannedDates.length === 0) return false;

  function monthStartEnd(offsetMonths){
    const d = new Date();
    d.setDate(1);
    d.setMonth(d.getMonth() + offsetMonths);
    const y = d.getFullYear();
    const m = d.getMonth();
    const start = `${y}-${pad2(m+1)}-01`;
    const endDay = new Date(y, m+1, 0).getDate();
    const end = `${y}-${pad2(m+1)}-${pad2(endDay)}`;
    return { start, end };
  }

  if(f === "thisMonth"){
    const { start, end } = monthStartEnd(0);
    return plannedDates.some(d => d >= start && d <= end);
  }

if(f === "lastMonth"){
  const { start, end } = monthStartEnd(-1);
  return plannedDates.some(d => d >= start && d <= end);
}

  if(f === "nextMonth"){
    const { start, end } = monthStartEnd(1);
    return plannedDates.some(d => d >= start && d <= end);
  }

  if(f === "year"){
    const y = new Date().getFullYear();
    const start = `${y}-01-01`;
    const end = `${y}-12-31`;
    return plannedDates.some(d => d >= start && d <= end);
  }

  return true;
}

function matchesIssuedInvoiceFilter(o){
  const f = ui.invIssuedFilter?.value || "all";
  if(f === "all") return true;

  const issuedDates = (o.invoices || [])
    .map(normalizeInvoice)
    .filter(i => i.status === "emessa" && i.date)
    .map(i => i.date);

  if(issuedDates.length === 0) return false;

  function monthStartEnd(offsetMonths){
    const d = new Date();
    d.setDate(1);
    d.setMonth(d.getMonth() + offsetMonths);
    const y = d.getFullYear();
    const m = d.getMonth();
    const start = `${y}-${pad2(m+1)}-01`;
    const endDay = new Date(y, m+1, 0).getDate();
    const end = `${y}-${pad2(m+1)}-${pad2(endDay)}`;
    return { start, end };
  }

  if(f === "thisMonth"){
    const { start, end } = monthStartEnd(0);
    return issuedDates.some(d => d >= start && d <= end);
  }

  if(f === "lastMonth"){
    const { start, end } = monthStartEnd(-1);
    return issuedDates.some(d => d >= start && d <= end);
  }

  if(f === "year"){
    const y = new Date().getFullYear();
    const start = `${y}-01-01`;
    const end = `${y}-12-31`;
    return issuedDates.some(d => d >= start && d <= end);
  }

  return true;
}

function matchesFilters(o){
  const q = ui.q.value.trim().toLowerCase();
  const sf = ui.statusFilter.value;
  const pf = ui.phaseFilter.value;
  const prf = ui.productFilter.value;
  const of = ui.ownerFilter?.value || "";

  if(sf && o.status !== sf) return false;
  if(pf && o.phase !== pf) return false;
  if(prf && o.product !== prf) return false;
  if(of && o.owner !== of) return false;
  if(!matchesDueFilter(o)) return false;
  if(!matchesPlannedInvoiceFilter(o)) return false;
if(!matchesIssuedInvoiceFilter(o)) return false;

  const invText = (o.invoices||[]).map(x => {
    const i = normalizeInvoice(x);
    return [
      i.status, i.plannedDate, i.plannedAmount,
      i.number, i.date, i.amount
    ].join(" ");
  }).join(" ");

  const hay = [
    o.lead, o.name, o.owner, o.status, o.phase, o.product, o.probability,
    o.nextAction, o.nextActionDate, o.notes,
    invText
  ].join(" ").toLowerCase();

  // ✅ se non stai cercando testo, i filtri sopra bastano
  if(!q) return true;

  return hay.includes(q);
}

function renderOwnerStats(all){
  const owners = normalizeSalespeopleList(db.salespeople);
  if(owners.length === 0) return "";

  const rows = owners.map(owner => {
    const mine = all.filter(o => o.owner === owner);
    const open = mine.filter(o => o.status === "aperta").length;
    const suspended = mine.filter(o => o.status === "sospesa").length;
    const won = mine.filter(o => o.status === "chiusa vinta").length;
    const lost = mine.filter(o => ["chiusa persa","abbandonata"].includes(o.status)).length;

    const pipe = mine
      .filter(o => ["aperta","sospesa"].includes(o.status))
      .reduce((s,o) => s + toNum(o.valueExpected), 0);

    const pipeW = mine
      .filter(o => ["aperta","sospesa"].includes(o.status))
      .reduce((s,o) => {
        const val = toNum(o.valueExpected);
        const prob = toNum(String(o.probability || "0").replace("%","")) / 100;
        return s + (val * prob);
      }, 0);

    const issued = mine.reduce((s,o) => s + totalIssued(o), 0);
    const planned = mine.reduce((s,o) => s + totalPlanned(o), 0);

    return { owner, open, suspended, won, lost, pipe, pipeW, issued, planned };
  });

  return `
    <div style="margin-top:10px;"><b>Statistiche per commerciale</b></div>
    <div style="overflow:auto; margin-top:6px;">
      <table class="miniTable">
        <thead>
          <tr>
            <th>Commerciale</th>
            <th>Aperte</th>
            <th>Sospese</th>
            <th>Vinte</th>
            <th>Perse</th>
            <th>Pipe €</th>
            <th>Pipe ponderato €</th>
            <th>Emesso €</th>
            <th>Pianificato €</th>
          </tr>
        </thead>
        <tbody>
          ${rows.map(r => `
            <tr>
              <td>${escapeHtml(r.owner)}</td>
              <td>${r.open}</td>
              <td>${r.suspended}</td>
              <td>${r.won}</td>
              <td>${r.lost}</td>
              <td>€ ${r.pipe.toFixed(2)}</td>
              <td>€ ${r.pipeW.toFixed(2)}</td>
              <td>€ ${r.issued.toFixed(2)}</td>
              <td>€ ${r.planned.toFixed(2)}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderKpi(){
  const all = db.opportunities.map(normalizeOpp);
  const open = all.filter(o => o.status === "aperta").length;
  const suspended = all.filter(o => o.status === "sospesa").length;
const today = todayStr();
const overdueActions = all.filter(o =>
  o.nextActionDate &&
  o.nextActionDate < today &&
  ["aperta","sospesa"].includes(o.status)
).length;

  const wonNotIssued = all.filter(o => o.status === "chiusa vinta" && totalIssued(o) === 0).length;
  const withIssued = all.filter(o => totalIssued(o) > 0).length;

  const totalPipe = all
    .filter(o => ["aperta","sospesa"].includes(o.status))
    .reduce((s,o) => s + toNum(o.valueExpected), 0);

const weightedPipe = all
  .filter(o => ["aperta","sospesa"].includes(o.status))
  .reduce((s,o) => {
    const val = toNum(o.valueExpected);
    const prob = toNum(String(o.probability || "0").replace("%","")) / 100;
    return s + (val * prob);
  }, 0);

  const totalIssuedAll = all.reduce((s,o) => s + totalIssued(o), 0);
  const totalPlannedAll = all.reduce((s,o) => s + totalPlanned(o), 0);

// === KPI CHIUSE VINTE (fatturate / non fatturate) ===
const won = all.filter(o => o.status === "chiusa vinta");

const wonNotInvoiced = won.filter(o => totalIssued(o) === 0);
const wonInvoiced = won.filter(o => totalIssued(o) > 0);

const eurNotInvoiced = wonNotInvoiced.reduce((s,o) => s + totalPlanned(o), 0);
const eurInvoiced = wonInvoiced.reduce((s,o) => s + totalIssued(o), 0);

const eurTotalWon = eurInvoiced + eurNotInvoiced;

const costWon = won.reduce((s,o) => s + toNum(o.serviceCost), 0);

const molWonEur = eurTotalWon - costWon;
const molWonPct = eurTotalWon > 0 ? (molWonEur / eurTotalWon) * 100 : 0;

ui.kpiBox.innerHTML =
  `<div><b>Opportunità aperte</b>: ${open} • <b>Sospese</b>: ${suspended}</div>` +
  `<div><b>Azioni commerciali scadute</b>: ${overdueActions}</div>` +
  `<div><b>Pipeline (valore previsto)</b>: € ${totalPipe.toFixed(2)}</div>` +
  `<div><b>Pipeline ponderato</b>: € ${weightedPipe.toFixed(2)}</div>` +
  `<div><b>Chiuse vinte non fatturate</b>: ${wonNotInvoiced.length}</div>` +
  `<div><b>€ non fatturate</b>: € ${eurNotInvoiced.toFixed(2)}</div>` +
  `<div><b>Chiuse vinte fatturate</b>: ${wonInvoiced.length}</div>` +
  `<div><b>€ fatturate</b>: € ${eurInvoiced.toFixed(2)}</div>` +
  `<div><b>€ totale (fatt. + non fatt.)</b>: € ${eurTotalWon.toFixed(2)}</div>` +
`<div><b>MOL (vinte: emesso + pianificato)</b>: € ${molWonEur.toFixed(2)}</div>` +
`<div><b>% MOL (vinte: emesso + pianificato)</b>: ${molWonPct.toFixed(1)}%</div>` +
  renderOwnerStats(all);
}

function duplicateOpp(id){
  const o = db.opportunities.find(x => x.id === id);
  if(!o) return;

  const base = normalizeOpp(o);

  const copy = normalizeOpp({
    ...base,
    id: uid(),
    name: `${base.name} (copia)`,
    createdAt: todayStr(),     // nuova data creazione
    createdAtTs: nowIso(),
    updatedAt: nowIso(),
    invoices: [],              // ✅ non duplica fatture
  });

  // inserisci in alto e apri subito la copia
  db.opportunities.unshift(copy);
  saveDb();
  renderAll();
  oppToForm(copy);
}

function renderOppList(){
  ui.oppList.innerHTML = "";

  const list = db.opportunities
    .map(normalizeOpp)
    .sort((a,b) => (b.createdAt || "").localeCompare(a.createdAt || ""))
    .filter(matchesFilters);
 
 const total = (db.opportunities || []).length;
  if(ui.oppCounter){
    ui.oppCounter.textContent = `Totali: ${total} • Visibili (con filtri): ${list.length}`;
  }

  if(list.length === 0){
    ui.oppList.textContent = "Nessuna opportunità (con questi filtri).";
    ui.oppList.classList.add("muted");
    return;
  }
  ui.oppList.classList.remove("muted");

  for(const o of list){
    const div = document.createElement("div");
    div.className = "item";

    const left = document.createElement("div");
    const title = document.createElement("strong");
    title.textContent = `${o.name || "(senza nome)"} — ${o.lead || "(lead)"}`;

    const meta = document.createElement("div");
    meta.className = "meta";

    const issued = totalIssued(o);
    const planned = totalPlanned(o);
    const overdue = o.nextActionDate && o.nextActionDate < todayStr() ? "⚠️ azione scaduta" : "";

    meta.textContent =
      `Creata: ${o.createdAt} • Commerciale: ${o.owner || "-"} • Stato: ${o.status} • Fase: ${o.phase} • Prodotto: ${o.product}` +
      ` • Previsto: € ${toNum(o.valueExpected).toFixed(2)}` +
      ` • Emesso: € ${issued.toFixed(2)}` +
      ` • Pianificato: € ${planned.toFixed(2)}` +
      `${overdue ? " • " + overdue : ""}`;

    left.appendChild(title);
left.appendChild(meta);

const btnOpen = document.createElement("button");
btnOpen.type = "button";
btnOpen.textContent = "Apri";
btnOpen.addEventListener("click", () => {
  if(!confirmIfDirty()) return;
  const fresh = db.opportunities.find(x => x.id === o.id);
  if(fresh) oppToForm(normalizeOpp(fresh));
});

const btnDup = document.createElement("button");
btnDup.type = "button";
btnDup.textContent = "Duplica";
btnDup.style.marginLeft = "6px"; // opzionale, solo per separare
btnDup.addEventListener("click", () => {
  duplicateOpp(o.id);
});

div.appendChild(left);
div.appendChild(btnOpen);
div.appendChild(btnDup);
ui.oppList.appendChild(div);
  }
}

function renderAll(){
  renderKpi();
  renderOppList();
  if(currentOppId){
    const fresh = db.opportunities.find(x => x.id === currentOppId);
    if(fresh){
      const norm = normalizeOpp(fresh);
      renderCalcBox(norm);
      renderInvoices(norm.invoices);
    }
  }
}

// Backup
function exportBackup(){
  const ts = new Date();
  const name = `sales-vault-backup_${ts.getFullYear()}-${pad2(ts.getMonth()+1)}-${pad2(ts.getDate())}_${pad2(ts.getHours())}${pad2(ts.getMinutes())}.json`;

  // Nel file backup includiamo quando è stato creato (per poter capire se è "l'ultimo")
  const backupCreatedAt = nowIso();
db.meta.lastBackupExportedAt = backupCreatedAt;

  const payload = JSON.parse(JSON.stringify(ensureDbShape(db)));
  payload.meta = payload.meta || {};
  payload.meta.backupCreatedAt = backupCreatedAt;

  // su questo dispositivo: segna che abbiamo visto un backup aggiornato
  db.meta.latestBackupSeenAt = (db.meta.latestBackupSeenAt && db.meta.latestBackupSeenAt > backupCreatedAt)
    ? db.meta.latestBackupSeenAt
    : backupCreatedAt;
  saveDb();

  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(a.href);
}

function importBackup(file){
  dirtyBypass = true;          // ✅ INIZIO bypass (evita controlli dirty durante l'import)
  const prevLatest = db?.meta?.latestBackupSeenAt || "";
  const r = new FileReader();
  r.onload = () => {
    try{
      const parsed = JSON.parse(r.result);
   if(!parsed || !Array.isArray(parsed.opportunities)){
  alert("File non valido.");
  dirtyBypass = false;
  return;
}
      db = ensureDbShape(parsed);

      // ✅ traccia nome backup importato (quello ora "in uso")
      db.meta.activeBackupName = file?.name || "(backup importato)";
      db.meta.activeBackupImportedAt = nowIso();
      db.meta.activeBackupCreatedAt = String(parsed?.meta?.backupCreatedAt || "");

      // max visto su questo dispositivo (per evidenziare se importi un file vecchio)
      const createdAt = db.meta.activeBackupCreatedAt;
      const latest = [prevLatest, createdAt].filter(Boolean).sort().slice(-1)[0] || "";
      db.meta.latestBackupSeenAt = latest;

      refreshOwnerSelects();
      saveDb();
      renderAll();
refreshLeadDatalist();
            newOpp();
      setFormSnapshot();        // ✅ dopo import e reset form: stato pulito
      alert("Backup importato ✅");
      dirtyBypass = false;      // ✅ FINE bypass
    }catch{
      dirtyBypass = false;      // ✅ FINE bypass anche in errore
      alert("Impossibile leggere il file (JSON non valido).");
    }
  };
  r.readAsText(file);
}

// Promemoria popup
function getOverdueActions(){
  const today = todayStr();
  return db.opportunities
    .map(normalizeOpp)
    .filter(o =>
      o.nextActionDate &&
      o.nextActionDate < today &&
      ["aperta","sospesa"].includes(o.status)
    )
    .sort((a,b) => (a.nextActionDate||"").localeCompare(b.nextActionDate||""));
}

function getOverduePlannedInvoices(){
  const today = todayStr();
  const rows = [];

  for(const o of db.opportunities.map(normalizeOpp)){
    for(const inv of (o.invoices || []).map(normalizeInvoice)){
      if(inv.status !== "pianificata") continue;
      if(!inv.plannedDate) continue;
      if(inv.plannedDate < today){
        rows.push({
          oppId: o.id,
          lead: o.lead,
          name: o.name,
          oppStatus: o.status,
          phase: o.phase,
          invId: inv.id,
          plannedDate: inv.plannedDate,
          plannedAmount: inv.plannedAmount,
        });
      }
    }
  }

  rows.sort((a,b) => (a.plannedDate||"").localeCompare(b.plannedDate||""));
  return rows;
}

function showModalOverdue(){
  const overdueActions = getOverdueActions();
  const overduePlanned = getOverduePlannedInvoices();

  if(overdueActions.length === 0 && overduePlanned.length === 0) return;

  let html = "";

  if(overdueActions.length > 0){
    html += `<div class="sectionTitle">Azioni commerciali scadute</div>`;
    html += overdueActions.map(o => {
      const action = o.nextAction ? o.nextAction : "(nessuna azione descritta)";
      return `<div class="rowline">
        <div><b>${o.lead || "(lead)"}</b> — ${o.name || "(opportunità)"}</div>
        <div class="muted">Azione: ${action}</div>
        <div class="muted">Data azione: <b>${o.nextActionDate}</b> • Stato: ${o.status} • Fase: ${o.phase}</div>
      </div>`;
    }).join("");
  }

  if(overduePlanned.length > 0){
    html += `<div class="sectionTitle">Fatture pianificate scadute</div>`;
    html += overduePlanned.map(r => {
      return `<div class="rowline">
        <div><b>${r.lead || "(lead)"}</b> — ${r.name || "(opportunità)"}</div>
        <div class="muted">Prevista: <b>${r.plannedDate}</b> • Importo: € ${toNum(r.plannedAmount).toFixed(2)}</div>
        <div class="muted">Stato opportunità: ${r.oppStatus} • Fase: ${r.phase}</div>
      </div>`;
    }).join("");
  }

  ui.modalBody.innerHTML = html;
  ui.modal.classList.remove("hidden");
}

function closeModal(){
  ui.modal.classList.add("hidden");
}

// Promemoria backup all'avvio
function showBackupLoadReminder(){
  try{
    // mostralo una sola volta per sessione (finché la scheda resta aperta)
    const key = "sales_vault_backup_reminder_shown";
    if(sessionStorage.getItem(key) === "1") return;

    const importedAt = db?.meta?.activeBackupImportedAt || "";
    const activeCreatedAt = db?.meta?.activeBackupCreatedAt || "";
    const latestSeenAt = db?.meta?.latestBackupSeenAt || "";

    const hasLocalData = (db?.opportunities?.length || 0) > 0;

    // Caso 1: non hai importato un backup (stai lavorando su dati locali)
    const needsImport = !importedAt;

    // Caso 2: hai importato, ma questo dispositivo ha già “visto” un backup più nuovo
    // (es. esportato o importato in passato) -> rischio di aver caricato un file vecchio.
    const importedIsOlderThanLatest = !!activeCreatedAt && !!latestSeenAt && activeCreatedAt < latestSeenAt;

    if(!needsImport && !importedIsOlderThanLatest) return;

    sessionStorage.setItem(key, "1");

    let msg = "";
    if(needsImport){
      msg += `<div class="rowline">
        <div><b>Promemoria backup</b></div>
        <div class="muted">Se stai lavorando da OneDrive/altro PC, importa <b>il backup più aggiornato</b> prima di fare modifiche.</div>
        ${hasLocalData ? `<div class="muted">Nota: su questo browser ci sono già dati locali. Importando un backup li sovrascriverai.</div>` : ""}
      </div>`;
    }
    if(importedIsOlderThanLatest){
      msg += `<div class="rowline">
        <div><b>Attenzione</b>: il backup importato potrebbe non essere l’ultimo.</div>
        <div class="muted">Su questo dispositivo risulta esistere un backup più recente rispetto a quello in uso. Valuta di importare il file più nuovo.</div>
      </div>`;
    }

    msg += `<div style="margin-top:10px; display:flex; gap:8px; flex-wrap:wrap;">
      <button id="modalImportBtn" type="button" style="width:auto;">Importa backup ora</button>
      <button id="modalLaterBtn" type="button" style="width:auto;">Ricordamelo più tardi</button>
    </div>`;

    ui.modalBody.innerHTML = msg;
    ui.modal.classList.remove("hidden");

    // wiring bottoni (il contenuto è appena stato iniettato)
    const bImport = document.getElementById("modalImportBtn");
    const bLater = document.getElementById("modalLaterBtn");
    bImport?.addEventListener("click", () => {
      closeModal();
      ui.importFile?.click();
    });
    bLater?.addEventListener("click", () => closeModal());
  }catch{
    // non bloccare l'app per un promemoria
  }
}

// CSV export (Excel)
function csvEscape(v){
  const s = String(v ?? "");
  if (s.includes(";") || s.includes('"') || s.includes("\n")) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function downloadText(filename, text, mime="text/plain"){
  const blob = new Blob([text], { type: mime });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(a.href);
}

function exportCsv(){
  const ops = db.opportunities.map(normalizeOpp);

  // Opportunità
  const oppHeader = [
    "ID","Lead","DataCreazione","NomeOpportunita","Stato","Fase","Prodotto",
    "ValorePrevisto","Probabilita","ProssimaAzione","DataProssimaAzione",
    "CostoErogazione","EmessoTotale","PianificatoTotale","MOL_su_emesso","MOL_percento_su_emesso"
  ].join(";");

  const oppRows = ops.map(o => {
    const issued = totalIssued(o);
    const planned = totalPlanned(o);
    const cost = toNum(o.serviceCost);
    const mol = issued - cost;
    const molPct = issued > 0 ? (mol / issued) * 100 : 0;

    return [
      o.id, o.lead, o.createdAt, o.name, o.status, o.phase, o.product,
      o.valueExpected, o.probability, o.nextAction, o.nextActionDate,
      cost.toFixed(2), issued.toFixed(2), planned.toFixed(2), mol.toFixed(2), molPct.toFixed(1)
    ].map(csvEscape).join(";");
  });

  downloadText(
    `salesvault_opportunita_${todayStr()}.csv`,
    [oppHeader, ...oppRows].join("\n"),
    "text/csv;charset=utf-8"
  );

  // Fatture emesse
  const issuedHeader = ["OppID","Lead","NomeOpportunita","NumeroFattura","DataFattura","ImportoFatturato"].join(";");
  const issuedRows = [];
  for (const o of ops) {
    for (const inv of (o.invoices || []).map(normalizeInvoice)) {
      if(inv.status !== "emessa") continue;
      issuedRows.push([
        o.id, o.lead, o.name,
        inv.number,
        inv.date,
        toNum(inv.amount).toFixed(2)
      ].map(csvEscape).join(";"));
    }
  }
  downloadText(
    `fatture_emesse_${todayStr()}.csv`,
    [issuedHeader, ...issuedRows].join("\n"),
    "text/csv;charset=utf-8"
  );

  // Fatture pianificate
  const plannedHeader = ["OppID","Lead","NomeOpportunita","DataPrevista","ImportoPrevisto","StatoOpportunita","Fase"].join(";");
  const plannedRows = [];
  for (const o of ops) {
    for (const inv of (o.invoices || []).map(normalizeInvoice)) {
      if(inv.status !== "pianificata") continue;
      plannedRows.push([
        o.id, o.lead, o.name,
        inv.plannedDate,
        toNum(inv.plannedAmount).toFixed(2),
        o.status,
        o.phase
      ].map(csvEscape).join(";"));
    }
  }
  downloadText(
    `fatture_pianificate_${todayStr()}.csv`,
    [plannedHeader, ...plannedRows].join("\n"),
    "text/csv;charset=utf-8"
  );
}

// Eventi
ui.newOppBtn.addEventListener("click", () => {
  if(!confirmIfDirty()) return;
  newOpp();
});

ui.oppForm.addEventListener("submit", saveOpp);
ui.deleteBtn.addEventListener("click", deleteOpp);
ui.addInvBtn.addEventListener("click", addInvoice);

ui.lead.addEventListener("input", () => {
  fillLeadContactFields(ui.lead.value);
});

ui.q.addEventListener("input", renderAll);
ui.statusFilter.addEventListener("change", renderAll);
ui.phaseFilter.addEventListener("change", renderAll);
ui.productFilter.addEventListener("change", renderAll);
if(ui.ownerFilter) ui.ownerFilter.addEventListener("change", renderAll);
ui.oppForm?.addEventListener("input", updateDirtyHint);
ui.oppForm?.addEventListener("change", updateDirtyHint);

if(ui.manageOwnersBtn){
  ui.manageOwnersBtn.addEventListener("click", () => {
    const current = normalizeSalespeopleList(db.salespeople).join(", ");
    const raw = prompt(
      "Inserisci l'elenco commerciali (separati da virgola).\nEsempio: Renato, Clizia, Jelena",
      current
    );
    if(raw === null) return;
    const next = normalizeSalespeopleList(raw.split(","));
    if(next.length === 0){
      alert("Devi inserire almeno un commerciale.");
      return;
    }
    db.salespeople = next;
    refreshOwnerSelects();

    // riallinea il form all'elenco aggiornato
    if(currentOppId){
      const o = db.opportunities.find(x => x.id === currentOppId);
      if(o){
        const norm = normalizeOpp(o);
        if(ui.owner) ui.owner.value = norm.owner;
      }
    }else{
      if(ui.owner) ui.owner.value = db.salespeople[0];
    }

    saveDb();
    renderAll();
  });
}
ui.dueFilter.addEventListener("change", renderAll);
ui.invPlannedFilter.addEventListener("change", renderAll);
ui.invIssuedFilter?.addEventListener("change", renderAll);

ui.exportBtn.addEventListener("click", exportBackup);
ui.importBtn.addEventListener("click", () => ui.importFile.click());
ui.importFile.addEventListener("change", (e) => {
  const f = e.target.files?.[0];
if(!confirmIfDirty("Hai modifiche non salvate. Importando un backup perderai le modifiche correnti. Continuare?")) return;
  if(f) importBackup(f);
  ui.importFile.value = "";
});

ui.exportCsvBtn.addEventListener("click", exportCsv);

ui.ghSyncBtn?.addEventListener("click", () => {
  openGhSetupModal();
});

ui.modalCloseBtn.addEventListener("click", closeModal);
ui.modal.addEventListener("click", (e) => {
  if(e.target === ui.modal) closeModal();
});

window.addEventListener("beforeunload", (e) => {
  const lastExport = db?.meta?.lastBackupExportedAt || "";
  const updated = db?.updatedAt || "";
  const needsBackup = (db?.opportunities?.length || 0) > 0 && updated > lastExport;

  if(isFormDirty() || needsBackup){
    e.preventDefault();
    e.returnValue = ""; // prompt del browser
  }
});

// Init
initSelects();
refreshLeadDatalist();
renderAll();
newOpp();
saveDb();

// ✅ GitHub Sync: se già configurato, scarica i dati aggiornati all'avvio e avvia il polling
if (GH.isConfigured()) {
  setGhStatus("Connessione a GitHub in corso…", "syncing");
  // Pull iniziale: aggiorna i dati E ottiene lo SHA corretto per i push successivi
  ghPullAndMerge(false).then((ok) => {
    if (!ok) {
      // Se il pull fallisce, proviamo comunque a ottenere lo SHA grezzo per non bloccare i push
      GH.pull().then(r => { if (r?.sha) _ghCurrentSha = r.sha; });
    }
    startGhPolling();
  });
} else {
  setGhStatus("GitHub Sync non configurato — clicca \"☁️ GitHub Sync\" per abilitarlo", "info");
}

// ✅ Promemoria: carica il backup più aggiornato quando apri l'app (solo se GitHub non attivo)
if (!GH.isConfigured()) {
  setTimeout(showBackupLoadReminder, 600);
}

// Promemoria: all'avvio + ogni 15 minuti
setTimeout(showModalOverdue, 800);
setInterval(showModalOverdue, 15 * 60 * 1000);
