// Sales Vault – app.js
// Architettura: GitHub come storage nativo (pull→merge→push ad ogni salvataggio)
// Merge multi-utente: last-write-wins per singola opportunità (confronto updatedAt)
// ID progressivo leggibile: OPP-001, OPP-002, …

"use strict";

// ═══════════════════════════════════════════════════════════════
// COSTANTI
// ═══════════════════════════════════════════════════════════════

const STORAGE_KEY    = "sales_vault_v1";   // stesso key vecchio → compatibile con backup
const GH_CONFIG_KEY  = "sales_vault_gh_config";
const GH_SHA_KEY     = "sales_vault_gh_sha";
const DB_VERSION     = 3;

// ═══════════════════════════════════════════════════════════════
// UTILITY
// ═══════════════════════════════════════════════════════════════

function nowIso()  { return new Date().toISOString(); }
function pad2(n)   { return String(n).padStart(2, "0"); }
function pad3(n)   { return String(n).padStart(3, "0"); }
function todayStr(){
  const d = new Date();
  return `${d.getFullYear()}-${pad2(d.getMonth()+1)}-${pad2(d.getDate())}`;
}
function uid(){ return `${Date.now()}_${Math.random().toString(16).slice(2)}`; }
function toNum(v){ const n = Number(v); return Number.isFinite(n) ? n : 0; }
function escapeHtml(s){
  return String(s ?? "")
    .replaceAll("&","&amp;").replaceAll("<","&lt;")
    .replaceAll(">","&gt;").replaceAll('"',"&quot;").replaceAll("'","&#39;");
}
function fmtItDateTime(iso){
  try{ return iso ? new Date(iso).toLocaleString("it-IT") : ""; } catch{ return ""; }
}

// ═══════════════════════════════════════════════════════════════
// ENUMERAZIONI
// ═══════════════════════════════════════════════════════════════

const ENUM = {
  status:       ["(tutti)", "aperta", "sospesa", "chiusa vinta", "chiusa persa", "abbandonata"],
  statusValues: ["aperta", "sospesa", "chiusa vinta", "chiusa persa", "abbandonata"],
  phase:        ["(tutte)",
    "contatto iniziale", "proposta inviata",
    "proposta accettata informalmente - mandare CTR",
    "proposta accettata - attesa CTR",
    "conseguita - non fatturata", "conseguita - fatturata", "persa"],
  phaseValues:  [
    "contatto iniziale", "proposta inviata",
    "proposta accettata informalmente - mandare CTR",
    "proposta accettata - attesa CTR",
    "conseguita - non fatturata", "conseguita - fatturata", "persa"],
  product:      ["(tutti)", "docente presenza", "docente online", "traduzione",
    "interpretariato", "blended presenza", "blended online", "piattaforma",
    "da definire", "altro"],
  productValues:["docente presenza", "docente online", "traduzione",
    "interpretariato", "blended presenza", "blended online", "piattaforma",
    "da definire", "altro"],
  probability:  ["10%", "50%", "90%", "100%"],
};

// ═══════════════════════════════════════════════════════════════
// GITHUB SYNC
// ═══════════════════════════════════════════════════════════════

const GH = {
  getConfig(){
    try { return JSON.parse(localStorage.getItem(GH_CONFIG_KEY) || "null"); }
    catch { return null; }
  },
  saveConfig(cfg) { localStorage.setItem(GH_CONFIG_KEY, JSON.stringify(cfg)); },
  clearConfig()   { localStorage.removeItem(GH_CONFIG_KEY); localStorage.removeItem(GH_SHA_KEY); },
  isConfigured()  { const c = this.getConfig(); return !!(c?.token && c?.owner && c?.repo && c?.path); },

  getSha()        { return localStorage.getItem(GH_SHA_KEY) || null; },
  saveSha(sha)    { if(sha) localStorage.setItem(GH_SHA_KEY, sha); },

  apiUrl(){
    const c = this.getConfig();
    return c ? `https://api.github.com/repos/${c.owner}/${c.repo}/contents/${c.path}` : null;
  },

  _headers(){
    const c = this.getConfig();
    return {
      "Authorization": `Bearer ${c.token}`,
      "Accept": "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
    };
  },

  // Scarica il JSON dal repo. Ritorna { content, sha } oppure null in caso di errore.
  async pull(){
    if(!this.isConfigured()) return null;
    try{
      const res = await fetch(this.apiUrl(), { headers: this._headers() });
      if(res.status === 404) return { content: null, sha: null };
      if(!res.ok){
        const err = await res.json().catch(()=>({}));
        throw new Error(err.message || `HTTP ${res.status}`);
      }
      const data = await res.json();
      const b64 = data.content.replace(/\n/g,"");
      const bytes = new Uint8Array(atob(b64).split("").map(c => c.charCodeAt(0)));
      const content = JSON.parse(new TextDecoder().decode(bytes));
      return { content, sha: data.sha };
    } catch(e){
      this._lastError = e.message || String(e);
      console.warn("GH pull error:", e);
      return null;
    }
  },

  // Carica il JSON su GitHub (crea o aggiorna). sha = null → crea il file.
  async push(jsonData, sha){
    if(!this.isConfigured()) return false;
    try{
      const jsonStr = JSON.stringify(jsonData, null, 2);
      const bytes = new TextEncoder().encode(jsonStr);
      let binary = ""; for(const b of bytes) binary += String.fromCharCode(b);
      const body = {
        message: `Sales Vault sync ${new Date().toISOString()}`,
        content: btoa(binary),
      };
      if(sha) body.sha = sha;
      const res = await fetch(this.apiUrl(), {
        method: "PUT",
        headers: { ...this._headers(), "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if(!res.ok){
        const err = await res.json().catch(()=>({}));
        throw new Error(err.message || `HTTP ${res.status}`);
      }
      const result = await res.json();
      const newSha = result?.content?.sha;
      this.saveSha(newSha);
      return true;
    } catch(e){
      this._lastError = e.message || String(e);
      console.warn("GH push error:", e);
      return false;
    }
  },

  async testConnection(token, owner, repo, path){
    try{
      const repoRes = await fetch(`https://api.github.com/repos/${owner}/${repo}`, {
        headers: { "Authorization":`Bearer ${token}`, "Accept":"application/vnd.github+json", "X-GitHub-Api-Version":"2022-11-28" }
      });
      if(repoRes.status === 401) return { ok:false, message:"Token non valido o scaduto (401)" };
      if(repoRes.status === 403) return { ok:false, message:"Token senza permessi (403)" };
      if(repoRes.status === 404) return { ok:false, message:`Repository "${owner}/${repo}" non trovato (404)` };
      if(!repoRes.ok){ const e=await repoRes.json().catch(()=>({})); return { ok:false, message:e.message||`HTTP ${repoRes.status}` }; }

      const fileRes = await fetch(`https://api.github.com/repos/${owner}/${repo}/contents/${path}`, {
        headers: { "Authorization":`Bearer ${token}`, "Accept":"application/vnd.github+json", "X-GitHub-Api-Version":"2022-11-28" }
      });
      if(fileRes.status === 200) return { ok:true, status:200, message:"File trovato ✓" };
      if(fileRes.status === 404) return { ok:true, status:404, message:"File non ancora creato — verrà creato al primo salvataggio ✓" };
      if(fileRes.status === 403) return { ok:false, message:"Token senza permesso Contents: Read and write (403)" };
      const e2=await fileRes.json().catch(()=>({}));
      return { ok:false, message:e2.message||`HTTP ${fileRes.status}` };
    } catch(e){
      return { ok:false, message:`Errore di rete: ${e.message}` };
    }
  }
};

// ═══════════════════════════════════════════════════════════════
// STATO UI SYNC
// ═══════════════════════════════════════════════════════════════

function setGhStatus(msg, type="info"){
  const el = document.getElementById("ghStatus");
  if(!el) return;
  const icons = { info:"☁️", ok:"✅", error:"⚠️", syncing:"🔄", warn:"⚠️" };
  el.textContent = `${icons[type]||"☁️"} ${msg}`;
  el.className = "ghStatus " + type;
}

// ═══════════════════════════════════════════════════════════════
// DB — struttura, normalizzazione, persistenza locale
// ═══════════════════════════════════════════════════════════════

function defaultDb(){
  return {
    version: DB_VERSION,
    updatedAt: nowIso(),
    nextOppSeqByYear: {},  // ← { "26": 4, "25": 12, … } progressivo per anno
    opportunities: [],
    leads: [],
    salespeople: ["Renato", "Clizia", "Jelena"],
    meta: { lastBackupExportedAt: "" },
  };
}

function normalizeSalespeopleList(list){
  return [...new Set((list||[]).map(x=>String(x??"").trim()).filter(Boolean))]
    .sort((a,b)=>a.localeCompare(b,"it"));
}

/** Estrae le ultime 2 cifre dell'anno da una stringa data YYYY-MM-DD o ISO */
function yearCode(dateStr){
  const s = String(dateStr||"");
  // supporta "2026-03-05" e "2026-03-05T..." e anche solo "26"
  const m = s.match(/^(\d{4})/);
  if(m) return m[1].slice(2); // "2026" → "26"
  return String(new Date().getFullYear()).slice(2);
}

/** Costruisce oppId leggibile dal codice anno e numero progressivo */
function buildOppId(yy, seq){ return `OPP-${yy}-${pad3(seq)}`; }

/**
 * Migrazione automatica + riassegnazione progressivi.
 * - Ordina le opportunità per createdAt (poi createdAtTs come spareggio)
 * - Raggruppa per anno di creazione
 * - Riassegna oppSeq e oppId in ordine crescente per ogni anno
 * - Aggiorna nextOppSeqByYear al massimo+1 per ogni anno
 */
function migrateDb(d){
  if(!d || typeof d !== "object") return defaultDb();
  if(!Array.isArray(d.opportunities)) d.opportunities = [];
  if(!Array.isArray(d.leads)) d.leads = [];
  if(!Array.isArray(d.salespeople) || d.salespeople.length === 0)
    d.salespeople = defaultDb().salespeople;
  d.salespeople = normalizeSalespeopleList(d.salespeople);
  if(!d.meta || typeof d.meta !== "object") d.meta = defaultDb().meta;
  if(typeof d.meta.lastBackupExportedAt !== "string") d.meta.lastBackupExportedAt = "";
  d.version = DB_VERSION;

  // Normalizza nextOppSeqByYear (può arrivare come nextOppSeq numerico da versioni vecchie)
  if(!d.nextOppSeqByYear || typeof d.nextOppSeqByYear !== "object" || Array.isArray(d.nextOppSeqByYear)){
    d.nextOppSeqByYear = {};
  }

  // ── Riassegnazione completa progressivi ──────────────────────
  // Ordina per (createdAt, createdAtTs) per assegnare i numeri
  // in ordine cronologico reale
  const sorted = [...d.opportunities].sort((a, b) => {
    const da = (a.createdAt||"9999") + "|" + (a.createdAtTs||"");
    const db_ = (b.createdAt||"9999") + "|" + (b.createdAtTs||"");
    return da.localeCompare(db_);
  });

  // Contatori progressivi per anno durante la riassegnazione
  const seqByYear = {};

  // Mappa id → { oppSeq, oppId } risultante
  const reassigned = new Map();
  for(const o of sorted){
    const yy  = yearCode(o.createdAt || todayStr());
    seqByYear[yy] = (seqByYear[yy] || 0) + 1;
    const seq = seqByYear[yy];
    reassigned.set(o.id || o, { oppSeq: seq, oppId: buildOppId(yy, seq) });
  }

  // Applica i nuovi ID a tutte le opportunità (nell'ordine originale del db)
  for(const o of d.opportunities){
    const r = reassigned.get(o.id || o);
    if(r){ o.oppSeq = r.oppSeq; o.oppId = r.oppId; }
  }

  // nextOppSeqByYear = massimo assegnato + 1 per ogni anno
  for(const [yy, seq] of Object.entries(seqByYear)){
    d.nextOppSeqByYear[yy] = Math.max(
      d.nextOppSeqByYear[yy] || 0,
      seq + 1
    );
  }

  return d;
}

function normalizeInvoice(x){
  const hasIssuedData = !!String(x?.number??"").trim() || !!String(x?.date??"").trim();
  const inferred = x?.status ? x.status : (hasIssuedData ? "emessa" : "pianificata");
  const status = (inferred === "pianificata" || inferred === "emessa") ? inferred : "emessa";
  return {
    id:            x?.id || uid(),
    status,
    plannedDate:   x?.plannedDate || "",
    plannedAmount: toNum(x?.plannedAmount),
    number:        x?.number || "",
    date:          x?.date || "",
    amount:        toNum(x?.amount),
    createdAt:     x?.createdAt || nowIso(),
    updatedAt:     x?.updatedAt || nowIso(),
  };
}

function normalizeOpp(o){
  const inv = Array.isArray(o.invoices) ? o.invoices : [];
  const owners = normalizeSalespeopleList(db?.salespeople);
  const rawOwner = String(o.owner??"").trim();
  // Fallback: se il proprietario salvato non è più nell'elenco, usa Renato (se presente)
  // oppure il primo della lista — MAI sovrascrivere un valore valido già presente
  const defaultOwner = owners.includes("Renato") ? "Renato" : (owners[0]||"");
  const safeOwner = rawOwner && owners.includes(rawOwner) ? rawOwner : defaultOwner;
  return {
    id:             o.id || uid(),
    oppSeq:         o.oppSeq || 0,
    oppId:          o.oppId || (o.oppSeq ? buildOppId(yearCode(o.createdAt||todayStr()), o.oppSeq) : ""),
    lead:           o.lead || "",
    createdAt:      o.createdAt || todayStr(),
    owner:          safeOwner,
    name:           o.name || "",
    status:         o.status || "aperta",
    phase:          o.phase || "contatto iniziale",
    product:        o.product || "da definire",
    valueExpected:  toNum(o.valueExpected),
    probability:    o.probability || "50%",
    nextAction:     o.nextAction || "",
    nextActionDate: o.nextActionDate || "",
    notes:          o.notes || "",
    serviceCost:    toNum(o.serviceCost),
    invoices:       inv.map(normalizeInvoice),
    createdAtTs:    o.createdAtTs || nowIso(),
    updatedAt:      o.updatedAt || nowIso(),
    // campi rubrica
    leadContactName: o.leadContactName || "",
    leadPhone:       o.leadPhone || "",
    leadEmail:       o.leadEmail || "",
    // dati fatturazione cliente
    billName:     o.billName     || "",
    billAddress:  o.billAddress  || "",
    billVat:      o.billVat      || "",
    billCf:       o.billCf       || "",
    billSdi:      o.billSdi      || "",
    billOrderRef: o.billOrderRef || "",
    // righe di dettaglio fattura
    billLines: Array.isArray(o.billLines) ? o.billLines.map(normalizeBillLine) : [],
  };
}

// ── Persistenza locale ────────────────────────────────────────

function loadDbLocal(){
  try{
    const raw = localStorage.getItem(STORAGE_KEY);
    if(!raw) return defaultDb();
    const parsed = JSON.parse(raw);
    if(!parsed || !Array.isArray(parsed.opportunities)) return defaultDb();
    return migrateDb(parsed);
  } catch { return defaultDb(); }
}

function saveDbLocal(){
  db.version   = DB_VERSION;
  db.updatedAt = nowIso();
  localStorage.setItem(STORAGE_KEY, JSON.stringify(db));
}

// ═══════════════════════════════════════════════════════════════
// MERGE MULTI-UTENTE
// ═══════════════════════════════════════════════════════════════

/**
 * Unisce il db remoto con quello locale.
 * Regola: per ogni opportunità vince quella con updatedAt più recente.
 * Le opportunità presenti solo in locale vengono mantenute.
 * Il nextOppSeq viene tenuto al massimo tra locale e remoto.
 */
function mergeDb(local, remote){
  const merged = { ...remote };

  // Indice locale per id interno
  const localById = new Map(local.opportunities.map(o => [o.id, o]));
  // Indice remoto per id interno
  const remoteById = new Map(remote.opportunities.map(o => [o.id, o]));

  const resultMap = new Map();

  // Tutte le opportunità remote
  for(const ro of remote.opportunities){
    const lo = localById.get(ro.id);
    if(!lo){
      resultMap.set(ro.id, ro);
    } else {
      // Vince l'ultima modifica
      const rTs = ro.updatedAt || "";
      const lTs = lo.updatedAt || "";
      resultMap.set(ro.id, lTs > rTs ? lo : ro);
    }
  }

  // Opportunità locali non ancora sul remote (appena create, non ancora pushate)
  for(const lo of local.opportunities){
    if(!remoteById.has(lo.id)){
      resultMap.set(lo.id, lo);
    }
  }

  merged.opportunities = [...resultMap.values()];

  // Merge salespeople: unione dei due set
  const sp = new Set([
    ...normalizeSalespeopleList(local.salespeople),
    ...normalizeSalespeopleList(remote.salespeople),
  ]);
  merged.salespeople = normalizeSalespeopleList([...sp]);

  // Merge leads: vince il più recente per nome
  const leadMap = new Map();
  for(const l of [...(local.leads||[]), ...(remote.leads||[])]){
    const key = String(l.name||"").trim().toLowerCase();
    if(!key) continue;
    const existing = leadMap.get(key);
    if(!existing || (l.updatedAt||"") > (existing.updatedAt||""))
      leadMap.set(key, l);
  }
  merged.leads = [...leadMap.values()];

  // nextOppSeqByYear: per ogni anno prende il massimo tra locale e remoto
  const mergedSeq = { ...(remote.nextOppSeqByYear||{}) };
  for(const [yy, v] of Object.entries(local.nextOppSeqByYear||{})){
    mergedSeq[yy] = Math.max(mergedSeq[yy]||0, v);
  }
  merged.nextOppSeqByYear = mergedSeq;

  // meta: manteniamo il nostro
  merged.meta = local.meta || defaultDb().meta;

  return migrateDb(merged);
}

// ═══════════════════════════════════════════════════════════════
// OPERAZIONI SYNC (pull / push con merge)
// ═══════════════════════════════════════════════════════════════

let _syncLock = false; // evita push concorrenti

async function syncPull(silent = false){
  if(!GH.isConfigured()) return false;
  if(!silent) setGhStatus("Download dal server…", "syncing");

  const result = await GH.pull();
  if(!result){
    const why = GH._lastError ? ` (${GH._lastError})` : "";
    setGhStatus(`Impossibile connettersi a GitHub${why}`, "error");
    return false;
  }

  if(result.content === null){
    // File non esiste ancora su GitHub
    GH.saveSha(null);
    if(!silent) setGhStatus("Pronto — nessun dato remoto ancora", "info");
    return true;
  }

  GH.saveSha(result.sha);
  const remote = migrateDb(result.content);
  db = mergeDb(db, remote);
  saveDbLocal();
  refreshOwnerSelects();
  refreshLeadDatalist();
  renderAll();

  if(!silent) setGhStatus(`Sincronizzato — ${fmtItDateTime(db.updatedAt)}`, "ok");
  return true;
}

/**
 * Flusso completo pull→merge→push usato ad ogni salvataggio.
 * Restituisce true se il push ha avuto successo.
 */
async function syncSave(){
  if(!GH.isConfigured()) return false;
  if(_syncLock){ console.warn("syncSave: lock attivo, skip"); return false; }
  _syncLock = true;

  setGhStatus("Sincronizzazione…", "syncing");

  try{
    // 1. Pull fresco per avere SHA attuale e dati aggiornati
    const result = await GH.pull();

    if(result === null){
      // errore di rete: salva solo in locale, riproveremo
      setGhStatus("Errore di rete — dati salvati in locale", "error");
      return false;
    }

    // 2. Merge con eventuali modifiche remote
    if(result.content !== null){
      GH.saveSha(result.sha);
      const remote = migrateDb(result.content);
      db = mergeDb(db, remote);
      saveDbLocal();
      refreshOwnerSelects();
      refreshLeadDatalist();
      renderAll();
    }

    // 3. Push del db unito
    const sha = result.sha || GH.getSha() || null;
    const ok  = await GH.push(db, sha);

    if(ok){
      setGhStatus(`Salvato — ${new Date().toLocaleTimeString("it-IT")}`, "ok");
      return true;
    } else {
      // Conflitto SHA: riprova una seconda volta con SHA appena letto
      setGhStatus("Risoluzione conflitto SHA…", "syncing");
      const fresh = await GH.pull();
      if(fresh && fresh.sha){
        GH.saveSha(fresh.sha);
        if(fresh.content){
          const rem = migrateDb(fresh.content);
          db = mergeDb(db, rem);
          saveDbLocal();
        }
        const retry = await GH.push(db, fresh.sha);
        if(retry){
          setGhStatus(`Salvato (retry) — ${new Date().toLocaleTimeString("it-IT")}`, "ok");
          return true;
        }
      }
      setGhStatus("Errore nel salvataggio su GitHub", "error");
      return false;
    }
  } finally {
    _syncLock = false;
  }
}

// Polling ogni 30 secondi: scarica aggiornamenti da altri utenti
let _pollInterval = null;
function startPolling(){
  if(_pollInterval) clearInterval(_pollInterval);
  _pollInterval = setInterval(async () => {
    if(!GH.isConfigured() || isFormDirty()) return;
    const result = await GH.pull();
    if(!result) return;
    if(result.content === null) return;
    const currentSha = GH.getSha();
    if(result.sha === currentSha) return; // nessuna novità
    GH.saveSha(result.sha);
    const remote = migrateDb(result.content);
    db = mergeDb(db, remote);
    saveDbLocal();
    refreshOwnerSelects();
    refreshLeadDatalist();
    renderAll();
    setGhStatus(`Aggiornato da remoto — ${new Date().toLocaleTimeString("it-IT")}`, "ok");
  }, 30_000);
}

// ═══════════════════════════════════════════════════════════════
// SALVATAGGIO PRINCIPALE (locale + sync)
// ═══════════════════════════════════════════════════════════════

function saveDb(){
  saveDbLocal();
  ui.status.textContent = `Salvato automaticamente • ${new Date().toLocaleString("it-IT")}`;
  if(GH.isConfigured()) syncSave(); // fire-and-forget
}

// ═══════════════════════════════════════════════════════════════
// UI ELEMENTS
// ═══════════════════════════════════════════════════════════════

const ui = {
  status:           document.getElementById("status"),
  newOppBtn:        document.getElementById("newOppBtn"),
  exportBtn:        document.getElementById("exportBtn"),
  importBtn:        document.getElementById("importBtn"),
  exportCsvBtn:     document.getElementById("exportCsvBtn"),
  importFile:       document.getElementById("importFile"),
  leadList:         document.getElementById("leadList"),

  q:                document.getElementById("q"),
  statusFilter:     document.getElementById("statusFilter"),
  phaseFilter:      document.getElementById("phaseFilter"),
  productFilter:    document.getElementById("productFilter"),
  ownerFilter:      document.getElementById("ownerFilter"),
  manageOwnersBtn:  document.getElementById("manageOwnersBtn"),
  dueFilter:        document.getElementById("dueFilter"),
  yearFilter:       document.getElementById("yearFilter"),
  invPlannedFilter: document.getElementById("invPlannedFilter"),
  invIssuedFilter:  document.getElementById("invIssuedFilter"),

  kpiBox:           document.getElementById("kpiBox"),
  oppList:          document.getElementById("oppList"),
  oppCounter:       document.getElementById("oppCounter"),

  lead:             document.getElementById("lead"),
  oppForm:          document.getElementById("oppForm"),
  dirtyHint:        document.getElementById("dirtyHint"),
  leadContactName:  document.getElementById("leadContactName"),
  leadPhone:        document.getElementById("leadPhone"),
  leadEmail:        document.getElementById("leadEmail"),
  createdAt:        document.getElementById("createdAt"),
  owner:            document.getElementById("owner"),
  oppName:          document.getElementById("oppName"),
  oppStatus:        document.getElementById("oppStatus"),
  oppPhase:         document.getElementById("oppPhase"),
  product:          document.getElementById("product"),
  probability:      document.getElementById("probability"),
  valueExpected:    document.getElementById("valueExpected"),
  serviceCost:      document.getElementById("serviceCost"),
  nextAction:       document.getElementById("nextAction"),
  nextActionDate:   document.getElementById("nextActionDate"),
  notes:            document.getElementById("notes"),

  invStatus:        document.getElementById("invStatus"),
  invPlannedDate:   document.getElementById("invPlannedDate"),
  invPlannedAmount: document.getElementById("invPlannedAmount"),
  invNumber:        document.getElementById("invNumber"),
  invDate:          document.getElementById("invDate"),
  invAmount:        document.getElementById("invAmount"),
  addInvBtn:        document.getElementById("addInvBtn"),
  invList:          document.getElementById("invList"),
  calcBox:          document.getElementById("calcBox"),

  saveBtn:          document.getElementById("saveBtn"),
  deleteBtn:        document.getElementById("deleteBtn"),
  fileInfo:         document.getElementById("fileInfo"),

  // dati fatturazione cliente
  billName:         document.getElementById("billName"),
  billAddress:      document.getElementById("billAddress"),
  billVat:          document.getElementById("billVat"),
  billCf:           document.getElementById("billCf"),
  billSdi:          document.getElementById("billSdi"),
  billOrderRef:     document.getElementById("billOrderRef"),
  // righe dettaglio
  billLinesList:    document.getElementById("billLinesList"),
  blDesc:           document.getElementById("blDesc"),
  blQty:            document.getElementById("blQty"),
  blUnit:           document.getElementById("blUnit"),
  blPrice:          document.getElementById("blPrice"),
  addBillLineBtn:   document.getElementById("addBillLineBtn"),
  billTotalsBox:    document.getElementById("billTotalsBox"),
  printBillBtn:     document.getElementById("printBillBtn"),

  modal:            document.getElementById("modal"),
  modalTitle:       document.getElementById("modalTitle"),
  modalBody:        document.getElementById("modalBody"),
  modalCloseBtn:    document.getElementById("modalCloseBtn"),
  ghSyncBtn:        document.getElementById("ghSyncBtn"),
};

// ═══════════════════════════════════════════════════════════════
// STATO APPLICAZIONE
// ═══════════════════════════════════════════════════════════════

let db            = loadDbLocal();
let currentOppId  = null;
let formSnapshot  = "";
let dirtyBypass   = false;

// ═══════════════════════════════════════════════════════════════
// FORM DIRTY
// ═══════════════════════════════════════════════════════════════

function getFormState(){
  return {
    lead:           ui.lead?.value||"",
    createdAt:      ui.createdAt?.value||"",
    name:           ui.oppName?.value||"",
    status:         ui.oppStatus?.value||"",
    phase:          ui.oppPhase?.value||"",
    product:        ui.product?.value||"",
    probability:    ui.probability?.value||"",
    valueExpected:  ui.valueExpected?.value||"",
    serviceCost:    ui.serviceCost?.value||"",
    nextAction:     ui.nextAction?.value||"",
    nextActionDate: ui.nextActionDate?.value||"",
    notes:          ui.notes?.value||"",
    leadContactName:ui.leadContactName?.value||"",
    leadPhone:      ui.leadPhone?.value||"",
    leadEmail:      ui.leadEmail?.value||"",
  };
}
function setFormSnapshot(){ formSnapshot = JSON.stringify(getFormState()); }
function isFormDirty(){ return !dirtyBypass && JSON.stringify(getFormState()) !== formSnapshot; }
function updateDirtyHint(){
  if(ui.dirtyHint) ui.dirtyHint.style.display = isFormDirty() ? "block" : "none";
}
function confirmIfDirty(msg = "Hai modifiche non salvate. Vuoi uscire senza salvare?"){
  return !isFormDirty() || confirm(msg);
}

// ═══════════════════════════════════════════════════════════════
// SELECTS & DATALIST
// ═══════════════════════════════════════════════════════════════

function setSelectOptions(el, arr){
  el.innerHTML = "";
  for(const v of arr){
    const opt = document.createElement("option");
    opt.value = (v === "(tutti)" || v === "(tutte)") ? "" : v;
    opt.textContent = v;
    el.appendChild(opt);
  }
}

function refreshOwnerSelects(){
  const owners = normalizeSalespeopleList(db.salespeople);
  db.salespeople = owners;
  if(ui.ownerFilter) setSelectOptions(ui.ownerFilter, ["(tutti)", ...owners]);
  if(ui.owner){
    ui.owner.innerHTML = "";
    for(const v of owners){
      const opt = document.createElement("option");
      opt.value = v; opt.textContent = v;
      ui.owner.appendChild(opt);
    }
  }
}

function refreshLeadDatalist(){
  if(!ui.leadList) return;
  ui.leadList.innerHTML = "";
  for(const name of (db.leads||[]).map(l=>l.name).filter(Boolean).sort((a,b)=>a.localeCompare(b,"it"))){
    const opt = document.createElement("option");
    opt.value = name;
    ui.leadList.appendChild(opt);
  }
}

function initSelects(){
  setSelectOptions(ui.statusFilter,  ENUM.status);
  setSelectOptions(ui.phaseFilter,   ENUM.phase);
  setSelectOptions(ui.productFilter, ENUM.product);
  refreshOwnerSelects();

  [
    [ui.oppStatus,   ENUM.statusValues],
    [ui.oppPhase,    ENUM.phaseValues],
    [ui.product,     ENUM.productValues],
    [ui.probability, ENUM.probability],
  ].forEach(([el, vals]) => {
    el.innerHTML = "";
    for(const v of vals){ const o=document.createElement("option"); o.value=v; o.textContent=v; el.appendChild(o); }
  });
}

// ═══════════════════════════════════════════════════════════════
// ID PROGRESSIVO
// ═══════════════════════════════════════════════════════════════

function allocateOppSeq(createdAt){
  const yy = yearCode(createdAt || todayStr());
  if(!d_nextSeqByYear()) db.nextOppSeqByYear = {};

  // Calcola il massimo effettivamente usato per questo anno (sicurezza anti-duplicati)
  let maxUsed = 0;
  for(const o of db.opportunities){
    if(yearCode(o.createdAt) === yy && typeof o.oppSeq === "number")
      maxUsed = Math.max(maxUsed, o.oppSeq);
  }
  const counter = Math.max(db.nextOppSeqByYear[yy] || 1, maxUsed + 1);
  db.nextOppSeqByYear[yy] = counter + 1;

  const seq   = counter;
  const oppId = buildOppId(yy, seq);
  return { oppSeq: seq, oppId };
}

/** Helper: restituisce true se nextOppSeqByYear è un oggetto valido */
function d_nextSeqByYear(){ return db.nextOppSeqByYear && typeof db.nextOppSeqByYear === "object" && !Array.isArray(db.nextOppSeqByYear); }

// ═══════════════════════════════════════════════════════════════
// RIGHE DI DETTAGLIO FATTURA
// ═══════════════════════════════════════════════════════════════

function normalizeBillLine(x){
  return {
    id:    x?.id    || uid(),
    desc:  x?.desc  || "",
    qty:   toNum(x?.qty),
    unit:  x?.unit  || "",
    price: toNum(x?.price),
  };
}

// ═══════════════════════════════════════════════════════════════
// LEADS (rubrica)
// ═══════════════════════════════════════════════════════════════

function normLeadKey(name){ return String(name||"").trim().toLowerCase(); }
function getLeadByName(name){
  const key = normLeadKey(name);
  return key ? (db.leads||[]).find(l => normLeadKey(l.name)===key)||null : null;
}
function upsertLeadFromInputs(){
  const name = ui.lead.value.trim();
  if(!name) return;
  if(!Array.isArray(db.leads)) db.leads = [];
  const existing = getLeadByName(name);
  const payload = { name, contactName:ui.leadContactName.value.trim(), phone:ui.leadPhone.value.trim(), email:ui.leadEmail.value.trim(), updatedAt:nowIso(), createdAt:existing?.createdAt||nowIso() };
  if(existing) Object.assign(existing, payload);
  else db.leads.push(payload);
}
function fillLeadContactFields(name){
  const lead = getLeadByName(name);
  ui.leadContactName.value = lead?.contactName||"";
  ui.leadPhone.value       = lead?.phone||"";
  ui.leadEmail.value       = lead?.email||"";
}

/** Aggiorna il menu a tendina "Anno creazione" con gli anni presenti nel db */
function refreshYearFilter(){
  if(!ui.yearFilter) return;
  const current = ui.yearFilter.value;
  const years = [...new Set(
    db.opportunities
      .map(o => (o.createdAt||"").slice(0,4))
      .filter(y => /^\d{4}$/.test(y))
  )].sort((a,b) => b.localeCompare(a)); // più recente prima

  ui.yearFilter.innerHTML = `<option value="all">(tutti gli anni)</option>`;
  for(const y of years){
    const opt = document.createElement("option");
    opt.value = y; opt.textContent = y;
    ui.yearFilter.appendChild(opt);
  }
  // ripristina selezione precedente se ancora valida
  if(years.includes(current)) ui.yearFilter.value = current;
}
// ═══════════════════════════════════════════════════════════════

function oppToForm(o){
  currentOppId = o.id;
  const owners = normalizeSalespeopleList(db.salespeople);
  ui.lead.value           = o.lead;
  fillLeadContactFields(o.lead);
  ui.createdAt.value      = o.createdAt;
  // Usa il commerciale salvato nell'opportunità — non sovrascriverlo mai con il default
  if(ui.owner) ui.owner.value = (o.owner && owners.includes(o.owner)) ? o.owner : (owners.includes("Renato") ? "Renato" : (owners[0]||""));
  ui.oppName.value        = o.name;
  ui.oppStatus.value      = o.status;
  ui.oppPhase.value       = o.phase;
  ui.product.value        = o.product;
  ui.valueExpected.value  = o.valueExpected || "";
  ui.probability.value    = o.probability;
  ui.nextAction.value     = o.nextAction;
  ui.nextActionDate.value = o.nextActionDate || "";
  ui.notes.value          = o.notes;
  ui.serviceCost.value    = o.serviceCost || "";

  // dati fatturazione cliente
  if(ui.billName)     ui.billName.value     = o.billName     || "";
  if(ui.billAddress)  ui.billAddress.value  = o.billAddress  || "";
  if(ui.billVat)      ui.billVat.value      = o.billVat      || "";
  if(ui.billCf)       ui.billCf.value       = o.billCf       || "";
  if(ui.billSdi)      ui.billSdi.value      = o.billSdi      || "";
  if(ui.billOrderRef) ui.billOrderRef.value = o.billOrderRef || "";
  renderBillLines(o.billLines || []);

  renderInvoices(o.invoices);
  renderCalcBox(o);

  ui.deleteBtn.disabled = false;
  const badge = o.oppId ? `<span class="oppid-badge">${escapeHtml(o.oppId)}</span>` : "";
  ui.fileInfo.innerHTML = `${badge}ID interno: ${o.id}`;
  setFormSnapshot();
  updateDirtyHint();
}

function formToOpp(){
  const existing = db.opportunities.find(x => x.id === currentOppId);
  const inv = existing?.invoices || [];
  const seqData = existing ? { oppSeq: existing.oppSeq, oppId: existing.oppId } : allocateOppSeq(ui.createdAt.value || todayStr());
  return normalizeOpp({
    id:             currentOppId || uid(),
    ...seqData,
    lead:           ui.lead.value.trim(),
    createdAt:      ui.createdAt.value,
    owner:          ui.owner?.value || (normalizeSalespeopleList(db.salespeople).includes("Renato") ? "Renato" : (db.salespeople?.[0]||"")),
    name:           ui.oppName.value.trim(),
    status:         ui.oppStatus.value,
    phase:          ui.oppPhase.value,
    product:        ui.product.value,
    valueExpected:  ui.valueExpected.value,
    probability:    ui.probability.value,
    nextAction:     ui.nextAction.value.trim(),
    nextActionDate: ui.nextActionDate.value,
    notes:          ui.notes.value.trim(),
    serviceCost:    ui.serviceCost.value,
    invoices:       inv,
    billLines:      existing?.billLines || [],
    billName:       ui.billName?.value.trim()     || "",
    billAddress:    ui.billAddress?.value.trim()  || "",
    billVat:        ui.billVat?.value.trim()       || "",
    billCf:         ui.billCf?.value.trim()        || "",
    billSdi:        ui.billSdi?.value.trim()       || "",
    billOrderRef:   ui.billOrderRef?.value.trim()  || "",
    createdAtTs:    existing?.createdAtTs || nowIso(),
    updatedAt:      nowIso(),
  });
}

function resetInvoiceInputs(){
  ui.invPlannedDate.value   = "";
  ui.invPlannedAmount.value = "";
  ui.invNumber.value        = "";
  ui.invDate.value          = "";
  ui.invAmount.value        = "";
}

function newOpp(){
  currentOppId = null;
  ui.oppForm.reset();
  ui.createdAt.value    = todayStr();
  // Commerciale di default: Renato (se presente nell'elenco), altrimenti il primo disponibile
  if(ui.owner){
    const owners = normalizeSalespeopleList(db.salespeople);
    ui.owner.value = owners.includes("Renato") ? "Renato" : (owners[0]||"");
  }
  ui.oppStatus.value    = "aperta";
  ui.oppPhase.value     = "contatto iniziale";
  ui.product.value      = "da definire";
  ui.probability.value  = "50%";
  ui.deleteBtn.disabled = true;
  if(ui.invStatus) ui.invStatus.value = "pianificata";
  resetInvoiceInputs();
  // reset billing fields
  if(ui.billName)     ui.billName.value     = "";
  if(ui.billAddress)  ui.billAddress.value  = "";
  if(ui.billVat)      ui.billVat.value      = "";
  if(ui.billCf)       ui.billCf.value       = "";
  if(ui.billSdi)      ui.billSdi.value      = "";
  if(ui.billOrderRef) ui.billOrderRef.value = "";
  renderBillLines([]);
  ui.invList.textContent = "Nessuna riga fattura.";
  ui.invList.classList.add("muted");
  ui.calcBox.textContent = "";
  ui.fileInfo.textContent = "";
  setFormSnapshot();
  updateDirtyHint();
}

// ═══════════════════════════════════════════════════════════════
// RIGHE DETTAGLIO — rendering e gestione
// ═══════════════════════════════════════════════════════════════

function renderBillTotals(lines){
  if(!ui.billTotalsBox) return;
  if(!lines || lines.length === 0){ ui.billTotalsBox.textContent = ""; return; }
  const imponibile = lines.reduce((s,l) => s + toNum(l.qty) * toNum(l.price), 0);
  const iva = imponibile * 0.22;
  const totale = imponibile + iva;
  ui.billTotalsBox.innerHTML =
    `<div><b>Imponibile</b>: € ${imponibile.toFixed(2)}</div>` +
    `<div><b>IVA 22%</b>: € ${iva.toFixed(2)}</div>` +
    `<div><b>Totale fattura</b>: € ${totale.toFixed(2)}</div>`;
}

function renderBillLines(lines){
  if(!ui.billLinesList) return;
  ui.billLinesList.innerHTML = "";
  if(!lines || lines.length === 0){
    ui.billLinesList.textContent = "Nessuna riga inserita.";
    ui.billLinesList.classList.add("muted");
    renderBillTotals([]);
    return;
  }
  ui.billLinesList.classList.remove("muted");
  lines.forEach((l, idx) => {
    const importo = toNum(l.qty) * toNum(l.price);
    const div = document.createElement("div"); div.className = "item";
    const left = document.createElement("div");
    const s = document.createElement("strong");
    s.textContent = l.desc || "(senza descrizione)";
    const meta = document.createElement("div"); meta.className = "meta";
    meta.textContent = `${l.qty} ${l.unit} × € ${toNum(l.price).toFixed(2)} = € ${importo.toFixed(2)}`;
    left.appendChild(s); left.appendChild(meta);
    const del = document.createElement("button"); del.type = "button"; del.textContent = "🗑";
    del.addEventListener("click", () => deleteBillLine(idx));
    div.appendChild(left); div.appendChild(del);
    ui.billLinesList.appendChild(div);
  });
  renderBillTotals(lines);
}

function addBillLine(){
  if(!currentOppId){ alert("Salva prima l'opportunità, poi aggiungi le righe di dettaglio."); return; }
  const desc  = ui.blDesc?.value.trim()  || "";
  const qty   = toNum(ui.blQty?.value);
  const unit  = ui.blUnit?.value.trim()  || "";
  const price = toNum(ui.blPrice?.value);
  if(!desc && qty === 0 && price === 0){ alert("Compila almeno la descrizione o quantità e prezzo."); return; }

  const idx = db.opportunities.findIndex(x => x.id === currentOppId);
  if(idx === -1) return;
  if(!Array.isArray(db.opportunities[idx].billLines)) db.opportunities[idx].billLines = [];
  db.opportunities[idx].billLines.push(normalizeBillLine({ id:uid(), desc, qty, unit, price }));
  db.opportunities[idx].updatedAt = nowIso();

  if(ui.blDesc)  ui.blDesc.value  = "";
  if(ui.blQty)   ui.blQty.value   = "";
  if(ui.blUnit)  ui.blUnit.value  = "";
  if(ui.blPrice) ui.blPrice.value = "";

  renderBillLines(db.opportunities[idx].billLines);
  saveDb();
}

function deleteBillLine(lineIdx){
  const idx = db.opportunities.findIndex(x => x.id === currentOppId);
  if(idx === -1) return;
  db.opportunities[idx].billLines = (db.opportunities[idx].billLines || []).filter((_, i) => i !== lineIdx);
  db.opportunities[idx].updatedAt = nowIso();
  renderBillLines(db.opportunities[idx].billLines);
  saveDb();
}

// ═══════════════════════════════════════════════════════════════
// STAMPA SCHEDA FATTURAZIONE
// ═══════════════════════════════════════════════════════════════

function printBillingSheet(){
  if(!currentOppId){ alert("Apri un'opportunità prima di stampare la scheda."); return; }
  const raw = db.opportunities.find(x => x.id === currentOppId);
  if(!raw){ alert("Opportunità non trovata."); return; }
  const o = normalizeOpp(raw);

  const lines = (o.billLines || []).map(normalizeBillLine);
  const imponibile = lines.reduce((s,l) => s + toNum(l.qty)*toNum(l.price), 0);
  const iva        = imponibile * 0.22;
  const totale     = imponibile + iva;

  const linesHtml = lines.length > 0
    ? `<table>
        <thead><tr>
          <th>Descrizione</th>
          <th style="text-align:right;">Qtà</th>
          <th>Unità</th>
          <th style="text-align:right;">Prezzo unit. €</th>
          <th style="text-align:right;">Importo €</th>
        </tr></thead>
        <tbody>
          ${lines.map(l => {
            const imp = toNum(l.qty)*toNum(l.price);
            return `<tr>
              <td>${escapeHtml(l.desc)}</td>
              <td class="num">${toNum(l.qty)}</td>
              <td>${escapeHtml(l.unit)}</td>
              <td class="num">${toNum(l.price).toFixed(2)}</td>
              <td class="num">${imp.toFixed(2)}</td>
            </tr>`;
          }).join("")}
        </tbody>
      </table>
      <div class="ps-totals">
        <div>Imponibile: <b>€ ${imponibile.toFixed(2)}</b></div>
        <div>IVA 22%: <b>€ ${iva.toFixed(2)}</b></div>
        <div class="tot-final">TOTALE: € ${totale.toFixed(2)}</div>
      </div>`
    : `<p style="color:#888; font-style:italic;">Nessuna riga di dettaglio inserita — compilare in fase di emissione.</p>`;

  const html = `<!DOCTYPE html>
<html lang="it">
<head>
  <meta charset="utf-8"/>
  <title>Scheda Fatturazione — ${escapeHtml(o.oppId)} ${escapeHtml(o.name)}</title>
  <link rel="stylesheet" href="style.css"/>
  <style>
    body { background: white; margin: 0; }
    @media print { .no-print { display:none!important; } }
  </style>
</head>
<body>
<div class="print-sheet">

  <div class="no-print" style="background:#e8f0fe; padding:12px 16px; border-radius:8px; margin-bottom:20px; display:flex; align-items:center; justify-content:space-between;">
    <span style="font-weight:600; color:#1a4a9a;">📋 Scheda pronta — usa Ctrl+P (o ⌘+P su Mac) per stampare o salvare come PDF</span>
    <button onclick="window.print()" style="background:#1a4a9a; color:white; border:none; border-radius:8px; padding:8px 16px; cursor:pointer; font-size:14px;">🖨️ Stampa / Salva PDF</button>
  </div>

  <h1>Scheda Fatturazione</h1>
  <div class="ps-meta">
    Generata il ${new Date().toLocaleDateString("it-IT", {day:"2-digit",month:"long",year:"numeric"})} •
    ${escapeHtml(o.oppId)} — ${escapeHtml(o.name)}
  </div>

  <!-- DATI OPPORTUNITÀ -->
  <div class="ps-section">
    <h2>Riferimento opportunità</h2>
    <div class="ps-row"><span class="ps-label">ID Opportunità</span><span class="ps-value">${escapeHtml(o.oppId)}</span></div>
    <div class="ps-row"><span class="ps-label">Nome opportunità</span><span class="ps-value">${escapeHtml(o.name)}</span></div>
    <div class="ps-row"><span class="ps-label">Cliente (Lead)</span><span class="ps-value">${escapeHtml(o.lead)}</span></div>
    <div class="ps-row"><span class="ps-label">Commerciale</span><span class="ps-value">${escapeHtml(o.owner)}</span></div>
    <div class="ps-row"><span class="ps-label">Prodotto</span><span class="ps-value">${escapeHtml(o.product)}</span></div>
    ${o.notes ? `<div class="ps-row"><span class="ps-label">Note opportunità</span><span class="ps-value">${escapeHtml(o.notes)}</span></div>` : ""}
  </div>

  <!-- DESTINATARIO -->
  <div class="ps-section">
    <h2>Destinatario fattura</h2>
    ${o.billName    ? `<div class="ps-row"><span class="ps-label">Ragione sociale</span><span class="ps-value"><b>${escapeHtml(o.billName)}</b></span></div>` : ""}
    ${o.billAddress ? `<div class="ps-row"><span class="ps-label">Indirizzo</span><span class="ps-value">${escapeHtml(o.billAddress)}</span></div>` : ""}
    ${o.billVat     ? `<div class="ps-row"><span class="ps-label">P.IVA</span><span class="ps-value">${escapeHtml(o.billVat)}</span></div>` : ""}
    ${o.billCf      ? `<div class="ps-row"><span class="ps-label">Codice Fiscale</span><span class="ps-value">${escapeHtml(o.billCf)}</span></div>` : ""}
    ${o.billSdi     ? `<div class="ps-row"><span class="ps-label">Email SDI / PEC</span><span class="ps-value">${escapeHtml(o.billSdi)}</span></div>` : ""}
    ${o.billOrderRef? `<div class="ps-row"><span class="ps-label">Riferimento ordine</span><span class="ps-value"><b>${escapeHtml(o.billOrderRef)}</b></span></div>` : ""}
    ${(!o.billName && !o.billAddress && !o.billVat && !o.billSdi)
      ? `<p style="color:#a00; font-style:italic;">⚠️ Dati destinatario non compilati nel gestionale — inserirli prima di emettere la fattura.</p>` : ""}
  </div>

  <!-- RIGHE DI DETTAGLIO -->
  <div class="ps-section">
    <h2>Dettaglio servizi da fatturare</h2>
    ${linesHtml}
  </div>

  <!-- RIQUADRO DA COMPILARE -->
  <div class="ps-compile">
    <h2>✏️ Da compilare al momento dell'emissione</h2>
    <div class="field"><label>Numero fattura</label><div class="line"></div></div>
    <div class="field"><label>Data fattura</label><div class="line"></div></div>
    <div class="field"><label>Scadenze pagamento</label><div class="line"></div></div>
    <div class="field"><label>Modalità pagamento</label><div class="line" style="flex:2;"></div></div>
    <div class="field"><label>Note aggiuntive</label><div class="line" style="flex:2;"></div></div>
  </div>

  <div class="ps-note">
    Documento interno generato da Sales Vault • ${escapeHtml(o.oppId)} • Non ha valenza fiscale
  </div>

</div>
</body>
</html>`;

  const win = window.open("", "_blank");
  if(!win){ alert("Il browser ha bloccato la finestra popup. Consenti i popup per questo sito."); return; }
  win.document.write(html);
  win.document.close();
}

// ═══════════════════════════════════════════════════════════════
// FATTURE
// ═══════════════════════════════════════════════════════════════

function totalIssued(o){ return (o.invoices||[]).filter(x=>x.status==="emessa").reduce((s,x)=>s+toNum(x.amount),0); }
function totalPlanned(o){ return (o.invoices||[]).filter(x=>x.status==="pianificata").reduce((s,x)=>s+toNum(x.plannedAmount),0); }

function renderCalcBox(o){
  const issued = totalIssued(o), planned = totalPlanned(o), cost = toNum(o.serviceCost);
  const mol = issued - cost, molPct = issued > 0 ? (mol/issued)*100 : 0;
  ui.calcBox.innerHTML =
    `<div><b>Fatturata (emessa)?</b> ${issued>0?"SÌ":"NO"}</div>` +
    `<div><b>Fatturato emesso</b>: € ${issued.toFixed(2)}</div>` +
    `<div><b>Fatture pianificate</b>: € ${planned.toFixed(2)}</div>` +
    `<div><b>Costo servizio</b>: € ${cost.toFixed(2)}</div>` +
    `<div><b>MOL (su emesso)</b>: € ${mol.toFixed(2)}</div>` +
    `<div><b>% MOL su emesso</b>: ${molPct.toFixed(1)}%</div>`;
}

function invoiceSortKey(inv){ return (inv.status==="pianificata" ? inv.plannedDate : inv.date) || inv.createdAt || ""; }

function renderInvoices(invoices){
  ui.invList.innerHTML = "";
  if(!invoices || invoices.length === 0){
    ui.invList.textContent = "Nessuna riga fattura.";
    ui.invList.classList.add("muted");
    return;
  }
  ui.invList.classList.remove("muted");
  const sorted = [...invoices].map(normalizeInvoice).sort((a,b)=>invoiceSortKey(b).localeCompare(invoiceSortKey(a)));
  for(const inv of sorted){
    const div = document.createElement("div"); div.className = "item";
    const left = document.createElement("div");
    const s = document.createElement("strong");
    const meta = document.createElement("div"); meta.className = "meta";
    if(inv.status === "pianificata"){
      s.textContent = `📅 Pianificata — € ${toNum(inv.plannedAmount).toFixed(2)}`;
      meta.textContent = `Data prevista: ${inv.plannedDate||"-"} • ID: ${inv.id}`;
    } else {
      s.textContent = `${inv.number||"(senza numero)"} — € ${toNum(inv.amount).toFixed(2)}`;
      meta.textContent = `Data: ${inv.date||"-"} • ID: ${inv.id}`;
    }
    left.appendChild(s); left.appendChild(meta);
    const right = document.createElement("div"); right.style.cssText = "display:flex;gap:6px;";
    if(inv.status === "pianificata"){
      const btn = document.createElement("button"); btn.type = "button"; btn.textContent = "Segna come emessa";
      btn.addEventListener("click", () => markPlannedAsIssued(inv.id));
      right.appendChild(btn);
    }
    const del = document.createElement("button"); del.type = "button"; del.textContent = "🗑";
    del.addEventListener("click", () => deleteInvoice(inv.id));
    right.appendChild(del);
    div.appendChild(left); div.appendChild(right);
    ui.invList.appendChild(div);
  }
}

function clearNextActionDateForOpp(idx){
  const opp = db.opportunities?.[idx];
  if(!opp) return;
  if(Array.isArray(opp.invoices) && opp.invoices.length > 0){
    opp.nextActionDate = "";
    if(ui.nextActionDate) ui.nextActionDate.value = "";
  }
}

/**
 * Aggiorna automaticamente stato, fase e probabilità in base alle fatture presenti.
 * Regole (in ordine di priorità):
 *   - almeno una fattura EMESSA  → stato "chiusa vinta", fase "conseguita - fatturata",    prob. "100%"
 *   - almeno una fattura PIANIFICATA (e nessuna emessa) → stato "chiusa vinta", fase "conseguita - non fatturata", prob. "100%"
 *   - nessuna fattura → nessuna modifica automatica
 * Aggiorna sia il db che i controlli del form aperto.
 */
function applyInvoiceAutoStatus(idx){
  const opp = db.opportunities?.[idx];
  if(!opp) return;

  const invoices = (opp.invoices||[]).map(normalizeInvoice);
  const hasIssued   = invoices.some(i => i.status === "emessa");
  const hasPlanned  = invoices.some(i => i.status === "pianificata");

  if(!hasIssued && !hasPlanned) return; // nessuna fattura: non toccare nulla

  let newStatus, newPhase;
  if(hasIssued){
    newStatus = "chiusa vinta";
    newPhase  = "conseguita - fatturata";
  } else {
    newStatus = "chiusa vinta";
    newPhase  = "conseguita - non fatturata";
  }
  const newProb = "100%";

  opp.status      = newStatus;
  opp.phase       = newPhase;
  opp.probability = newProb;

  // Aggiorna il form se è l'opportunità correntemente aperta
  if(opp.id === currentOppId){
    if(ui.oppStatus)   ui.oppStatus.value   = newStatus;
    if(ui.oppPhase)    ui.oppPhase.value     = newPhase;
    if(ui.probability) ui.probability.value  = newProb;
  }
}

function addInvoice(){
  if(!currentOppId){ alert("Salva prima l'opportunità, poi aggiungi le fatture."); return; }
  const kind = ui.invStatus?.value || "emessa";
  const idx  = db.opportunities.findIndex(x => x.id === currentOppId);
  if(idx === -1) return;
  db.opportunities[idx].invoices = (db.opportunities[idx].invoices||[]).map(normalizeInvoice);

  if(kind === "pianificata"){
    const plannedDate   = ui.invPlannedDate.value;
    const plannedAmount = toNum(ui.invPlannedAmount.value);
    if(!plannedDate && plannedAmount === 0){ alert("Compila almeno Data prevista o Importo previsto."); return; }
    db.opportunities[idx].invoices.push(normalizeInvoice({ id:uid(), status:"pianificata", plannedDate, plannedAmount, createdAt:nowIso(), updatedAt:nowIso() }));
    ui.invPlannedDate.value = ""; ui.invPlannedAmount.value = "";
  } else {
    const number = ui.invNumber.value.trim(), date = ui.invDate.value, amount = toNum(ui.invAmount.value);
    if(!number && !date && amount === 0){ alert("Compila almeno uno tra numero, data o importo."); return; }
    db.opportunities[idx].invoices.push(normalizeInvoice({ id:uid(), status:"emessa", number, date, amount, createdAt:nowIso(), updatedAt:nowIso() }));
    ui.invNumber.value = ""; ui.invDate.value = ""; ui.invAmount.value = "";
  }

  db.opportunities[idx].updatedAt = nowIso();
  clearNextActionDateForOpp(idx);
  applyInvoiceAutoStatus(idx);
  renderInvoices(db.opportunities[idx].invoices);
  saveDb();
  renderAll();
  const fresh = db.opportunities.find(x => x.id === currentOppId);
  if(fresh) renderCalcBox(normalizeOpp(fresh));
}

function deleteInvoice(invId){
  const idx = db.opportunities.findIndex(x => x.id === currentOppId);
  if(idx === -1) return;
  db.opportunities[idx].invoices = (db.opportunities[idx].invoices||[]).map(normalizeInvoice).filter(x=>x.id!==invId);
  db.opportunities[idx].updatedAt = nowIso();
  applyInvoiceAutoStatus(idx);
  saveDb();
  renderAll();
}

function markPlannedAsIssued(invId){
  const idx = db.opportunities.findIndex(x => x.id === currentOppId);
  if(idx === -1) return;
  const opp = normalizeOpp(db.opportunities[idx]);
  const inv = (opp.invoices||[]).find(x => x.id === invId);
  if(!inv) return;
  const num    = prompt("Numero fattura (es. 12/2026):", ""); if(num === null) return;
  const dt     = prompt("Data fattura (YYYY-MM-DD):", todayStr()); if(dt === null) return;
  const amtStr = prompt("Importo fatturato (€):", toNum(inv.plannedAmount).toFixed(2)); if(amtStr === null) return;
  const newInvoices = opp.invoices.map(x => x.id !== invId ? x : normalizeInvoice({ ...x, status:"emessa", number:num.trim(), date:(dt||"").trim(), amount:toNum(amtStr), updatedAt:nowIso() }));
  db.opportunities[idx] = normalizeOpp({ ...opp, invoices:newInvoices, updatedAt:nowIso() });
  clearNextActionDateForOpp(idx);
  applyInvoiceAutoStatus(idx);
  saveDb();
  renderAll();
}

// ═══════════════════════════════════════════════════════════════
// SALVA / ELIMINA OPPORTUNITÀ
// ═══════════════════════════════════════════════════════════════

function saveOpp(e){
  e.preventDefault();
  upsertLeadFromInputs();
  const o = formToOpp();
  if(!o.createdAt || !o.name){ alert("Compila almeno Data creazione e Nome opportunità."); return; }

  const idx = db.opportunities.findIndex(x => x.id === o.id);
  if(idx === -1){
    db.opportunities.push(o);
  } else {
    o.invoices   = db.opportunities[idx].invoices   || [];
    o.billLines  = db.opportunities[idx].billLines  || [];
    db.opportunities[idx] = o;
  }

  const savedIdx = db.opportunities.findIndex(x => x.id === o.id);
  clearNextActionDateForOpp(savedIdx);
  applyInvoiceAutoStatus(savedIdx);

  currentOppId = o.id;
  ui.deleteBtn.disabled = false;
  const badge = o.oppId ? `<span class="oppid-badge">${escapeHtml(o.oppId)}</span>` : "";
  ui.fileInfo.innerHTML = `${badge}ID interno: ${o.id}`;

  saveDb();
  refreshLeadDatalist();
  renderAll();
  setFormSnapshot();
  updateDirtyHint();
}

function deleteOpp(){
  if(!currentOppId) return;
  if(!confirm("Vuoi eliminare definitivamente questa opportunità?")) return;
  db.opportunities = db.opportunities.filter(x => x.id !== currentOppId);
  saveDb();
  renderAll();
  newOpp();
}

function duplicateOpp(id){
  const o = db.opportunities.find(x => x.id === id);
  if(!o) return;
  const base   = normalizeOpp(o);
  const seqData = allocateOppSeq(todayStr());
  const copy   = normalizeOpp({ ...base, id:uid(), ...seqData, name:`${base.name} (copia)`, createdAt:todayStr(), createdAtTs:nowIso(), updatedAt:nowIso(), invoices:[] });
  db.opportunities.unshift(copy);
  saveDb();
  renderAll();
  oppToForm(copy);
}

// ═══════════════════════════════════════════════════════════════
// FILTRI
// ═══════════════════════════════════════════════════════════════

function matchesDueFilter(o){
  const f = ui.dueFilter.value;
  if(f === "all") return true;
  const d = o.nextActionDate || ""; if(!d) return false;
  const today = todayStr();
  if(f === "overdue") return d < today;
  if(f === "today")   return d === today;
  if(f === "next7"){
    const t = new Date(today); const d7 = new Date(t.getTime()+7*86400000);
    const max = `${d7.getFullYear()}-${pad2(d7.getMonth()+1)}-${pad2(d7.getDate())}`;
    return d >= today && d <= max;
  }
  return true;
}

function monthRange(offset){
  const d = new Date(); d.setDate(1); d.setMonth(d.getMonth()+offset);
  const y=d.getFullYear(), m=d.getMonth();
  return { start:`${y}-${pad2(m+1)}-01`, end:`${y}-${pad2(m+1)}-${pad2(new Date(y,m+1,0).getDate())}` };
}

function matchesPlannedInvoiceFilter(o){
  const f = ui.invPlannedFilter?.value||"all"; if(f==="all") return true;
  const dates = (o.invoices||[]).map(normalizeInvoice).filter(i=>i.status==="pianificata"&&i.plannedDate).map(i=>i.plannedDate);
  if(dates.length===0) return false;
  if(f==="thisMonth"){ const {start,end}=monthRange(0);   return dates.some(d=>d>=start&&d<=end); }
  if(f==="lastMonth"){ const {start,end}=monthRange(-1);  return dates.some(d=>d>=start&&d<=end); }
  if(f==="nextMonth"){ const {start,end}=monthRange(1);   return dates.some(d=>d>=start&&d<=end); }
  if(f==="year"){      const y=new Date().getFullYear();   return dates.some(d=>d>=`${y}-01-01`&&d<=`${y}-12-31`); }
  return true;
}

function matchesIssuedInvoiceFilter(o){
  const f = ui.invIssuedFilter?.value||"all"; if(f==="all") return true;
  const dates = (o.invoices||[]).map(normalizeInvoice).filter(i=>i.status==="emessa"&&i.date).map(i=>i.date);
  if(dates.length===0) return false;
  if(f==="thisMonth"){ const {start,end}=monthRange(0);  return dates.some(d=>d>=start&&d<=end); }
  if(f==="lastMonth"){ const {start,end}=monthRange(-1); return dates.some(d=>d>=start&&d<=end); }
  if(f==="year"){      const y=new Date().getFullYear(); return dates.some(d=>d>=`${y}-01-01`&&d<=`${y}-12-31`); }
  return true;
}

function matchesFilters(o){
  const q   = ui.q.value.trim().toLowerCase();
  const sf  = ui.statusFilter.value;
  const pf  = ui.phaseFilter.value;
  const prf = ui.productFilter.value;
  const of  = ui.ownerFilter?.value||"";
  const yf  = ui.yearFilter?.value||"all";
  if(sf && o.status !== sf)   return false;
  if(pf && o.phase !== pf)    return false;
  if(prf && o.product !== prf) return false;
  if(of && o.owner !== of)    return false;
  if(yf !== "all" && (o.createdAt||"").slice(0,4) !== yf) return false;
  if(!matchesDueFilter(o))             return false;
  if(!matchesPlannedInvoiceFilter(o))  return false;
  if(!matchesIssuedInvoiceFilter(o))   return false;
  if(!q) return true;
  const invText = (o.invoices||[]).map(x=>{const i=normalizeInvoice(x); return [i.status,i.plannedDate,i.plannedAmount,i.number,i.date,i.amount].join(" ");}).join(" ");
  return [o.lead,o.oppId,o.name,o.owner,o.status,o.phase,o.product,o.probability,o.nextAction,o.nextActionDate,o.notes,invText].join(" ").toLowerCase().includes(q);
}

// ═══════════════════════════════════════════════════════════════
// RENDERING
// ═══════════════════════════════════════════════════════════════

function renderOwnerStats(all){
  const owners = normalizeSalespeopleList(db.salespeople);
  if(owners.length === 0) return "";
  const rows = owners.map(owner => {
    const mine = all.filter(o=>o.owner===owner);
    const open = mine.filter(o=>o.status==="aperta").length;
    const susp = mine.filter(o=>o.status==="sospesa").length;
    const won  = mine.filter(o=>o.status==="chiusa vinta").length;
    const lost = mine.filter(o=>["chiusa persa","abbandonata"].includes(o.status)).length;
    const pipe = mine.filter(o=>["aperta","sospesa"].includes(o.status)).reduce((s,o)=>s+toNum(o.valueExpected),0);
    const pipeW= mine.filter(o=>["aperta","sospesa"].includes(o.status)).reduce((s,o)=>s+toNum(o.valueExpected)*(toNum(String(o.probability||"0").replace("%",""))/100),0);
    const issued  = mine.reduce((s,o)=>s+totalIssued(o),0);
    const planned = mine.reduce((s,o)=>s+totalPlanned(o),0);
    return { owner, open, susp, won, lost, pipe, pipeW, issued, planned };
  });
  return `<div style="margin-top:10px;"><b>Statistiche per commerciale</b></div>
    <div style="overflow:auto; margin-top:6px;">
    <table class="miniTable"><thead><tr>
      <th>Commerciale</th><th>Aperte</th><th>Sospese</th><th>Vinte</th><th>Perse</th>
      <th>Pipe €</th><th>Pipe pond. €</th><th>Emesso €</th><th>Pianificato €</th>
    </tr></thead><tbody>
    ${rows.map(r=>`<tr><td>${escapeHtml(r.owner)}</td><td>${r.open}</td><td>${r.susp}</td><td>${r.won}</td><td>${r.lost}</td><td>€ ${r.pipe.toFixed(2)}</td><td>€ ${r.pipeW.toFixed(2)}</td><td>€ ${r.issued.toFixed(2)}</td><td>€ ${r.planned.toFixed(2)}</td></tr>`).join("")}
    </tbody></table></div>`;
}

function renderKpi(){
  const allNorm  = db.opportunities.map(normalizeOpp);
  // Il cruscotto rispecchia esattamente i filtri attivi nella lista
  const all = allNorm.filter(matchesFilters);

  // Etichetta filtri attivi
  const filterLabels = [];
  const yf  = ui.yearFilter?.value||"all";
  const sf  = ui.statusFilter?.value||"";
  const of  = ui.ownerFilter?.value||"";
  const pf  = ui.phaseFilter?.value||"";
  const prf = ui.productFilter?.value||"";
  const iif = ui.invIssuedFilter?.value||"all";
  const ipf = ui.invPlannedFilter?.value||"all";
  if(yf !== "all") filterLabels.push(`Anno: ${yf}`);
  if(sf)  filterLabels.push(`Stato: ${sf}`);
  if(of)  filterLabels.push(`Commerciale: ${of}`);
  if(pf)  filterLabels.push(`Fase: ${pf}`);
  if(prf) filterLabels.push(`Prodotto: ${prf}`);
  if(iif !== "all") filterLabels.push(`Fatture emesse: ${iif}`);
  if(ipf !== "all") filterLabels.push(`Fatture prog.: ${ipf}`);
  const filterNote = filterLabels.length > 0
    ? `<div style="background:#e8f0fe; border-radius:6px; padding:4px 8px; margin-bottom:8px; font-size:12px; color:#1a4a9a;">🔍 Filtri attivi: ${filterLabels.join(" • ")} <span style="color:#555;">(${all.length} su ${allNorm.length} opp.)</span></div>`
    : `<div style="font-size:12px; color:#999; margin-bottom:6px;">Tutte le opportunità (${allNorm.length})</div>`;

  const today = todayStr();
  const open  = all.filter(o=>o.status==="aperta").length;
  const susp  = all.filter(o=>o.status==="sospesa").length;
  const overdue = all.filter(o=>o.nextActionDate && o.nextActionDate<today && ["aperta","sospesa"].includes(o.status)).length;
  const totalPipe  = all.filter(o=>["aperta","sospesa"].includes(o.status)).reduce((s,o)=>s+toNum(o.valueExpected),0);
  const weightedPipe=all.filter(o=>["aperta","sospesa"].includes(o.status)).reduce((s,o)=>s+toNum(o.valueExpected)*(toNum(String(o.probability||"0").replace("%",""))/100),0);
  const won = all.filter(o=>o.status==="chiusa vinta");
  const wonNotInvoiced = won.filter(o=>totalIssued(o)===0);
  const wonInvoiced    = won.filter(o=>totalIssued(o)>0);
  const eurNotInvoiced = wonNotInvoiced.reduce((s,o)=>s+totalPlanned(o),0);
  const eurInvoiced    = wonInvoiced.reduce((s,o)=>s+totalIssued(o),0);
  const eurTotal = eurInvoiced + eurNotInvoiced;
  const costWon  = won.reduce((s,o)=>s+toNum(o.serviceCost),0);
  const molWon   = eurTotal - costWon;
  const molPct   = eurTotal > 0 ? (molWon/eurTotal)*100 : 0;

  ui.kpiBox.innerHTML =
    filterNote +
    `<div><b>Opportunità aperte</b>: ${open} • <b>Sospese</b>: ${susp}</div>` +
    `<div><b>Azioni commerciali scadute</b>: ${overdue}</div>` +
    `<div><b>Pipeline (valore previsto)</b>: € ${totalPipe.toFixed(2)}</div>` +
    `<div><b>Pipeline ponderato</b>: € ${weightedPipe.toFixed(2)}</div>` +
    `<div><b>Chiuse vinte non fatturate</b>: ${wonNotInvoiced.length} (€ ${eurNotInvoiced.toFixed(2)})</div>` +
    `<div><b>Chiuse vinte fatturate</b>: ${wonInvoiced.length} (€ ${eurInvoiced.toFixed(2)})</div>` +
    `<div><b>€ totale chiuse vinte</b>: € ${eurTotal.toFixed(2)}</div>` +
    `<div><b>MOL (chiuse vinte)</b>: € ${molWon.toFixed(2)} — ${molPct.toFixed(1)}%</div>` +
    renderOwnerStats(all);
}

function renderOppList(){
  ui.oppList.innerHTML = "";
  const list = db.opportunities.map(normalizeOpp)
    .sort((a,b)=>(b.createdAt||"").localeCompare(a.createdAt||""))
    .filter(matchesFilters);
  const total = (db.opportunities||[]).length;
  if(ui.oppCounter) ui.oppCounter.textContent = `Totali: ${total} • Visibili: ${list.length}`;
  if(list.length === 0){
    ui.oppList.textContent = "Nessuna opportunità (con questi filtri).";
    ui.oppList.classList.add("muted");
    return;
  }
  ui.oppList.classList.remove("muted");

  for(const o of list){
    const div   = document.createElement("div"); div.className = "item";
    const left  = document.createElement("div");
    const title = document.createElement("strong");

    // Titolo con badge OPP-NNN
    if(o.oppId){
      title.innerHTML = `<span class="oppid-badge">${escapeHtml(o.oppId)}</span>${escapeHtml(o.name||"(senza nome)")} — ${escapeHtml(o.lead||"(lead)")}`;
    } else {
      title.textContent = `${o.name||"(senza nome)"} — ${o.lead||"(lead)"}`;
    }

    const meta  = document.createElement("div"); meta.className = "meta";
    const issued  = totalIssued(o), planned = totalPlanned(o);
    const overdue = o.nextActionDate && o.nextActionDate < todayStr() ? "⚠️ azione scaduta" : "";
    meta.textContent =
      `Creata: ${o.createdAt} • ${o.owner||"-"} • ${o.status} • ${o.phase} • ${o.product}` +
      ` • Prev.: € ${toNum(o.valueExpected).toFixed(2)}` +
      ` • Emesso: € ${issued.toFixed(2)}` +
      ` • Pianif.: € ${planned.toFixed(2)}` +
      (overdue ? ` • ${overdue}` : "");

    left.appendChild(title); left.appendChild(meta);

    const btnOpen = document.createElement("button"); btnOpen.type="button"; btnOpen.textContent="Apri";
    btnOpen.addEventListener("click", () => {
      if(!confirmIfDirty()) return;
      const fresh = db.opportunities.find(x=>x.id===o.id);
      if(fresh) oppToForm(normalizeOpp(fresh));
    });
    const btnDup = document.createElement("button"); btnDup.type="button"; btnDup.textContent="Duplica";
    btnDup.addEventListener("click", () => duplicateOpp(o.id));

    div.appendChild(left); div.appendChild(btnOpen); div.appendChild(btnDup);
    ui.oppList.appendChild(div);
  }
}

function renderAll(){
  refreshYearFilter();
  renderKpi();
  renderOppList();
  if(currentOppId){
    const fresh = db.opportunities.find(x=>x.id===currentOppId);
    if(fresh){ const n=normalizeOpp(fresh); renderCalcBox(n); renderInvoices(n.invoices); }
  }
}

// ═══════════════════════════════════════════════════════════════
// BACKUP (export / import JSON)
// ═══════════════════════════════════════════════════════════════

function exportBackup(){
  const ts   = new Date();
  const name = `sales-vault-backup_${ts.getFullYear()}-${pad2(ts.getMonth()+1)}-${pad2(ts.getDate())}_${pad2(ts.getHours())}${pad2(ts.getMinutes())}.json`;
  const payload = JSON.parse(JSON.stringify(migrateDb(db)));
  payload.meta = payload.meta || {};
  payload.meta.backupCreatedAt = nowIso();
  db.meta.lastBackupExportedAt = payload.meta.backupCreatedAt;
  saveDbLocal();
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type:"application/json" });
  const a = document.createElement("a"); a.href=URL.createObjectURL(blob); a.download=name;
  document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(a.href);
}

function importBackup(file){
  dirtyBypass = true;
  const r = new FileReader();
  r.onload = () => {
    try{
      const parsed = JSON.parse(r.result);
      if(!parsed || !Array.isArray(parsed.opportunities)){ alert("File non valido."); dirtyBypass=false; return; }
      db = migrateDb(parsed);
      refreshOwnerSelects(); saveDbLocal(); renderAll(); refreshLeadDatalist(); newOpp(); setFormSnapshot();
      if(GH.isConfigured()) syncSave(); // carica il backup importato su GitHub
      alert("Backup importato ✅");
    } catch { alert("Impossibile leggere il file (JSON non valido)."); }
    dirtyBypass = false;
  };
  r.readAsText(file);
}

// ═══════════════════════════════════════════════════════════════
// ESPORTA CSV
// ═══════════════════════════════════════════════════════════════

function csvEscape(v){
  const s = String(v??"");
  return (s.includes(";")||s.includes('"')||s.includes("\n")) ? `"${s.replace(/"/g,'""')}"` : s;
}
function downloadText(filename, text, mime="text/plain"){
  const blob=new Blob([text],{type:mime}); const a=document.createElement("a");
  a.href=URL.createObjectURL(blob); a.download=filename;
  document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(a.href);
}

function exportCsv(){
  const ops = db.opportunities.map(normalizeOpp);
  const oppHeader = ["OppID","Lead","DataCreazione","NomeOpportunita","Stato","Fase","Prodotto","ValorePrevisto","Probabilita","ProssimaAzione","DataProssimaAzione","CostoErogazione","EmessoTotale","PianificatoTotale","MOL_su_emesso","MOL_percento_su_emesso"].join(";");
  const oppRows = ops.map(o=>{
    const issued=totalIssued(o), planned=totalPlanned(o), cost=toNum(o.serviceCost);
    const mol=issued-cost, molPct=issued>0?(mol/issued)*100:0;
    return [o.oppId||o.id,o.lead,o.createdAt,o.name,o.status,o.phase,o.product,o.valueExpected,o.probability,o.nextAction,o.nextActionDate,cost.toFixed(2),issued.toFixed(2),planned.toFixed(2),mol.toFixed(2),molPct.toFixed(1)].map(csvEscape).join(";");
  });
  downloadText(`salesvault_opportunita_${todayStr()}.csv`, [oppHeader,...oppRows].join("\n"), "text/csv;charset=utf-8");

  const issuedHeader = ["OppID","Lead","NomeOpportunita","NumeroFattura","DataFattura","ImportoFatturato"].join(";");
  const issuedRows = [];
  for(const o of ops) for(const inv of (o.invoices||[]).map(normalizeInvoice))
    if(inv.status==="emessa") issuedRows.push([o.oppId||o.id,o.lead,o.name,inv.number,inv.date,toNum(inv.amount).toFixed(2)].map(csvEscape).join(";"));
  downloadText(`fatture_emesse_${todayStr()}.csv`, [issuedHeader,...issuedRows].join("\n"), "text/csv;charset=utf-8");

  const plannedHeader = ["OppID","Lead","NomeOpportunita","DataPrevista","ImportoPrevisto","StatoOpportunita","Fase"].join(";");
  const plannedRows = [];
  for(const o of ops) for(const inv of (o.invoices||[]).map(normalizeInvoice))
    if(inv.status==="pianificata") plannedRows.push([o.oppId||o.id,o.lead,o.name,inv.plannedDate,toNum(inv.plannedAmount).toFixed(2),o.status,o.phase].map(csvEscape).join(";"));
  downloadText(`fatture_pianificate_${todayStr()}.csv`, [plannedHeader,...plannedRows].join("\n"), "text/csv;charset=utf-8");
}

// ═══════════════════════════════════════════════════════════════
// PROMEMORIA POPUP (scaduti)
// ═══════════════════════════════════════════════════════════════

function closeModal(){ ui.modal.classList.add("hidden"); }

function showModalOverdue(){
  try{
    const today = todayStr();
    const overdueActions = db.opportunities.map(normalizeOpp)
      .filter(o=>o.nextActionDate && o.nextActionDate<today && ["aperta","sospesa"].includes(o.status))
      .sort((a,b)=>(a.nextActionDate||"").localeCompare(b.nextActionDate||""));
    const overduePlanned = [];
    for(const o of db.opportunities.map(normalizeOpp))
      for(const inv of (o.invoices||[]).map(normalizeInvoice))
        if(inv.status==="pianificata"&&inv.plannedDate&&inv.plannedDate<today)
          overduePlanned.push({...o, invId:inv.id, plannedDate:inv.plannedDate, plannedAmount:inv.plannedAmount});
    overduePlanned.sort((a,b)=>(a.plannedDate||"").localeCompare(b.plannedDate||""));

    if(overdueActions.length===0 && overduePlanned.length===0) return;

    let html = "";
    if(overdueActions.length>0){
      html += `<div class="sectionTitle">Azioni commerciali scadute</div>`;
      html += overdueActions.map(o=>`<div class="rowline"><div><b>${escapeHtml(o.lead||"(lead)")}</b> — ${escapeHtml(o.name||"(opportunità)")} <span class="oppid-badge">${escapeHtml(o.oppId||"")}</span></div><div class="muted">Azione: ${escapeHtml(o.nextAction||"(nessuna azione descritta)")}</div><div class="muted">Data: <b>${o.nextActionDate}</b> • ${o.status} • ${o.phase}</div></div>`).join("");
    }
    if(overduePlanned.length>0){
      html += `<div class="sectionTitle">Fatture pianificate scadute</div>`;
      html += overduePlanned.map(r=>`<div class="rowline"><div><b>${escapeHtml(r.lead||"(lead)")}</b> — ${escapeHtml(r.name||"(opportunità)")} <span class="oppid-badge">${escapeHtml(r.oppId||"")}</span></div><div class="muted">Prevista: <b>${r.plannedDate}</b> • € ${toNum(r.plannedAmount).toFixed(2)}</div><div class="muted">Stato: ${r.status} • ${r.phase}</div></div>`).join("");
    }

    if(ui.modalTitle) ui.modalTitle.textContent = "Promemoria";
    ui.modalBody.innerHTML = html;
    ui.modal.classList.remove("hidden");
  } catch(e){ console.warn("showModalOverdue:", e); }
}

// ═══════════════════════════════════════════════════════════════
// MODAL GITHUB SETUP
// ═══════════════════════════════════════════════════════════════

function buildGhSetupModal(){
  const cfg = GH.getConfig()||{};
  return `<div style="max-width:520px;">
    <p style="margin:0 0 12px; color:#444; font-size:14px;">
      Collega Sales Vault a un file su un repository GitHub privato.
      I dati saranno sincronizzati automaticamente tra tutti gli utenti che usano lo stesso repository.
    </p>
    <div style="background:#f0f7ff; border:1px solid #b8d4f5; border-radius:8px; padding:10px 12px; margin-bottom:14px; font-size:13px; color:#1a4a7a;">
      <b>Come configurare (una volta sola):</b><br>
      1. Crea un account su <a href="https://github.com" target="_blank">github.com</a> (gratis)<br>
      2. Crea un repository <b>privato</b> (es. <code>sales-vault-data</code>)<br>
      3. Vai su <b>Settings → Developer settings → Personal access tokens → Fine-grained tokens</b><br>
      4. Crea un token con permesso <b>Contents: Read and write</b> sul repo<br>
      5. Condividi token + nome repo con i colleghi (ognuno lo inserisce sul proprio PC)
    </div>
    <label style="display:block; margin-bottom:10px; font-size:14px;">
      <span style="display:block; margin-bottom:4px; font-weight:600;">Token GitHub (Personal Access Token)</span>
      <input id="ghToken" type="password" placeholder="github_pat_…" value="${escapeHtml(cfg.token||"")}"
        style="width:100%; box-sizing:border-box; padding:8px; border:1px solid #ccc; border-radius:8px; font-size:14px; font-family:monospace;" />
    </label>
    <label style="display:block; margin-bottom:10px; font-size:14px;">
      <span style="display:block; margin-bottom:4px; font-weight:600;">Username GitHub (owner del repo)</span>
      <input id="ghOwner" type="text" placeholder="es. mario-rossi" value="${escapeHtml(cfg.owner||"")}"
        style="width:100%; box-sizing:border-box; padding:8px; border:1px solid #ccc; border-radius:8px; font-size:14px;" />
    </label>
    <label style="display:block; margin-bottom:10px; font-size:14px;">
      <span style="display:block; margin-bottom:4px; font-weight:600;">Nome repository</span>
      <input id="ghRepo" type="text" placeholder="es. sales-vault-data" value="${escapeHtml(cfg.repo||"")}"
        style="width:100%; box-sizing:border-box; padding:8px; border:1px solid #ccc; border-radius:8px; font-size:14px;" />
    </label>
    <label style="display:block; margin-bottom:14px; font-size:14px;">
      <span style="display:block; margin-bottom:4px; font-weight:600;">Percorso file nel repo</span>
      <input id="ghPath" type="text" placeholder="data/sales-vault.json" value="${escapeHtml(cfg.path||"data/sales-vault.json")}"
        style="width:100%; box-sizing:border-box; padding:8px; border:1px solid #ccc; border-radius:8px; font-size:14px;" />
    </label>
    <div id="ghTestResult" style="min-height:24px; font-size:13px; margin-bottom:10px;"></div>
    <div style="display:flex; gap:8px; flex-wrap:wrap;">
      <button id="ghTestBtn" type="button" style="width:auto; padding:8px 16px;">🔌 Testa connessione</button>
      <button id="ghSaveConfigBtn" type="button" style="width:auto; padding:8px 16px; background:#1a7a3a; color:white; border-color:#1a7a3a;">✅ Salva configurazione</button>
      ${GH.isConfigured() ? `<button id="ghDisconnectBtn" type="button" style="width:auto; padding:8px 16px;" class="danger">Disconnetti</button>` : ""}
    </div>
  </div>`;
}

function openGhSetupModal(){
  if(ui.modalTitle) ui.modalTitle.textContent = "☁️ GitHub Sync";
  ui.modalBody.innerHTML = buildGhSetupModal();
  ui.modal.classList.remove("hidden");

  document.getElementById("ghTestBtn")?.addEventListener("click", async () => {
    const token=document.getElementById("ghToken").value.trim();
    const owner=document.getElementById("ghOwner").value.trim();
    const repo =document.getElementById("ghRepo").value.trim();
    const path =document.getElementById("ghPath").value.trim();
    const res  =document.getElementById("ghTestResult");
    res.textContent = "🔄 Connessione in corso…";
    const r = await GH.testConnection(token, owner, repo, path);
    res.innerHTML = r.ok
      ? `<span style="color:green;">✅ Connessione riuscita! ${r.status===404?"(file verrà creato)":"(file trovato)"}</span>`
      : `<span style="color:red;">❌ Errore: ${escapeHtml(r.message)}</span>`;
  });

  document.getElementById("ghSaveConfigBtn")?.addEventListener("click", async () => {
    const token=document.getElementById("ghToken").value.trim();
    const owner=document.getElementById("ghOwner").value.trim();
    const repo =document.getElementById("ghRepo").value.trim();
    const path =document.getElementById("ghPath").value.trim();
    if(!token||!owner||!repo||!path){ alert("Compila tutti i campi."); return; }
    GH.saveConfig({ token, owner, repo, path });
    closeModal();
    setGhStatus("Configurazione salvata — connessione in corso…", "syncing");
    await syncPull(false);
    startPolling();
  });

  document.getElementById("ghDisconnectBtn")?.addEventListener("click", () => {
    if(!confirm("Disconnettere GitHub Sync? I dati locali rimarranno intatti.")) return;
    GH.clearConfig();
    if(_pollInterval) clearInterval(_pollInterval);
    setGhStatus("Non configurato — modalità locale", "info");
    closeModal();
  });
}

// ═══════════════════════════════════════════════════════════════
// GESTIONE COMMERCIALI
// ═══════════════════════════════════════════════════════════════

function handleManageOwners(){
  const current = normalizeSalespeopleList(db.salespeople).join(", ");
  const raw = prompt("Inserisci l'elenco commerciali (separati da virgola).\nEsempio: Renato, Clizia, Jelena", current);
  if(raw === null) return;
  const next = normalizeSalespeopleList(raw.split(","));
  if(next.length === 0){ alert("Devi inserire almeno un commerciale."); return; }
  db.salespeople = next;
  refreshOwnerSelects();
  if(currentOppId){
    const o = db.opportunities.find(x=>x.id===currentOppId);
    if(o && ui.owner) ui.owner.value = normalizeOpp(o).owner;
  } else {
    if(ui.owner) ui.owner.value = db.salespeople[0];
  }
  saveDb();
  renderAll();
}

// ═══════════════════════════════════════════════════════════════
// EVENT LISTENERS
// ═══════════════════════════════════════════════════════════════

ui.newOppBtn.addEventListener("click", () => { if(!confirmIfDirty()) return; newOpp(); });
ui.oppForm.addEventListener("submit", saveOpp);
ui.deleteBtn.addEventListener("click", deleteOpp);
ui.addInvBtn.addEventListener("click", addInvoice);
ui.addBillLineBtn?.addEventListener("click", addBillLine);
ui.printBillBtn?.addEventListener("click", printBillingSheet);
ui.lead.addEventListener("input", () => fillLeadContactFields(ui.lead.value));

ui.q.addEventListener("input", renderAll);
ui.statusFilter.addEventListener("change", renderAll);
ui.phaseFilter.addEventListener("change", renderAll);
ui.productFilter.addEventListener("change", renderAll);
ui.ownerFilter?.addEventListener("change", renderAll);
ui.yearFilter?.addEventListener("change", renderAll);
ui.dueFilter.addEventListener("change", renderAll);
ui.invPlannedFilter.addEventListener("change", renderAll);
ui.invIssuedFilter?.addEventListener("change", renderAll);
ui.oppForm?.addEventListener("input", updateDirtyHint);
ui.oppForm?.addEventListener("change", updateDirtyHint);

ui.manageOwnersBtn?.addEventListener("click", handleManageOwners);

ui.exportBtn.addEventListener("click", exportBackup);
ui.importBtn.addEventListener("click", () => ui.importFile.click());
ui.importFile.addEventListener("change", (e) => {
  const f = e.target.files?.[0];
  if(!confirmIfDirty("Hai modifiche non salvate. Importando un backup le perderai. Continuare?")) return;
  if(f) importBackup(f);
  ui.importFile.value = "";
});
ui.exportCsvBtn.addEventListener("click", exportCsv);

ui.ghSyncBtn?.addEventListener("click", openGhSetupModal);
ui.modalCloseBtn.addEventListener("click", closeModal);
ui.modal.addEventListener("click", (e) => { if(e.target === ui.modal) closeModal(); });

window.addEventListener("beforeunload", (e) => {
  if(isFormDirty()){ e.preventDefault(); e.returnValue = ""; }
});

// ═══════════════════════════════════════════════════════════════
// INIZIALIZZAZIONE
// ═══════════════════════════════════════════════════════════════

initSelects();
refreshLeadDatalist();
renderAll();
newOpp();

if(GH.isConfigured()){
  setGhStatus("Connessione a GitHub in corso…", "syncing");
  syncPull(false).then(() => startPolling());
} else {
  setGhStatus("GitHub Sync non configurato — clicca \"☁️ GitHub Sync\" per abilitarlo", "info");
  ui.status.textContent = "Pronto (dati locali)";
}

// Promemoria azioni scadute: all'avvio + ogni 15 minuti
setTimeout(showModalOverdue, 1000);
setInterval(showModalOverdue, 15 * 60 * 1000);
