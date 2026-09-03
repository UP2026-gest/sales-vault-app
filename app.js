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

// Formatta un numero come importo in euro con separatore migliaia (punto) e
// decimali (virgola), in modo deterministico. Es. 9375 → "9.375,00".
// Non usa toLocaleString perché in alcuni ambienti non raggruppa le migliaia
// per i numeri sotto le 5 cifre, generando output incoerenti.
function eur(v){
  const num = toNum(v);
  const neg = num < 0;
  const fixed = Math.abs(num).toFixed(2);
  const [intPart, decPart] = fixed.split(".");
  const withSep = intPart.replace(/\B(?=(\d{3})+(?!\d))/g, ".");
  return `${neg ? "-" : ""}${withSep},${decPart}`;
}
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
    meta: { lastBackupExportedAt: "", fiscalYearMigrated2026: false, fiscalReminderLastShown: "", fiscalReminderIgnored: {} },
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

/** Anno di competenza EFFETTIVO di un'opportunità (4 cifre):
 *  usa fiscalYear se impostato, altrimenti l'anno di creazione. */
function fiscalYearOf(o){
  if(o && o.fiscalYear && /^\d{4}$/.test(String(o.fiscalYear))) return String(o.fiscalYear);
  return (o && o.createdAt ? String(o.createdAt).slice(0,4) : String(new Date().getFullYear()));
}

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

  // ── Migrazione una-tantum: anno di competenza sulle opportunità 2026 ──
  // Le opportunità già esistenti create nel 2026 e senza "anno competenza"
  // esplicito vengono marcate come competenza "2026" (una volta sola).
  // Serve a distinguere "confermato 2026" da "mai impostato" per il reminder
  // di fine anno.
  if(!d.meta.fiscalYearMigrated2026){
    (d.opportunities||[]).forEach(o=>{
      const created = (o.createdAt||"").slice(0,4);
      if(created === "2026" && (!o.fiscalYear || !/^\d{4}$/.test(String(o.fiscalYear)))){
        o.fiscalYear = "2026";
      }
    });
    d.meta.fiscalYearMigrated2026 = true;
  }

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
  const defaultOwner = owners.includes("Renato") ? "Renato" : (owners[0]||"");
  // Regola: NON sovrascrivere mai un owner non vuoto, anche se non è più nell'elenco attuale.
  // Il fallback scatta SOLO se il campo è vuoto. Un owner rimosso dall'elenco resta valido
  // sull'opportunità finché non viene modificato manualmente dall'utente.
  const safeOwner = rawOwner || defaultOwner;

  // Coerenza stato ↔ fase per le opportunità PERSE.
  // Se lo stato è "chiusa persa"/"abbandonata", la fase deve essere "persa",
  // e se la fase è "persa" lo stato deve essere uno stato di perdita.
  // Questo garantisce la coerenza anche sui dati caricati da GitHub o importati.
  let _status = o.status || "aperta";
  let _phase  = o.phase  || "contatto iniziale";
  const _lostStatuses = ["chiusa persa", "abbandonata"];
  if(_lostStatuses.includes(_status)){
    _phase = "persa";
  } else if(_phase === "persa"){
    // fase persa ma stato non-perso: allinea lo stato
    _status = "chiusa persa";
  }

  return {
    id:             o.id || uid(),
    oppSeq:         o.oppSeq || 0,
    oppId:          o.oppId || (o.oppSeq ? buildOppId(yearCode(o.createdAt||todayStr()), o.oppSeq) : ""),
    lead:           o.lead || "",
    createdAt:      o.createdAt || todayStr(),
    owner:          safeOwner,
    name:           o.name || "",
    status:         _status,
    phase:          _phase,
    product:        o.product || "da definire",
    valueExpected:  toNum(o.valueExpected),
    probability:    o.probability || "50%",
    nextAction:     o.nextAction || "",
    nextActionDate: o.nextActionDate || "",
    notes:          o.notes || "",
    serviceCost:    toNum(o.serviceCost),
    // Incarico "splittato": cliente paga separatamente UP e il docente.
    // In questi casi il costo di erogazione di UP può legittimamente essere 0
    // (o solo il materiale didattico), quindi NON va segnalato come mancante.
    split:          o.split === true,
    // Anno di competenza della fattura (es. corso 2027 inserito nel 2026).
    // Se vuoto, si assume l'anno di creazione dell'opportunità.
    fiscalYear:     (o.fiscalYear && /^\d{4}$/.test(String(o.fiscalYear))) ? String(o.fiscalYear) : "",
    invoices:       inv.map(normalizeInvoice),
    createdAtTs:    o.createdAtTs || nowIso(),
    updatedAt:      o.updatedAt || nowIso(),
    // campi rubrica
    leadContactName: o.leadContactName || "",
    leadPhone:       o.leadPhone || "",
    leadEmail:       o.leadEmail || "",
    // archivio
    archived: o.archived === true,
    // contratto
    contractNumber: String(o.contractNumber || "").trim(),
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
      let winner = lTs >= rTs ? lo : ro;
      // Protezione owner: se la versione vincente ha owner vuoto ma l'altra ce l'ha,
      // usa quello non vuoto. Previene che un dato corrotto remoto sovrascriva un owner valido.
      const winnerOwner = String(winner.owner || "").trim();
      const otherOwner  = String((winner === lo ? ro : lo).owner || "").trim();
      if(!winnerOwner && otherOwner){
        winner = { ...winner, owner: otherOwner };
      }
      resultMap.set(ro.id, winner);
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
  renderAllSafe();

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
      renderAllSafe();
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
          renderAllSafe();
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
    renderAllSafe();
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
  phaseFilterBox:   document.getElementById("phaseFilterBox"),
  productFilterBox: document.getElementById("productFilterBox"),
  clearFiltersBtn:  document.getElementById("clearFiltersBtn"),
  ownerFilter:      document.getElementById("ownerFilter"),
  manageOwnersBtn:  document.getElementById("manageOwnersBtn"),
  dueFilter:        document.getElementById("dueFilter"),
  yearFilter:       document.getElementById("yearFilter"),
  fiscalYearFilter: document.getElementById("fiscalYearFilter"),
  oppFiscalYear:    document.getElementById("oppFiscalYear"),
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
  // owner: rimosso — gestito dai pill #ownerRadioGroup
  oppName:          document.getElementById("oppName"),
  oppStatus:        document.getElementById("oppStatus"),
  oppPhase:         document.getElementById("oppPhase"),
  product:          document.getElementById("product"),
  probability:      document.getElementById("probability"),
  valueExpected:    document.getElementById("valueExpected"),
  serviceCost:      document.getElementById("serviceCost"),
  oppSplit:         document.getElementById("oppSplit"),
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

  archiveFilter:    document.getElementById("archiveFilter"),

  saveBtn:          document.getElementById("saveBtn"),
  archiveBtn:       document.getElementById("archiveBtn"),
  deleteBtn:        document.getElementById("deleteBtn"),
  restoreBtn:       document.getElementById("restoreBtn"),
  archivedBanner:   document.getElementById("archivedBanner"),
  fileInfo:         document.getElementById("fileInfo"),

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
    fiscalYear:     ui.oppFiscalYear?.value||"",
    serviceCost:    ui.serviceCost?.value||"",
    split:          !!(ui.oppSplit && ui.oppSplit.checked),
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
/**
 * Se il form non è dirty: risolve subito true (procedi).
 * Se è dirty: mostra una modal con tre bottoni e ritorna una Promise<boolean>.
 *   true  = l'utente ha scelto "Esci senza salvare" oppure ha salvato
 *   false = l'utente ha scelto "Annulla"
 */
function confirmIfDirty(){
  if(!isFormDirty()) return Promise.resolve(true);
  return new Promise(resolve => {
    // Usa la modal esistente
    if(ui.modalTitle) ui.modalTitle.textContent = "Modifiche non salvate";
    ui.modalBody.innerHTML = `
      <div style="font-size:15px; margin-bottom:16px;">
        Hai modifiche non salvate su questa opportunità.<br>
        <b>Vuoi salvarla prima di uscire?</b>
      </div>
      <div style="display:flex; gap:10px; flex-wrap:wrap;">
        <button id="_dirtySave"   style="background:#1a7a3a; color:white; border-color:#1a7a3a; width:auto; padding:8px 18px;">💾 Salva</button>
        <button id="_dirtyLeave"  style="width:auto; padding:8px 18px;">Esci senza salvare</button>
        <button id="_dirtyCancel" style="width:auto; padding:8px 18px;" class="danger">Annulla</button>
      </div>`;
    ui.modal.classList.remove("hidden");

    document.getElementById("_dirtySave").onclick = () => {
      closeModal();
      // Salva l'opportunità corrente (simula submit del form)
      if(currentOppId || ui.oppName.value.trim()){
        const evt = new Event("submit", { bubbles:true, cancelable:true });
        ui.oppForm.dispatchEvent(evt);
      }
      resolve(true);
    };
    document.getElementById("_dirtyLeave").onclick = () => {
      closeModal();
      resolve(true);
    };
    document.getElementById("_dirtyCancel").onclick = () => {
      closeModal();
      resolve(false);
    };
  });
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

// ═══════════════════════════════════════════════════════════════
// FILTRI A CASELLE MULTIPLE (Fase, Prodotto)
// Permettono di selezionare più voci insieme (es. "docente presenza" +
// "docente online" per vedere tutti i corsi con docenza). Nessuna casella
// spuntata = nessun filtro (mostra tutte le voci).
// ═══════════════════════════════════════════════════════════════
function buildCheckFilter(boxEl, values, onChange){
  if(!boxEl) return;
  const actions = `<div class="check-actions">
    <button type="button" data-act="all">Tutte</button>
    <button type="button" data-act="none">Nessuna</button>
  </div>`;
  const rows = values.map(v=>`
    <label><input type="checkbox" value="${escapeHtml(v)}"/> ${escapeHtml(v)}</label>
  `).join("");
  boxEl.innerHTML = actions + rows;
  boxEl.querySelectorAll('input[type="checkbox"]').forEach(chk=>{
    chk.addEventListener("change", onChange);
  });
  boxEl.querySelector('[data-act="all"]').addEventListener("click", ()=>{
    boxEl.querySelectorAll('input[type="checkbox"]').forEach(c=>c.checked=true); onChange();
  });
  boxEl.querySelector('[data-act="none"]').addEventListener("click", ()=>{
    boxEl.querySelectorAll('input[type="checkbox"]').forEach(c=>c.checked=false); onChange();
  });
}

// Ritorna l'array dei valori spuntati in un box checkbox.
function getCheckedValues(boxEl){
  if(!boxEl) return [];
  return Array.from(boxEl.querySelectorAll('input[type="checkbox"]:checked')).map(c=>c.value);
}

function refreshOwnerSelects(){
  const owners = normalizeSalespeopleList(db.salespeople);
  db.salespeople = owners;
  if(ui.ownerFilter) setSelectOptions(ui.ownerFilter, ["(tutti)", ...owners]);
  buildOwnerPills(owners);
}

function buildOwnerPills(owners, selectValue){
  const group = document.getElementById("ownerRadioGroup");
  if(!group) return;
  const target = selectValue ||
    group.querySelector("input[type=radio]:checked")?.value ||
    (owners.includes("Renato") ? "Renato" : (owners[0] || ""));
  group.innerHTML = "";
  for(const name of owners){
    const lbl = document.createElement("label");
    lbl.className = "owner-radio-item";
    const inp = document.createElement("input");
    inp.type = "radio"; inp.name = "ownerRadio"; inp.value = name;
    inp.checked = (name === target);
    lbl.appendChild(inp);
    lbl.appendChild(document.createTextNode(name));
    group.appendChild(lbl);
  }
}

function getOwnerFromPills(){
  const group = document.getElementById("ownerRadioGroup");
  if(!group) return "";
  return group.querySelector("input[type=radio]:checked")?.value || "";
}

function setOwnerPill(name){
  const group = document.getElementById("ownerRadioGroup");
  if(!group) return;
  const owners = normalizeSalespeopleList(db.salespeople);
  buildOwnerPills(owners, name);
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
  buildCheckFilter(ui.phaseFilterBox,   ENUM.phaseValues,   renderAll);
  buildCheckFilter(ui.productFilterBox, ENUM.productValues, renderAll);
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

/** Elenco anni da mostrare nei menu competenza: da 2025 all'anno corrente+2,
 *  più eventuali anni già presenti nei dati (creazione o competenza). */
function relevantYears(){
  const set = new Set();
  const nowY = new Date().getFullYear();
  for(let y = 2025; y <= nowY + 2; y++) set.add(String(y));
  (db.opportunities||[]).forEach(o=>{
    const cy = (o.createdAt||"").slice(0,4); if(/^\d{4}$/.test(cy)) set.add(cy);
    if(o.fiscalYear && /^\d{4}$/.test(String(o.fiscalYear))) set.add(String(o.fiscalYear));
  });
  return [...set].sort((a,b)=>b.localeCompare(a)); // più recente prima
}

/** Popola il filtro "Anno competenza fattura". */
function refreshFiscalYearFilter(){
  if(!ui.fiscalYearFilter) return;
  const current = ui.fiscalYearFilter.value;
  const years = relevantYears();
  ui.fiscalYearFilter.innerHTML = `<option value="all">(tutti gli anni)</option>`;
  for(const y of years){
    const opt = document.createElement("option");
    opt.value = y; opt.textContent = y;
    ui.fiscalYearFilter.appendChild(opt);
  }
  if(current && (years.includes(current) || current === "all")) ui.fiscalYearFilter.value = current;
}

/** Popola il menu "Anno competenza fattura" nel form opportunità. */
function refreshOppFiscalYearSelect(){
  if(!ui.oppFiscalYear) return;
  const current = ui.oppFiscalYear.value;
  const years = relevantYears();
  ui.oppFiscalYear.innerHTML = `<option value="">(anno di creazione)</option>`;
  for(const y of years){
    const opt = document.createElement("option");
    opt.value = y; opt.textContent = y;
    ui.oppFiscalYear.appendChild(opt);
  }
  if(current) ui.oppFiscalYear.value = current;
}
// ═══════════════════════════════════════════════════════════════

function oppToForm(o){
  currentOppId = o.id;
  const owners = normalizeSalespeopleList(db.salespeople);
  ui.lead.value           = o.lead;
  fillLeadContactFields(o.lead);
  ui.createdAt.value      = o.createdAt;
  // Usa il commerciale salvato nell'opportunità — pill selezionabile
  { const ownerVal = (o.owner && owners.includes(o.owner)) ? o.owner : (owners.includes("Renato") ? "Renato" : (owners[0]||"")); setOwnerPill(ownerVal); }
  ui.oppName.value        = o.name;
  ui.oppStatus.value      = o.status;
  ui.oppPhase.value       = o.phase;
  ui.product.value        = o.product;
  ui.valueExpected.value  = o.valueExpected || "";
  refreshOppFiscalYearSelect();
  if(ui.oppFiscalYear) ui.oppFiscalYear.value = o.fiscalYear || "";
  ui.probability.value    = o.probability;
  ui.nextAction.value     = o.nextAction;
  ui.nextActionDate.value = o.nextActionDate || "";
  ui.notes.value          = o.notes;
  ui.serviceCost.value    = o.serviceCost || "";
  if(ui.oppSplit) ui.oppSplit.checked = (o.split === true);

  renderInvoices(o.invoices);
  renderCalcBox(o);

  ui.deleteBtn.disabled = false;
  const badge = o.oppId ? `<span class="oppid-badge">${escapeHtml(o.oppId)}</span>` : "";
  ui.fileInfo.innerHTML = `${badge}ID interno: ${o.id}`;

  // Mostra/nasconde banner archiviato e pulsanti
  const isArch = o.archived === true;
  if(ui.archivedBanner) ui.archivedBanner.style.display = isArch ? "block" : "none";
  if(ui.archiveBtn){
    ui.archiveBtn.style.display = isArch ? "none" : "";
    ui.archiveBtn.textContent   = "📦 Archivia";
  }
  if(ui.deleteBtn) ui.deleteBtn.style.display = isArch ? "" : "none";

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
    owner:          getOwnerFromPills() || (normalizeSalespeopleList(db.salespeople).includes("Renato") ? "Renato" : (db.salespeople?.[0]||"")),
    name:           ui.oppName.value.trim(),
    status:         ui.oppStatus.value,
    phase:          ui.oppPhase.value,
    product:        ui.product.value,
    valueExpected:  ui.valueExpected.value,
    fiscalYear:     ui.oppFiscalYear?.value||"",
    probability:    ui.probability.value,
    nextAction:     ui.nextAction.value.trim(),
    nextActionDate: ui.nextActionDate.value,
    notes:          ui.notes.value.trim(),
    serviceCost:    ui.serviceCost.value,
    split:          !!(ui.oppSplit && ui.oppSplit.checked),
    contractNumber: (document.getElementById("contractNumber")?.value || "").trim(),
    invoices:       inv,
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
  { const owners = normalizeSalespeopleList(db.salespeople); setOwnerPill(owners.includes("Renato") ? "Renato" : (owners[0]||"")); }
  ui.oppStatus.value    = "aperta";
  ui.oppPhase.value     = "contatto iniziale";
  ui.product.value      = "da definire";
  ui.probability.value  = "50%";
  refreshOppFiscalYearSelect();
  if(ui.oppFiscalYear) ui.oppFiscalYear.value = "";
  if(ui.oppSplit) ui.oppSplit.checked = false;
  ui.deleteBtn.disabled = true;
  if(ui.archivedBanner) ui.archivedBanner.style.display = "none";
  if(ui.archiveBtn)     ui.archiveBtn.style.display = "";
  if(ui.deleteBtn)      ui.deleteBtn.style.display  = "none";
  if(ui.invStatus) ui.invStatus.value = "pianificata";
  resetInvoiceInputs();
  ui.invList.textContent = "Nessuna riga fattura.";
  ui.invList.classList.add("muted");
  ui.calcBox.textContent = "";
  ui.fileInfo.textContent = "";
  setFormSnapshot();
  updateDirtyHint();
}

// ═══════════════════════════════════════════════════════════════
// FATTURE
// ═══════════════════════════════════════════════════════════════

function totalIssued(o){ return (o.invoices||[]).filter(x=>x.status==="emessa").reduce((s,x)=>s+toNum(x.amount),0); }
function totalPlanned(o){ return (o.invoices||[]).filter(x=>x.status==="pianificata").reduce((s,x)=>s+toNum(x.plannedAmount),0); }

// Base di importo per il MOL di UNA opportunità, a cascata:
//   emesso (se > 0) → pianificato (se > 0) → valore previsto.
// Ritorna { base, label } dove label = "emesso" | "pianificato" | "valore previsto".
function molBaseFor(o){
  const issued = totalIssued(o), planned = totalPlanned(o), expected = toNum(o.valueExpected);
  if(issued > 0)  return { base: issued,   label: "emesso" };
  if(planned > 0) return { base: planned,  label: "pianificato" };
  return { base: expected, label: "valore previsto" };
}

function renderCalcBox(o){
  const issued = totalIssued(o), planned = totalPlanned(o), cost = toNum(o.serviceCost);
  const expected = toNum(o.valueExpected);

  // Base di calcolo del MOL, a cascata (usa l'helper condiviso):
  //   1) fatturato EMESSO se presente  → MOL effettivo
  //   2) altrimenti PIANIFICATO se presente → MOL previsto
  //   3) altrimenti "Valore previsto €" → MOL stimato
  const bd = molBaseFor(o);
  const base = bd.base, baseLabel = bd.label;
  const molKind = bd.label === "emesso" ? "effettivo" : bd.label === "pianificato" ? "previsto" : "stimato";

  const mol = base - cost;
  const molPct = base > 0 ? (mol/base)*100 : 0;

  // Rilevamento discrepanza: se c'è una fattura (emessa o pianificata) il cui
  // totale differisce dal "Valore previsto", segnalo la cosa.
  const invoiceBase = issued > 0 ? issued : planned; // il totale fatture rilevante
  const hasInvoice = (issued > 0 || planned > 0);
  const discrepancy = hasInvoice && expected > 0 && Math.abs(invoiceBase - expected) > 0.005;

  ui.calcBox.innerHTML =
    `<div><b>Fatturata (emessa)?</b> ${issued>0?"SÌ":"NO"}</div>` +
    `<div><b>Fatturato emesso</b>: € ${eur(issued)}</div>` +
    `<div><b>Fatture pianificate</b>: € ${eur(planned)}</div>` +
    `<div><b>Valore previsto</b>: € ${eur(expected)}</div>` +
    `<div><b>Costo servizio</b>: € ${eur(cost)}</div>` +
    `<div style="margin-top:6px;padding-top:6px;border-top:1px solid #e5e8f0;">` +
      `<b>MOL (${molKind})</b>: € ${eur(mol)} — ${molPct.toFixed(1)}%` +
      `<div class="muted" style="font-size:11px;margin-top:2px;">Calcolato su ${baseLabel}: € ${eur(base)}</div>` +
    `</div>` +
    (discrepancy
      ? `<div style="margin-top:8px;padding:8px 10px;background:#fff4ec;border:1px solid #f0c89a;border-radius:8px;color:#c75a00;font-size:12px;">` +
        `⚠ <b>Discrepanza importi</b>: il totale ${issued>0?"emesso":"pianificato"} (€ ${eur(invoiceBase)}) è diverso dal Valore previsto (€ ${eur(expected)}). ` +
        `Differenza: € ${eur(invoiceBase-expected)}. Verifica e allinea i due valori.` +
        `</div>`
      : "");
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

  // Alert discrepanza: se il totale fatture (emesso o, in mancanza, pianificato)
  // differisce dal "Valore previsto", segnalalo così l'utente può allineare i valori.
  if(fresh){
    const iss = totalIssued(fresh), pln = totalPlanned(fresh), exp = toNum(fresh.valueExpected);
    const invBase = iss > 0 ? iss : pln;
    if(exp > 0 && (iss > 0 || pln > 0) && Math.abs(invBase - exp) > 0.005){
      const diff = invBase - exp;
      alert(
        `⚠ Discrepanza importi\n\n` +
        `Totale ${iss>0?"emesso":"pianificato"}: € ${eur(invBase)}\n` +
        `Valore previsto: € ${eur(exp)}\n` +
        `Differenza: € ${eur(diff)}\n\n` +
        `Verifica e allinea il "Valore previsto €" all'importo della fattura, oppure correggi la fattura.`
      );
    }
  }
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
  // Lavora direttamente sul raw db per non perdere owner e altri campi
  const raw = db.opportunities[idx];
  const opp = normalizeOpp(raw);
  const inv = (opp.invoices||[]).find(x => x.id === invId);
  if(!inv) return;
  const num    = prompt("Numero fattura (es. 12/2026):", ""); if(num === null) return;
  const dt     = prompt("Data fattura (YYYY-MM-DD):", todayStr()); if(dt === null) return;
  const amtStr = prompt("Importo fatturato (€):", toNum(inv.plannedAmount).toFixed(2)); if(amtStr === null) return;
  const newInvoices = opp.invoices.map(x => x.id !== invId ? x : normalizeInvoice({
    ...x, status:"emessa", number:num.trim(), date:(dt||"").trim(), amount:toNum(amtStr), updatedAt:nowIso()
  }));
  // Aggiorna solo le fatture e updatedAt — non toccare owner né altri campi
  raw.invoices  = newInvoices;
  raw.updatedAt = nowIso();
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

function archiveOpp(){
  if(!currentOppId) return;
  if(!confirm("Archiviare questa opportunità? Non apparirà più nei conteggi né nella lista normale. Potrai recuperarla con il filtro Archivio.")) return;
  const idx = db.opportunities.findIndex(x => x.id === currentOppId);
  if(idx === -1) return;
  db.opportunities[idx].archived  = true;
  db.opportunities[idx].updatedAt = nowIso();
  saveDb();
  renderAll();
  // Aggiorna banner e pulsanti senza uscire dall'opportunità
  const fresh = db.opportunities.find(x => x.id === currentOppId);
  if(fresh) oppToForm(normalizeOpp(fresh));
}

function restoreOpp(){
  if(!currentOppId) return;
  const idx = db.opportunities.findIndex(x => x.id === currentOppId);
  if(idx === -1) return;
  db.opportunities[idx].archived  = false;
  db.opportunities[idx].updatedAt = nowIso();
  saveDb();
  renderAll();
  const fresh = db.opportunities.find(x => x.id === currentOppId);
  if(fresh) oppToForm(normalizeOpp(fresh));
}

function deleteOpp(){
  if(!currentOppId) return;
  const o = db.opportunities.find(x => x.id === currentOppId);
  if(!o?.archived){
    alert("Puoi eliminare definitivamente solo le opportunità già archiviate.");
    return;
  }
  if(!confirm("ATTENZIONE: eliminazione definitiva e irreversibile. Continuare?")) return;
  if(!confirm("Sei sicuro? I dati non potranno essere recuperati.")) return;
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

// Riepilogo testuale dei filtri attivi (usato nell'intestazione del report).
function activeFilterSummary(){
  const parts = [];
  const yf  = ui.yearFilter?.value||"all";
  const fyf = ui.fiscalYearFilter?.value||"all";
  const sf  = ui.statusFilter?.value||"";
  const of  = ui.ownerFilter?.value||"";
  const phases   = getCheckedValues(ui.phaseFilterBox);
  const products = getCheckedValues(ui.productFilterBox);
  const af  = ui.archiveFilter?.value||"active";
  if(yf !== "all")  parts.push(`Anno creazione ${yf}`);
  if(fyf !== "all") parts.push(`Competenza ${fyf}`);
  if(sf)  parts.push(`Stato: ${sf}`);
  if(of)  parts.push(`Commerciale: ${of}`);
  if(phases.length)   parts.push(`Fase: ${phases.length===1?phases[0]:phases.length+" voci"}`);
  if(products.length) parts.push(`Prodotto: ${products.length===1?products[0]:products.length+" voci"}`);
  if(af === "archived") parts.push("Archiviate");
  if(af === "all")      parts.push("Archiviate incluse");
  return parts.join(" · ");
}

function matchesFilters(o){
  const q   = ui.q.value.trim().toLowerCase();
  const sf  = ui.statusFilter.value;
  const phases   = getCheckedValues(ui.phaseFilterBox);   // array di fasi spuntate
  const products = getCheckedValues(ui.productFilterBox); // array di prodotti spuntati
  const of  = ui.ownerFilter?.value||"";
  const yf  = ui.yearFilter?.value||"all";
  const fyf = ui.fiscalYearFilter?.value||"all";
  const af  = ui.archiveFilter?.value||"active";

  // Filtro archivio — default: mostra solo attive
  if(af === "active"   &&  o.archived) return false;
  if(af === "archived" && !o.archived) return false;
  // af === "all": mostra tutto

  if(sf && o.status !== sf)    return false;
  // Fase/Prodotto: se nessuna casella spuntata → nessun filtro; altrimenti
  // l'opportunità deve rientrare tra le voci selezionate (logica OR).
  if(phases.length   > 0 && !phases.includes(o.phase))     return false;
  if(products.length > 0 && !products.includes(o.product)) return false;
  if(of && o.owner !== of)     return false;
  if(yf !== "all" && (o.createdAt||"").slice(0,4) !== yf) return false;
  if(fyf !== "all" && fiscalYearOf(o) !== fyf) return false;
  if(!matchesDueFilter(o))            return false;
  if(!matchesPlannedInvoiceFilter(o)) return false;
  if(!matchesIssuedInvoiceFilter(o))  return false;
  if(!q) return true;
  const invText = (o.invoices||[]).map(x=>{const i=normalizeInvoice(x); return [i.status,i.plannedDate,i.plannedAmount,i.number,i.date,i.amount].join(" ");}).join(" ");
  return [o.lead,o.oppId,o.name,o.owner,o.status,o.phase,o.product,o.probability,o.nextAction,o.nextActionDate,o.notes,o.contractNumber||"" ,invText].join(" ").toLowerCase().includes(q);
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
    // Valore base MOL per commerciale (stessa cascata del totale): così la
    // somma della colonna quadra col "Valore totale (base MOL)" del riepilogo.
    const molBase = mine.reduce((s,o)=>s+molBaseFor(o).base,0);
    return { owner, open, susp, won, lost, pipe, pipeW, issued, planned, molBase };
  });
  // Riga totale
  const T = rows.reduce((t,r)=>({
    open:t.open+r.open, susp:t.susp+r.susp, won:t.won+r.won, lost:t.lost+r.lost,
    pipe:t.pipe+r.pipe, pipeW:t.pipeW+r.pipeW, issued:t.issued+r.issued,
    planned:t.planned+r.planned, molBase:t.molBase+r.molBase
  }), {open:0,susp:0,won:0,lost:0,pipe:0,pipeW:0,issued:0,planned:0,molBase:0});
  return `<div style="margin-top:10px;"><b>Statistiche per commerciale</b></div>
    <div style="overflow:auto; margin-top:6px;">
    <table class="miniTable"><thead><tr>
      <th>Commerciale</th><th>Aperte</th><th>Sospese</th><th>Vinte</th><th>Perse</th>
      <th>Pipe €</th><th>Pipe pond. €</th><th>Emesso €</th><th>Pianificato €</th><th>Valore base MOL €</th>
    </tr></thead><tbody>
    ${rows.map(r=>`<tr><td>${escapeHtml(r.owner)}</td><td>${r.open}</td><td>${r.susp}</td><td>${r.won}</td><td>${r.lost}</td><td>€ ${eur(r.pipe)}</td><td>€ ${eur(r.pipeW)}</td><td>€ ${eur(r.issued)}</td><td>€ ${eur(r.planned)}</td><td>€ ${eur(r.molBase)}</td></tr>`).join("")}
    <tr style="font-weight:700;border-top:2px solid #c8cfe0;"><td>Totale</td><td>${T.open}</td><td>${T.susp}</td><td>${T.won}</td><td>${T.lost}</td><td>€ ${eur(T.pipe)}</td><td>€ ${eur(T.pipeW)}</td><td>€ ${eur(T.issued)}</td><td>€ ${eur(T.planned)}</td><td>€ ${eur(T.molBase)}</td></tr>
    </tbody></table></div>`;
}

function renderKpi(){
  const allNorm  = db.opportunities.map(normalizeOpp);
  // Il cruscotto rispecchia esattamente i filtri attivi nella lista
  const all = allNorm.filter(matchesFilters);

  // Etichetta filtri attivi
  const filterLabels = [];
  const yf  = ui.yearFilter?.value||"all";
  const fyf = ui.fiscalYearFilter?.value||"all";
  const sf  = ui.statusFilter?.value||"";
  const of  = ui.ownerFilter?.value||"";
  const phases   = getCheckedValues(ui.phaseFilterBox);
  const products = getCheckedValues(ui.productFilterBox);
  const iif = ui.invIssuedFilter?.value||"all";
  const ipf = ui.invPlannedFilter?.value||"all";
  const af  = ui.archiveFilter?.value||"active";
  if(yf !== "all") filterLabels.push(`Anno creazione: ${yf}`);
  if(fyf !== "all") filterLabels.push(`Anno competenza: ${fyf}`);
  if(sf)  filterLabels.push(`Stato: ${sf}`);
  if(of)  filterLabels.push(`Commerciale: ${of}`);
  if(phases.length > 0)   filterLabels.push(`Fase: ${phases.length===1 ? phases[0] : phases.length+" selezionate"}`);
  if(products.length > 0) filterLabels.push(`Prodotto: ${products.length===1 ? products[0] : products.length+" selezionati"}`);
  if(iif !== "all") filterLabels.push(`Fatture emesse: ${iif}`);
  if(ipf !== "all") filterLabels.push(`Fatture prog.: ${ipf}`);
  if(af === "archived") filterLabels.push("📦 Archiviate");
  if(af === "all")      filterLabels.push("Archiviate incluse");
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

  // MOL del cruscotto: calcolato su TUTTE le opportunità filtrate, usando per
  // ognuna la base a cascata (emesso → pianificato → valore previsto).
  // Così il margine ha senso anche quando le opportunità non sono ancora fatturate.
  const molBaseTotal = all.reduce((s,o)=>s+molBaseFor(o).base,0);
  const molCostTotal = all.reduce((s,o)=>s+toNum(o.serviceCost),0);
  const molWon   = molBaseTotal - molCostTotal;
  const molPct   = molBaseTotal > 0 ? (molWon/molBaseTotal)*100 : 0;

  ui.kpiBox.innerHTML =
    filterNote +
    `<div><b>Opportunità aperte</b>: ${open} • <b>Sospese</b>: ${susp}</div>` +
    `<div><b>Azioni commerciali scadute</b>: ${overdue}</div>` +
    `<div><b>Pipeline (valore previsto)</b>: € ${eur(totalPipe)}</div>` +
    `<div><b>Pipeline ponderato</b>: € ${eur(weightedPipe)}</div>` +
    `<div><b>Chiuse vinte non fatturate</b>: ${wonNotInvoiced.length} (€ ${eur(eurNotInvoiced)})</div>` +
    `<div><b>Chiuse vinte fatturate</b>: ${wonInvoiced.length} (€ ${eur(eurInvoiced)})</div>` +
    `<div><b>€ totale chiuse vinte</b>: € ${eur(eurTotal)}</div>` +
    `<div style="margin-top:6px;padding-top:6px;border-top:1px solid #e5e8f0;">` +
      `<b>Valore totale (base MOL)</b>: € ${eur(molBaseTotal)}` +
    `</div>` +
    `<div><b>Costo erogazione totale</b>: € ${eur(molCostTotal)}</div>` +
    `<div><b>MOL (opportunità filtrate)</b>: € ${eur(molWon)} — ${molPct.toFixed(1)}%</div>` +
    `<div class="muted" style="font-size:11px;margin-top:2px;">MOL su ${all.length} opp. filtrate, base a cascata: emesso → pianificato → valore previsto</div>` +
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
      title.innerHTML = `${o.archived ? '<span class="archived-badge">📦 ARCHIVIATA</span>' : ''}<span class="oppid-badge">${escapeHtml(o.oppId)}</span>${escapeHtml(o.name||"(senza nome)")} — ${escapeHtml(o.lead||"(lead)")}`;
    } else {
      title.textContent = `${o.name||"(senza nome)"} — ${o.lead||"(lead)"}`;
    }

    const meta  = document.createElement("div"); meta.className = "meta";
    const issued  = totalIssued(o), planned = totalPlanned(o);
    const cost    = toNum(o.serviceCost);
    const overdue = o.nextActionDate && o.nextActionDate < todayStr() ? "⚠️ azione scaduta" : "";
    // Il costo è mostrato in anteprima. L'avviso "costo mancante" compare solo
    // se l'incarico NON è splittato e il costo è 0 (perché per gli splittati il
    // costo 0 è legittimo: il cliente paga il docente separatamente).
    const esc = s => String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
    let costHtml;
    if(cost > 0){
      costHtml = `Costo: € ${cost.toFixed(2)}` + (o.split ? ` <span class="split-badge">splittato</span>` : "");
    } else if(o.split){
      costHtml = `Costo: € 0.00 <span class="split-badge">splittato</span>`;
    } else {
      costHtml = `<span class="cost-missing">⚠ costo mancante</span>`;
    }
    meta.innerHTML =
      `Creata: ${esc(o.createdAt)} • ${esc(o.owner||"-")} • ${esc(o.status)} • ${esc(o.phase)} • ${esc(o.product)}` +
      ` • Prev.: € ${toNum(o.valueExpected).toFixed(2)}` +
      ` • ${costHtml}` +
      ` • Emesso: € ${issued.toFixed(2)}` +
      ` • Pianif.: € ${planned.toFixed(2)}` +
      (overdue ? ` • ${esc(overdue)}` : "");

    left.appendChild(title); left.appendChild(meta);

    const btnOpen = document.createElement("button"); btnOpen.type="button"; btnOpen.textContent="Apri";
    btnOpen.addEventListener("click", async () => {
      const ok = await confirmIfDirty();
      if(!ok) return;
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
  refreshFiscalYearFilter();
  renderKpi();
  renderOppList();
  if(currentOppId){
    const fresh = db.opportunities.find(x=>x.id===currentOppId);
    if(fresh){ const n=normalizeOpp(fresh); renderCalcBox(n); renderInvoices(n.invoices); }
  }
}

/**
 * Versione "safe" di renderAll usata dopo sync/pull/merge in background.
 * Aggiorna KPI e lista, ma NON sovrascrive i campi del form aperto
 * (per non perdere modifiche in corso o cambiare il commerciale).
 * Aggiorna fatture e calcBox dell'opp aperta (read-only) senza toccare i campi editabili.
 */
function renderAllSafe(){
  refreshYearFilter();
  refreshFiscalYearFilter();
  renderKpi();
  renderOppList();
  if(currentOppId){
    const fresh = db.opportunities.find(x=>x.id===currentOppId);
    if(fresh){
      const n = normalizeOpp(fresh);
      // Aggiorna solo le sezioni read-only (fatture e calcoli), MAI i campi del form
      renderCalcBox(n);
      renderInvoices(n.invoices);
    }
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
  const oppHeader = ["OppID","Lead","DataCreazione","AnnoCompetenza","NomeOpportunita","Stato","Fase","Prodotto","ValorePrevisto","Probabilita","ProssimaAzione","DataProssimaAzione","CostoErogazione","Splittato","EmessoTotale","PianificatoTotale","BaseMOL","MOL","MOL_percento"].join(";");
  const oppRows = ops.map(o=>{
    const issued=totalIssued(o), planned=totalPlanned(o), cost=toNum(o.serviceCost), expected=toNum(o.valueExpected);
    // Stessa cascata usata a schermo: emesso → pianificato → valore previsto
    let base, baseLabel;
    if(issued>0){ base=issued; baseLabel="emesso"; }
    else if(planned>0){ base=planned; baseLabel="pianificato"; }
    else { base=expected; baseLabel="valore previsto"; }
    const mol=base-cost, molPct=base>0?(mol/base)*100:0;
    return [o.oppId||o.id,o.lead,o.createdAt,fiscalYearOf(o),o.name,o.status,o.phase,o.product,o.valueExpected,o.probability,o.nextAction,o.nextActionDate,cost.toFixed(2),(o.split?"sì":"no"),issued.toFixed(2),planned.toFixed(2),baseLabel,mol.toFixed(2),molPct.toFixed(1)].map(csvEscape).join(";");
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

// ═══════════════════════════════════════════════════════════════
// REMINDER ANNO DI COMPETENZA (fine anno)
// A ogni primo accesso del mese (da gennaio in poi) segnala le opportunità
// dell'anno appena concluso che sono rimaste aperte/sospese, senza fattura e
// non chiuse: probabilmente il corso è slittato e la competenza va spostata
// all'anno successivo. Si ripete ogni anno (usa "anno corrente − 1").
// ═══════════════════════════════════════════════════════════════

// Opportunità candidate allo spostamento per un dato anno target.
function fiscalReminderCandidates(targetYear){
  const ignored = (db.meta && db.meta.fiscalReminderIgnored && db.meta.fiscalReminderIgnored[targetYear]) || [];
  return db.opportunities.map(normalizeOpp).filter(o=>{
    if(!["aperta","sospesa"].includes(o.status)) return false;   // solo aperte/sospese
    if(fiscalYearOf(o) !== String(targetYear)) return false;      // competenza = anno target
    if(totalIssued(o) > 0 || totalPlanned(o) > 0) return false;    // nessuna fattura
    if(ignored.includes(o.id)) return false;                      // ignorata per quest'anno
    return true;
  });
}

// Chiave "anno-mese" corrente, es. "2027-01"
function currentYearMonth(){
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,"0")}`;
}

function showFiscalReminder(force){
  try{
    const now = new Date();
    const targetYear = now.getFullYear() - 1; // anno appena concluso
    // Solo da gennaio in poi (il mese 0 = gennaio è già >= 0, quindi sempre valido
    // nell'anno solare successivo). Non mostrare a dicembre dell'anno target.
    const candidates = fiscalReminderCandidates(targetYear);
    if(candidates.length === 0) return;

    // Cadenza mensile: se già mostrato questo mese e non è forzato, esci.
    const ym = currentYearMonth();
    if(!force){
      const lastShown = db.meta?.fiscalReminderLastShown || "";
      if(lastShown === ym) return;
    }

    // Costruisci il contenuto del modal
    const rows = candidates.map(o=>`
      <label class="fiscalRow" style="display:flex;gap:10px;align-items:flex-start;cursor:pointer;padding:9px 11px;border:1px solid var(--border);border-radius:8px;margin-bottom:7px;background:var(--surface);">
        <input type="checkbox" class="fiscalChk" data-id="${escapeHtml(o.id)}" checked style="width:18px;min-width:18px;max-width:18px;height:18px;margin:2px 0 0 0;flex:0 0 auto;"/>
        <span style="flex:1 1 auto;min-width:0;font-size:13px;">
          <b>${escapeHtml(o.lead||"(lead)")}</b> — ${escapeHtml(o.name||"(opportunità)")}
          <span class="oppid-badge">${escapeHtml(o.oppId||"")}</span>
          <div class="muted" style="margin-top:2px;">Stato: ${escapeHtml(o.status)} • Valore previsto: € ${toNum(o.valueExpected).toFixed(2)}</div>
        </span>
      </label>`).join("");

    const html = `
      <div style="margin-bottom:10px;font-size:13px;">
        Ci sono <b>${candidates.length}</b> opportunità con competenza <b>${targetYear}</b> ancora aperte/sospese e senza fattura.
        Probabilmente il corso è slittato: vuoi spostare la loro competenza al <b>${targetYear+1}</b>?
      </div>
      <div style="max-height:280px;overflow-y:auto;overflow-x:hidden;border:1px solid var(--border);border-radius:8px;padding:8px;margin-bottom:12px;">
        ${rows}
      </div>
      <div style="display:flex;flex-wrap:wrap;gap:8px;">
        <button id="fiscalMoveAll" type="button" class="primary" style="width:auto;">📅 Sposta TUTTE al ${targetYear+1}</button>
        <button id="fiscalMoveSel" type="button" style="width:auto;">Sposta solo le selezionate</button>
        <button id="fiscalIgnoreMonth" type="button" style="width:auto;">Ignora per questo mese</button>
      </div>
      <div style="margin-top:8px;font-size:11.5px;color:#888;">
        Suggerimento: togli la spunta a un'opportunità e usa "Sposta solo le selezionate"
        per lasciarne alcune al ${targetYear}. Il promemoria tornerà il mese prossimo per quelle rimaste.
      </div>`;

    if(ui.modalTitle) ui.modalTitle.textContent = `Competenza ${targetYear} — opportunità da rivedere`;
    ui.modalBody.innerHTML = html;
    ui.modal.classList.remove("hidden");

    // Registra che è stato mostrato questo mese
    if(!db.meta) db.meta = {};
    db.meta.fiscalReminderLastShown = ym;
    saveDbLocal();

    // Sposta un insieme di id al targetYear+1
    const moveIds = (ids) => {
      const set = new Set(ids);
      let moved = 0;
      db.opportunities.forEach(o=>{
        if(set.has(o.id)){ o.fiscalYear = String(targetYear+1); o.updatedAt = nowIso(); moved++; }
      });
      if(moved>0){ saveDb(); renderAll(); }
      closeModal();
      if(moved>0) alert(`${moved} opportunità spostate alla competenza ${targetYear+1}.`);
    };

    const btnAll = document.getElementById("fiscalMoveAll");
    if(btnAll) btnAll.onclick = () => moveIds(candidates.map(o=>o.id));

    const btnSel = document.getElementById("fiscalMoveSel");
    if(btnSel) btnSel.onclick = () => {
      const ids = Array.from(document.querySelectorAll(".fiscalChk:checked")).map(c=>c.dataset.id);
      if(ids.length === 0){ alert("Non hai selezionato nessuna opportunità."); return; }
      // Le NON selezionate restano; per non riproporle subito, le ignoro per quest'anno
      const selected = new Set(ids);
      const notSelected = candidates.filter(o=>!selected.has(o.id)).map(o=>o.id);
      if(notSelected.length > 0){
        if(!db.meta.fiscalReminderIgnored) db.meta.fiscalReminderIgnored = {};
        const arr = db.meta.fiscalReminderIgnored[targetYear] || [];
        db.meta.fiscalReminderIgnored[targetYear] = [...new Set([...arr, ...notSelected])];
        saveDbLocal();
      }
      moveIds(ids);
    };

    const btnIgn = document.getElementById("fiscalIgnoreMonth");
    if(btnIgn) btnIgn.onclick = () => { closeModal(); }; // lastShown è già impostato: torna il mese prossimo

  } catch(e){ console.warn("showFiscalReminder:", e); }
}

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
    if(o) setOwnerPill(normalizeOpp(o).owner);
  } else {
    setOwnerPill(db.salespeople[0] || "");
  }
  saveDb();
  renderAll();
}

// ═══════════════════════════════════════════════════════════════
// EVENT LISTENERS
// ═══════════════════════════════════════════════════════════════

ui.newOppBtn.addEventListener("click", async () => { if(!await confirmIfDirty()) return; newOpp(); });
ui.oppForm.addEventListener("submit", saveOpp);
ui.archiveBtn?.addEventListener("click", archiveOpp);
ui.restoreBtn?.addEventListener("click", restoreOpp);
ui.deleteBtn.addEventListener("click", deleteOpp);
ui.addInvBtn.addEventListener("click", addInvoice);
ui.lead.addEventListener("input", () => fillLeadContactFields(ui.lead.value));

ui.q.addEventListener("input", renderAll);
ui.clearFiltersBtn?.addEventListener("click", () => clearAllFilters(true));
ui.statusFilter.addEventListener("change", renderAll);
// Fase e Prodotto ora sono filtri a caselle multiple: i listener sono già
// agganciati in buildCheckFilter(), non servono qui.
ui.ownerFilter?.addEventListener("change", renderAll);
ui.archiveFilter?.addEventListener("change", renderAll);
ui.yearFilter?.addEventListener("change", renderAll);
ui.fiscalYearFilter?.addEventListener("change", renderAll);
ui.dueFilter.addEventListener("change", renderAll);
ui.invPlannedFilter.addEventListener("change", renderAll);
ui.invIssuedFilter?.addEventListener("change", renderAll);
ui.oppForm?.addEventListener("input", updateDirtyHint);
ui.oppForm?.addEventListener("change", updateDirtyHint);

// ═══════════════════════════════════════════════════════════════
// SINCRONIZZAZIONE STATO ↔ FASE per opportunità PERSE
// Se lo stato diventa "chiusa persa" o "abbandonata" → la fase diventa "persa".
// Se la fase diventa "persa" → lo stato diventa "chiusa persa".
// Così stato e fase non restano mai incoerenti.
// ═══════════════════════════════════════════════════════════════
const LOST_STATUSES = ["chiusa persa", "abbandonata"];
ui.oppStatus?.addEventListener("change", () => {
  if(LOST_STATUSES.includes(ui.oppStatus.value)){
    if(ui.oppPhase && ui.oppPhase.value !== "persa"){
      ui.oppPhase.value = "persa";
      updateDirtyHint();
    }
  } else {
    // Se si esce da uno stato "perso" ma la fase è ancora "persa",
    // riportala a un valore neutro coerente per non lasciarla bloccata.
    if(ui.oppPhase && ui.oppPhase.value === "persa"){
      ui.oppPhase.value = "contatto iniziale";
      updateDirtyHint();
    }
  }
});
ui.oppPhase?.addEventListener("change", () => {
  if(ui.oppPhase.value === "persa"){
    if(ui.oppStatus && !LOST_STATUSES.includes(ui.oppStatus.value)){
      ui.oppStatus.value = "chiusa persa";
      updateDirtyHint();
    }
  } else {
    // Fase diversa da "persa" ma stato ancora "perso": riallinea lo stato ad "aperta".
    if(ui.oppStatus && LOST_STATUSES.includes(ui.oppStatus.value)){
      ui.oppStatus.value = "aperta";
      updateDirtyHint();
    }
  }
});

ui.manageOwnersBtn?.addEventListener("click", handleManageOwners);

ui.exportBtn.addEventListener("click", exportBackup);
ui.importBtn.addEventListener("click", () => ui.importFile.click());
ui.importFile.addEventListener("change", async (e) => {
  const f = e.target.files?.[0];
  if(!await confirmIfDirty()) return;
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

// DEFAULT ALL'AVVIO: attivo solo il filtro "Anno competenza fattura" sull'anno
// corrente; tutti gli altri filtri partono neutri. Popolo prima i menu anni.
refreshFiscalYearFilter();
// Riporta TUTTI i filtri allo stato di default:
// - Anno competenza fattura → anno corrente (se disponibile), altrimenti "tutti"
// - Anno creazione, stato, commerciale, fatture, archivio → neutri
// - Fase e Prodotto (caselle multiple) → nessuna spuntata
// Usata sia all'avvio sia dal pulsante "Pulisci filtri".
function clearAllFilters(rerender){
  const cy = String(new Date().getFullYear());
  if(ui.fiscalYearFilter){
    const hasCy = Array.from(ui.fiscalYearFilter.options).some(o=>o.value===cy);
    ui.fiscalYearFilter.value = hasCy ? cy : "all";
  }
  if(ui.yearFilter)        ui.yearFilter.value = "all";
  if(ui.statusFilter)      ui.statusFilter.value = "";
  if(ui.phaseFilterBox)    ui.phaseFilterBox.querySelectorAll('input[type="checkbox"]').forEach(c=>c.checked=false);
  if(ui.productFilterBox)  ui.productFilterBox.querySelectorAll('input[type="checkbox"]').forEach(c=>c.checked=false);
  if(ui.ownerFilter)       ui.ownerFilter.value = "";
  if(ui.dueFilter)         ui.dueFilter.value = "all";
  if(ui.invPlannedFilter)  ui.invPlannedFilter.value = "all";
  if(ui.invIssuedFilter)   ui.invIssuedFilter.value = "all";
  if(ui.archiveFilter)     ui.archiveFilter.value = "active";
  if(ui.q)                 ui.q.value = "";
  if(rerender) renderAll();
}

// All'avvio: applica i default (senza re-render, che avviene subito dopo)
clearAllFilters(false);

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

// Promemoria competenza fine anno: all'avvio (dopo l'eventuale sync).
// Compare una volta al mese, da gennaio in poi, finché non sistemate/ignorate.
// Ritardo maggiore così non si sovrappone al promemoria azioni scadute.
setTimeout(() => { try { showFiscalReminder(false); } catch(e){} }, 2500);

// ═══════════════════════════════════════════════════════════════
// REPORT MENSILE
// ═══════════════════════════════════════════════════════════════

function generateReport() {
  // Il report si basa sui FILTRI ATTIVI del cruscotto (non più su un intervallo
  // di date). Considera solo le opportunità che passano matchesFilters, così
  // rispetta anno di competenza, fase, prodotto, stato, commerciale, ecc.
  const opps = db.opportunities.map(normalizeOpp).filter(matchesFilters);
  const owners = normalizeSalespeopleList(db.salespeople);

  // Con il report sui filtri attivi non c'è più un "periodo": ogni conteggio
  // considera tutte le opportunità filtrate. inPeriod() resta definita come
  // sempre-vera per non alterare la struttura interna delle sezioni.
  function inPeriod(dateStr) { return true; }
  function fmtEur(n) {
    // Formattazione deterministica (non dipende da toLocaleString, che in
    // alcuni ambienti non applica il separatore delle migliaia sotto le 5 cifre):
    // separatore migliaia = punto, decimali = virgola. Es. 9375 → "9.375,00 €"
    const num = toNum(n);
    const neg = num < 0;
    const fixed = Math.abs(num).toFixed(2);      // "9375.00"
    const [intPart, decPart] = fixed.split(".");
    const withSep = intPart.replace(/\B(?=(\d{3})+(?!\d))/g, "."); // "9.375"
    return `${neg ? "-" : ""}${withSep},${decPart} €`;
  }
  function fmtDate(d) {
    if (!d) return "—";
    const [y, m, dd] = d.split("-");
    return `${dd}/${m}/${y}`;
  }
  // Etichetta descrittiva dei filtri attivi (sostituisce il periodo)
  function periodLabel() {
    return activeFilterSummary() || "Tutte le opportunità";
  }

  // ── 1. Fatturato emesso nel periodo ─────────────────────────
  let fatturato = 0;
  const fattRows = [];
  for (const o of opps) {
    for (const inv of o.invoices) {
      if (inv.status === "emessa" && inPeriod(inv.date)) {
        fatturato += inv.amount;
        fattRows.push({ opp: o, inv });
      }
    }
  }

  // ── 2. Fatture pianificate in arrivo (data prevista > oggi) ─
  const today = todayStr();
  let pianificato = 0;
  const pianRows = [];
  for (const o of opps.filter(o => !o.archived)) {
    for (const inv of o.invoices) {
      if (inv.status === "pianificata" && inv.plannedDate && inv.plannedDate >= today) {
        pianificato += inv.plannedAmount;
        pianRows.push({ opp: o, inv });
      }
    }
  }
  // ordina per data
  pianRows.sort((a, b) => (a.inv.plannedDate || "").localeCompare(b.inv.plannedDate || ""));

  // ── 3. Pipeline aperta ───────────────────────────────────────
  const pipelineOpps = opps.filter(o => !o.archived && o.status === "aperta");
  const pipelineVal  = pipelineOpps.reduce((s, o) => s + o.valueExpected, 0);
  const pipelinePond = pipelineOpps.reduce((s, o) => s + o.valueExpected * (parseFloat(o.probability) / 100 || 0), 0);
  // Elenco dettagliato delle opportunità aperte (per la sezione
  // "Opportunità aperte / pipeline" del report). Le sospese sono escluse,
  // coerentemente col riquadro "Pipeline aperta" e col fatto che di norma
  // non rientrano più nel processo commerciale.
  const pipelineDetailOpps = opps.filter(o => !o.archived && o.status === "aperta")
    .sort((a,b)=>toNum(b.valueExpected)-toNum(a.valueExpected));

  // ── 4. Opportunità vinte/perse nel periodo ───────────────────
  const vinteOpps = opps.filter(o => o.status === "chiusa vinta" && inPeriod(o.updatedAt?.slice(0,10)));
  const perseOpps = opps.filter(o => (o.status === "chiusa persa" || o.status === "abbandonata") && inPeriod(o.updatedAt?.slice(0,10)));
  const valoreVinte = vinteOpps.reduce((s, o) => s + o.valueExpected, 0);

  // ── 5. Azioni scadute ────────────────────────────────────────
  const scaduteOpps = opps.filter(o =>
    !o.archived &&
    o.nextActionDate && o.nextActionDate < today &&
    ["aperta", "sospesa"].includes(o.status)
  ).sort((a, b) => a.nextActionDate.localeCompare(b.nextActionDate));

  // ── 5-bis. Totali MOL (STESSA logica del cruscotto) ──────────
  // Base a cascata per ogni opportunità filtrata: emesso → pianificato →
  // valore previsto. Garantisce che il report quadri col cruscotto.
  const molBaseTotal = opps.reduce((s,o)=>s+molBaseFor(o).base,0);
  const molCostTotal = opps.reduce((s,o)=>s+toNum(o.serviceCost),0);
  const molValue     = molBaseTotal - molCostTotal;
  const molPct       = molBaseTotal > 0 ? (molValue/molBaseTotal)*100 : 0;

  // ── 6. Dettaglio per commerciale ────────────────────────────
  const byOwner = {};
  for (const name of owners) {
    const mine = opps.filter(o => o.owner === name);
    byOwner[name] = {
      aperte:        mine.filter(o => !o.archived && o.status === "aperta").length,
      pipeline:      mine.filter(o => !o.archived && o.status === "aperta").reduce((s,o) => s+o.valueExpected, 0),
      vinte:         mine.filter(o => o.status === "chiusa vinta" && inPeriod(o.updatedAt?.slice(0,10))).length,
      valoreVinte:   mine.filter(o => o.status === "chiusa vinta" && inPeriod(o.updatedAt?.slice(0,10))).reduce((s,o)=>s+toNum(o.valueExpected),0),
      perse:         mine.filter(o => (o.status==="chiusa persa"||o.status==="abbandonata") && inPeriod(o.updatedAt?.slice(0,10))).length,
      fatturato:     mine.flatMap(o=>o.invoices).filter(i=>i.status==="emessa"&&inPeriod(i.date)).reduce((s,i)=>s+i.amount, 0),
      // Valore base MOL per commerciale: pipeline + vinte con cascata. La somma
      // di questa colonna quadra col "Valore totale (base MOL)" del riepilogo.
      molBase:       mine.reduce((s,o)=>s+molBaseFor(o).base,0),
      scadute:       mine.filter(o=>!o.archived&&o.nextActionDate&&o.nextActionDate<today&&["aperta","sospesa"].includes(o.status)).length,
    };
  }

  // ── HTML ─────────────────────────────────────────────────────
  const css = `
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400;500&display=swap');
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: 'DM Sans', sans-serif; font-size: 13px; color: #1a2133; background: #fff; padding: 28px 32px; }
    h1 { font-size: 22px; font-weight: 700; color: #1a2744; margin-bottom: 2px; }
    .subtitle { color: #7a85a0; font-size: 13px; margin-bottom: 24px; }
    .section { margin-bottom: 24px; break-inside: avoid; }
    .section-title { font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: .7px;
      color: #7a85a0; border-bottom: 1px solid #dde2ec; padding-bottom: 6px; margin-bottom: 12px; }
    .kpi-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-bottom: 8px; }
    .kpi { background: #f2f4f8; border-radius: 10px; padding: 12px 14px; }
    .kpi .val { font-size: 20px; font-weight: 700; color: #1a2744; }
    .kpi .lbl { font-size: 11px; color: #7a85a0; margin-top: 2px; }
    .kpi.green .val { color: #1a7a3a; }
    .kpi.amber .val { color: #c75a00; }
    .kpi.red   .val { color: #b52a2a; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th { background: #f2f4f8; font-size: 10.5px; font-weight: 600; text-transform: uppercase;
      letter-spacing: .4px; color: #7a85a0; padding: 6px 8px; text-align: left; }
    td { padding: 6px 8px; border-bottom: 1px solid #eef0f4; vertical-align: top; }
    tr:last-child td { border-bottom: none; }
    .mono { font-family: 'DM Mono', monospace; }
    .badge { display: inline-block; font-size: 10px; font-weight: 700; font-family: 'DM Mono', monospace;
      border-radius: 4px; padding: 1px 5px; margin-right: 4px; }
    .badge.green { background: #edf7f1; color: #1a7a3a; }
    .badge.red   { background: #fdf0f0; color: #b52a2a; }
    .badge.blue  { background: #eef3fd; color: #2255c4; }
    .badge.grey  { background: #f0f0f0; color: #888; }
    .badge.amber { background: #fdf3e0; color: #c75a00; }
    .warn { color: #b52a2a; font-weight: 600; }
    .muted { color: #7a85a0; }
    .right { text-align: right; }
    .total-row td { font-weight: 700; background: #f8f9fb; }
    @media print {
      body { padding: 0; }
      .no-print { display: none; }
      .section { break-inside: avoid; }
    }
  `;

  function statusBadge(s) {
    const map = { "aperta":"green","sospesa":"amber","chiusa vinta":"blue","chiusa persa":"red","abbandonata":"grey" };
    return `<span class="badge ${map[s]||'grey'}">${escapeHtml(s)}</span>`;
  }

  // Sezione fatturato
  let fattHtml = `<table><thead><tr>
    <th>Opportunità</th><th>Cliente</th><th>N° Fattura</th><th>Data</th><th class="right">Importo</th>
  </tr></thead><tbody>`;
  if (fattRows.length === 0) {
    fattHtml += `<tr><td colspan="5" class="muted">Nessuna fattura emessa nel periodo.</td></tr>`;
  } else {
    for (const { opp, inv } of fattRows) {
      fattHtml += `<tr>
        <td><span class="badge blue mono">${escapeHtml(opp.oppId)}</span>${escapeHtml(opp.name)}</td>
        <td>${escapeHtml(opp.lead)}</td>
        <td class="mono">${escapeHtml(inv.number||"—")}</td>
        <td>${fmtDate(inv.date)}</td>
        <td class="right mono">${fmtEur(inv.amount)}</td>
      </tr>`;
    }
    fattHtml += `<tr class="total-row"><td colspan="4">Totale fatturato</td><td class="right mono">${fmtEur(fatturato)}</td></tr>`;
  }
  fattHtml += `</tbody></table>`;

  // Sezione pianificate
  let pianHtml = `<table><thead><tr>
    <th>Opportunità</th><th>Cliente</th><th>Data prevista</th><th class="right">Importo previsto</th>
  </tr></thead><tbody>`;
  if (pianRows.length === 0) {
    pianHtml += `<tr><td colspan="4" class="muted">Nessuna fattura pianificata.</td></tr>`;
  } else {
    for (const { opp, inv } of pianRows) {
      pianHtml += `<tr>
        <td><span class="badge blue mono">${escapeHtml(opp.oppId)}</span>${escapeHtml(opp.name)}</td>
        <td>${escapeHtml(opp.lead)}</td>
        <td>${fmtDate(inv.plannedDate)}</td>
        <td class="right mono">${fmtEur(inv.plannedAmount)}</td>
      </tr>`;
    }
    pianHtml += `<tr class="total-row"><td colspan="3">Totale pianificato</td><td class="right mono">${fmtEur(pianificato)}</td></tr>`;
  }
  pianHtml += `</tbody></table>`;

  // Sezione vinte/perse
  function oppTableHtml(list, label) {
    if (list.length === 0) return `<p class="muted">Nessuna opportunità ${label} nel periodo.</p>`;
    let tot = 0;
    let h = `<table><thead><tr><th>Opportunità</th><th>Cliente</th><th>Commerciale</th><th class="right">Valore previsto</th></tr></thead><tbody>`;
    for (const o of list) {
      tot += toNum(o.valueExpected);
      h += `<tr>
        <td><span class="badge blue mono">${escapeHtml(o.oppId)}</span>${escapeHtml(o.name)}</td>
        <td>${escapeHtml(o.lead)}</td>
        <td>${escapeHtml(o.owner)}</td>
        <td class="right mono">${fmtEur(o.valueExpected)}</td>
      </tr>`;
    }
    h += `<tr class="total-row"><td colspan="3">Totale</td><td class="right mono">${fmtEur(tot)}</td></tr>`;
    h += `</tbody></table>`;
    return h;
  }

  // Come oppTableHtml, ma con colonna Fase e riga totale — usata per la
  // pipeline aperta, dove serve distinguere le fasi (es. attesa CTR / mandare CTR).
  function oppTableHtmlWithPhase(list, label) {
    if (list.length === 0) return `<p class="muted">Nessuna opportunità ${label}.</p>`;
    let tot = 0;
    let h = `<table><thead><tr><th>Opportunità</th><th>Cliente</th><th>Commerciale</th><th>Fase</th><th class="right">Valore previsto</th></tr></thead><tbody>`;
    for (const o of list) {
      tot += toNum(o.valueExpected);
      h += `<tr>
        <td><span class="badge blue mono">${escapeHtml(o.oppId)}</span>${escapeHtml(o.name)}</td>
        <td>${escapeHtml(o.lead)}</td>
        <td>${escapeHtml(o.owner)}</td>
        <td>${escapeHtml(o.phase)}</td>
        <td class="right mono">${fmtEur(o.valueExpected)}</td>
      </tr>`;
    }
    h += `<tr class="total-row"><td colspan="4">Totale pipeline</td><td class="right mono">${fmtEur(tot)}</td></tr>`;
    h += `</tbody></table>`;
    return h;
  }

  // Sezione azioni scadute
  let scaduteHtml = `<table><thead><tr>
    <th>Opportunità</th><th>Cliente</th><th>Commerciale</th><th>Scaduta il</th><th>Azione</th>
  </tr></thead><tbody>`;
  if (scaduteOpps.length === 0) {
    scaduteHtml += `<tr><td colspan="5" class="muted">Nessuna azione scaduta.</td></tr>`;
  } else {
    for (const o of scaduteOpps) {
      scaduteHtml += `<tr>
        <td><span class="badge blue mono">${escapeHtml(o.oppId)}</span>${escapeHtml(o.name)}</td>
        <td>${escapeHtml(o.lead)}</td>
        <td>${escapeHtml(o.owner)}</td>
        <td class="warn">${fmtDate(o.nextActionDate)}</td>
        <td>${escapeHtml(o.nextAction)}</td>
      </tr>`;
    }
  }
  scaduteHtml += `</tbody></table>`;

  // Sezione per commerciale
  let ownerHtml = `<table><thead><tr>
    <th>Commerciale</th><th class="right">Opp. aperte</th><th class="right">Pipeline</th>
    <th class="right">Vinte periodo</th><th class="right">Valore vinte</th><th class="right">Perse periodo</th>
    <th class="right">Fatturato periodo</th><th class="right">Valore base MOL</th><th class="right">Azioni scadute</th>
  </tr></thead><tbody>`;
  const OT = { aperte:0, pipeline:0, vinte:0, valoreVinte:0, perse:0, fatturato:0, molBase:0, scadute:0 };
  for (const [name, d] of Object.entries(byOwner)) {
    OT.aperte+=d.aperte; OT.pipeline+=d.pipeline; OT.vinte+=d.vinte; OT.valoreVinte+=d.valoreVinte;
    OT.perse+=d.perse; OT.fatturato+=d.fatturato; OT.molBase+=d.molBase; OT.scadute+=d.scadute;
    ownerHtml += `<tr>
      <td><strong>${escapeHtml(name)}</strong></td>
      <td class="right">${d.aperte}</td>
      <td class="right mono">${fmtEur(d.pipeline)}</td>
      <td class="right">${d.vinte}</td>
      <td class="right mono">${fmtEur(d.valoreVinte)}</td>
      <td class="right">${d.perse}</td>
      <td class="right mono">${fmtEur(d.fatturato)}</td>
      <td class="right mono">${fmtEur(d.molBase)}</td>
      <td class="right ${d.scadute > 0 ? "warn" : ""}">${d.scadute}</td>
    </tr>`;
  }
  ownerHtml += `<tr class="total-row">
      <td>Totale</td>
      <td class="right">${OT.aperte}</td>
      <td class="right mono">${fmtEur(OT.pipeline)}</td>
      <td class="right">${OT.vinte}</td>
      <td class="right mono">${fmtEur(OT.valoreVinte)}</td>
      <td class="right">${OT.perse}</td>
      <td class="right mono">${fmtEur(OT.fatturato)}</td>
      <td class="right mono">${fmtEur(OT.molBase)}</td>
      <td class="right">${OT.scadute}</td>
    </tr>`;
  ownerHtml += `</tbody></table>`;

  return `<!doctype html><html lang="it"><head>
    <meta charset="utf-8"/>
    <title>Report UP Formazione – ${periodLabel()}</title>
    <style>${css}</style>
  </head><body>
    <div class="no-print" style="background:#1a2744;color:#fff;padding:10px 16px;margin:-28px -32px 24px;display:flex;justify-content:space-between;align-items:center;">
      <span style="font-size:13px;font-weight:600;">Report UP Formazione — ${escapeHtml(periodLabel())}</span>
      <button onclick="window.print()" style="background:#e8a020;color:#1a2744;border:none;border-radius:8px;padding:7px 16px;font-weight:700;cursor:pointer;font-size:13px;">🖨 Stampa</button>
    </div>
    <h1>Report Commerciale</h1>
    <div class="subtitle">Periodo: ${escapeHtml(periodLabel())} · Generato il ${fmtDate(todayStr())}</div>

    <div class="section">
      <div class="section-title">Riepilogo</div>
      <div class="kpi-grid">
        <div class="kpi green"><div class="val mono">${fmtEur(fatturato)}</div><div class="lbl">Fatturato emesso nel periodo</div></div>
        <div class="kpi amber"><div class="val mono">${fmtEur(pianificato)}</div><div class="lbl">Fatture pianificate in arrivo</div></div>
        <div class="kpi"><div class="val mono">${fmtEur(pipelineVal)}</div><div class="lbl">Pipeline aperta (${pipelineOpps.length} opp.)</div></div>
        <div class="kpi"><div class="val mono">${fmtEur(pipelinePond)}</div><div class="lbl">Pipeline ponderata</div></div>
      </div>
      <div class="kpi-grid" style="grid-template-columns:repeat(3,1fr);">
        <div class="kpi green"><div class="val">${vinteOpps.length}</div><div class="lbl">Opportunità vinte · ${fmtEur(valoreVinte)}</div></div>
        <div class="kpi red"><div class="val">${perseOpps.length}</div><div class="lbl">Opportunità perse / abbandonate</div></div>
        <div class="kpi red"><div class="val">${scaduteOpps.length}</div><div class="lbl">Azioni commerciali scadute</div></div>
      </div>
      <div class="kpi-grid" style="grid-template-columns:repeat(3,1fr);margin-top:10px;">
        <div class="kpi"><div class="val mono">${fmtEur(molBaseTotal)}</div><div class="lbl">Valore totale (base MOL)</div></div>
        <div class="kpi"><div class="val mono">${fmtEur(molCostTotal)}</div><div class="lbl">Costo erogazione totale</div></div>
        <div class="kpi ${molValue>=0?'green':'red'}"><div class="val mono">${fmtEur(molValue)} — ${molPct.toFixed(1)}%</div><div class="lbl">MOL (opportunità filtrate)</div></div>
      </div>
      <div style="font-size:11px;color:#7a85a0;margin-top:6px;">
        MOL su ${opps.length} opportunità filtrate · base a cascata: fatturato emesso → fatture pianificate → valore previsto.
        Coincide con i valori del cruscotto per gli stessi filtri.
      </div>
    </div>

    <div class="section">
      <div class="section-title">Fatturato emesso nel periodo</div>
      ${fattHtml}
    </div>

    <div class="section">
      <div class="section-title">Fatture pianificate in arrivo</div>
      ${pianHtml}
    </div>

    <div class="section">
      <div class="section-title">Opportunità aperte / pipeline (${pipelineDetailOpps.length})</div>
      ${oppTableHtmlWithPhase(pipelineDetailOpps, "aperta in pipeline")}
    </div>

    <div class="section">
      <div class="section-title">Opportunità vinte nel periodo</div>
      ${oppTableHtml(vinteOpps, "vinte")}
    </div>

    <div class="section">
      <div class="section-title">Opportunità perse / abbandonate nel periodo</div>
      ${oppTableHtml(perseOpps, "perse o abbandonate")}
    </div>

    <div class="section">
      <div class="section-title">Azioni commerciali scadute</div>
      ${scaduteHtml}
    </div>

    <div class="section">
      <div class="section-title">Dettaglio per commerciale</div>
      ${ownerHtml}
    </div>
  </body></html>`;
}

function showReportModal() {
  ui.modalTitle.textContent = "📊 Genera report";

  // Il report si basa sui filtri attivi del cruscotto. Mostro un riepilogo
  // di ciò che verrà incluso, così l'utente sa cosa sta per generare.
  const summary = activeFilterSummary();
  const count = db.opportunities.map(normalizeOpp).filter(matchesFilters).length;
  const total = db.opportunities.length;

  ui.modalBody.innerHTML = `
    <div style="margin-bottom:14px;font-size:13px;line-height:1.6;">
      Il report considera le <b>opportunità attualmente filtrate</b> nel cruscotto.
      Per cambiare cosa includere, chiudi questa finestra e modifica i filtri
      (Anno competenza, Fase, Prodotto, Stato, Commerciale…), poi riapri il report.
    </div>
    <div style="background:#e8f0fe;border-radius:8px;padding:10px 12px;margin-bottom:16px;font-size:13px;color:#1a4a9a;">
      🔍 <b>Filtri attivi:</b> ${summary ? escapeHtml(summary) : "nessuno (tutte le opportunità)"}
      <div style="margin-top:4px;color:#555;font-size:12px;">${count} opportunità su ${total} verranno incluse nel report.</div>
    </div>
    <button type="button" onclick="openReport()" style="width:100%;padding:9px;background:#1a2744;color:#fff;border:none;border-radius:10px;font-size:14px;font-weight:700;cursor:pointer;">📄 Genera e apri report</button>
  `;
  ui.modal.classList.remove("hidden");
}


function openReport() {
  const html = generateReport();
  const win  = window.open("", "_blank");
  win.document.write(html);
  win.document.close();
  ui.modal.classList.add("hidden");
}

document.getElementById("reportBtn").addEventListener("click", showReportModal);
