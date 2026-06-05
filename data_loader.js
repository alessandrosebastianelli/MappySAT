/**
 * data_loader.js
 * Carica i CSV generati da gee_export.py e li inietta nel dashboard.
 * Include questo file DOPO chart.js nel tuo index.html:
 *   <script src="data_loader.js"></script>
 *
 * I CSV devono stare in ./data/<var_id>/<var_id>_full.csv
 * oppure nella struttura chunk: ./data/<var_id>/<var_id>_DATE_DATE.csv
 */

const DATA_BASE = "./data";

/**
 * Carica tutti i CSV disponibili per una variabile e restituisce
 * { labels: string[], values: number[], anomalies: number[], mean: number }
 * compatibile con la firma attesa dal dashboard.
 *
 * @param {string} varId   - id variabile (es. "ndvi")
 * @param {string} agg     - "monthly" | "seasonal" | "annual"
 * @param {number} decimals
 */
async function loadVarData(varId, agg, decimals = 3) {
  // Prima prova il file merged (_full.csv)
  const fullUrl = `${DATA_BASE}/${varId}/${varId}_full.csv`;
  let raw = null;

  try {
    const resp = await fetch(fullUrl);
    if (resp.ok) raw = await resp.text();
  } catch (_) {}

  if (!raw) {
    // Fallback: tenta metadata per sapere i chunk disponibili
    try {
      const meta = await fetch(`${DATA_BASE}/metadata.json`).then(r => r.json());
      console.warn(`[data_loader] ${varId}: _full.csv non trovato, usa merge locale`);
    } catch (_) {
      console.warn(`[data_loader] ${varId}: nessun dato trovato, uso placeholder`);
      return null;   // il dashboard continuerà con dati sintetici
    }
    return null;
  }

  // Parsing CSV semplice
  const lines = raw.trim().split("\n");
  const header = lines[0].split(",");
  const dateIdx = header.indexOf("date");
  const valIdx  = header.findIndex(h => h !== "date" && !h.endsWith("_stdDev"));
  const stdIdx  = header.findIndex(h => h.endsWith("_stdDev"));

  const rows = lines.slice(1).map(l => {
    const cols = l.split(",");
    return {
      date:  cols[dateIdx],
      value: parseFloat(cols[valIdx]),
      std:   stdIdx >= 0 ? parseFloat(cols[stdIdx]) : null,
    };
  }).filter(r => !isNaN(r.value));

  if (rows.length === 0) return null;

  // Aggregazione
  const aggregated = aggregate(rows, agg);
  const mean = aggregated.values.reduce((a, b) => a + b, 0) / aggregated.values.length;
  const anomalies = aggregated.values.map(v => +(v - mean).toFixed(decimals));

  return {
    labels:    aggregated.labels,
    values:    aggregated.values.map(v => +v.toFixed(decimals)),
    anomalies,
    mean,
    std:       aggregated.stds,
  };
}

/**
 * Aggrega un array di {date, value} in monthly / seasonal / annual.
 */
function aggregate(rows, agg) {
  const buckets = {};

  rows.forEach(r => {
    const d = new Date(r.date.includes("/") ? r.date.split("/")[0] : r.date);
    let key;
    if (agg === "annual") {
      key = d.getFullYear().toString();
    } else if (agg === "seasonal") {
      const m = d.getMonth();
      const season = m < 3 ? "Win" : m < 6 ? "Spr" : m < 9 ? "Sum" : "Aut";
      key = `${season} ${d.getFullYear().toString().slice(2)}`;
    } else {
      // monthly — usa anno+mese
      key = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,"0")}`;
    }
    if (!buckets[key]) buckets[key] = [];
    buckets[key].push(r.value);
  });

  const labels = Object.keys(buckets);
  const values = labels.map(k => {
    const arr = buckets[k];
    return arr.reduce((a, b) => a + b, 0) / arr.length;
  });
  const stds = labels.map(k => {
    const arr = buckets[k];
    const m = arr.reduce((a,b)=>a+b,0)/arr.length;
    return Math.sqrt(arr.reduce((a,b)=>a+(b-m)**2,0)/arr.length);
  });

  return { labels, values, stds };
}

/**
 * Carica metadata.json e aggiorna il badge "ultimo aggiornamento".
 */
async function loadMetadata() {
  try {
    const meta = await fetch(`${DATA_BASE}/metadata.json`).then(r => r.json());
    const el = document.getElementById("last-update");
    if (el && meta.last_update) {
      el.textContent = `↻ ${meta.last_update.slice(0, 10)}`;
    }
    return meta;
  } catch (_) {
    return null;
  }
}

/**
 * Patch del dashboard: sostituisce genTimeSeries con la versione
 * che carica dati reali, con fallback sintetico.
 * Chiama initDataLoader() dopo che il DOM è pronto.
 */
async function initDataLoader() {
  await loadMetadata();

  // Override della funzione sintetica nel dashboard
  window.genTimeSeries = async function(varId, agg) {
    const v = VARS.find(x => x.id === varId);
    const real = await loadVarData(varId, agg, v.decimals);
    if (real) return real;

    // Fallback sintetico (già definito in index.html)
    return genTimeSeriesSynthetic(varId, agg);
  };

  // Rinomina la funzione originale come fallback
  if (typeof genTimeSeries === "function") {
    window.genTimeSeriesSynthetic = genTimeSeries;
  }

  console.log("[data_loader] inizializzato — dati reali attivi se disponibili");
}

// Auto-init quando il DOM è pronto
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initDataLoader);
} else {
  initDataLoader();
}
