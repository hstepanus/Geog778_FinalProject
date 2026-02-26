let map, zipsLayer, novecLayer;
let latestGeoJSON = null;

function fmt(n, digits=2) {
  if (n === null || n === undefined) return "";
  const x = Number(n);
  if (!isFinite(x)) return "";
  return x.toFixed(digits);
}

async function fetchJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return await r.json();
}

function styleByScore(feature) {
  const p = feature.properties || {};
  const s = Number(p.adoption_score || 0);
  // Keep it simple: derive opacity from score (no fixed colors requested)
  return {
    weight: 1,
    fillOpacity: Math.max(0.1, Math.min(0.8, 0.1 + s * 0.7))
  };
}

function popupHTML(p) {
  return `
    <div style="font-family:system-ui; font-size:12px; line-height:1.4">
      <div style="font-weight:700; font-size:14px">ZIP ${p.zip}</div>
      <div><b>Adoption score:</b> ${fmt(p.adoption_score, 3)}</div>
      <hr/>
      <div><b>Solar customers:</b> ${p.solar_customers}</div>
      <div><b>EV accounts (points):</b> ${p.ev_accounts_pts}</div>
      <div><b>Superchargers:</b> ${p.superchargers}</div>
      <div><b>EV by ZIP:</b> ${p.ev_by_zip_count}</div>
      <hr/>
      <div><b>Solar/km²:</b> ${fmt(p.solar_per_km2, 2)}</div>
      <div><b>EV/km²:</b> ${fmt(p.evpts_per_km2, 2)}</div>
      <div><b>Chargers/km²:</b> ${fmt(p.super_per_km2, 2)}</div>
      <div><b>Area km²:</b> ${fmt(p.area_km2, 2)}</div>
    </div>
  `;
}

async function loadBoundary() {
  const fc = await fetchJSON("/api/novec/boundary");
  if (novecLayer) novecLayer.remove();
  novecLayer = L.geoJSON(fc, { weight: 2, fillOpacity: 0 }).addTo(map);
  map.fitBounds(novecLayer.getBounds(), { padding: [10, 10] });
}

async function loadSummary() {
  const s = await fetchJSON("/api/summary");
  const el = document.getElementById("summary");
  el.innerHTML = `
    <div><b>ZIPs:</b> ${s.zip_count}</div>
    <div><b>Total solar customers:</b> ${s.total_solar_customers}</div>
    <div><b>Total EV points:</b> ${s.total_ev_points}</div>
    <div><b>Total superchargers:</b> ${s.total_superchargers}</div>
    <div><b>Average score:</b> ${fmt(s.avg_score, 3)}</div>
  `;
}

function renderTopTable(fc) {
  const feats = (fc.features || []).slice()
    .sort((a,b) => (b.properties.adoption_score||0) - (a.properties.adoption_score||0))
    .slice(0, 12);

  const el = document.getElementById("topTable");
  el.innerHTML = feats.map(f => {
    const p = f.properties;
    return `<div class="row">
      <div class="zip">${p.zip}</div>
      <div>score ${fmt(p.adoption_score,3)} • EV/km² ${fmt(p.evpts_per_km2,1)} • Solar/km² ${fmt(p.solar_per_km2,1)}</div>
    </div>`;
  }).join("");
}

async function loadZips() {
  const tech = document.getElementById("tech").value;
  const minScore = Number(document.getElementById("minScore").value);
  const minValue = Number(document.getElementById("minValue").value);

  const url = `/api/zips?min_score=${encodeURIComponent(minScore)}&tech=${encodeURIComponent(tech)}&min_value=${encodeURIComponent(minValue)}`;
  const fc = await fetchJSON(url);
  latestGeoJSON = fc;

  if (zipsLayer) zipsLayer.remove();
  zipsLayer = L.geoJSON(fc, {
    style: styleByScore,
    onEachFeature: (feature, layer) => {
      const p = feature.properties || {};
      layer.bindPopup(popupHTML(p));
      layer.on("mouseover", () => layer.setStyle({ weight: 2 }));
      layer.on("mouseout", () => layer.setStyle({ weight: 1 }));
    }
  }).addTo(map);

  renderTopTable(fc);
}

function zoomToZip(zip) {
  if (!latestGeoJSON) return;
  const feat = (latestGeoJSON.features || []).find(f => String(f.properties.zip) === String(zip));
  if (!feat) return alert(`ZIP ${zip} not found in current filter.`);
  const layer = L.geoJSON(feat);
  map.fitBounds(layer.getBounds(), { padding: [20, 20] });
}

async function buildPipeline() {
  const btn = document.getElementById("btnBuild");
  btn.disabled = true;
  btn.textContent = "Building...";
  try {
    const r = await fetch("/api/pipeline/build", { method: "POST" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    await loadSummary();
    await loadBoundary();
    await loadZips();
  } catch (e) {
    alert(`Build failed: ${e.message}`);
  } finally {
    btn.disabled = false;
    btn.textContent = "Build/Refresh Metrics";
  }
}

function init() {
  map = L.map("map").setView([38.8, -77.4], 9);

  // Base map (OpenStreetMap). If you must use Esri basemap later, swap URL.
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: "© OpenStreetMap"
  }).addTo(map);

  // UI wires
  const minScore = document.getElementById("minScore");
  const minScoreVal = document.getElementById("minScoreVal");
  minScore.addEventListener("input", () => minScoreVal.textContent = minScore.value);

  document.getElementById("btnApply").addEventListener("click", async () => {
    await loadZips();
  });

  document.getElementById("btnBuild").addEventListener("click", buildPipeline);

  document.getElementById("btnExport").addEventListener("click", () => {
    const minScore = Number(document.getElementById("minScore").value);
    window.location.href = `/api/export/csv?min_score=${encodeURIComponent(minScore)}`;
  });

  document.getElementById("btnSearch").addEventListener("click", () => {
    const zip = document.getElementById("zipSearch").value.trim();
    if (!zip) return;
    zoomToZip(zip);
  });

  // Initial load (assumes metrics already exist; if not, click Build)
  loadSummary().catch(()=>{});
  loadBoundary().catch(()=>{});
  loadZips().catch(()=>{});
}

init();