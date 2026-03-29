const map = L.map('map', {
  zoomControl: false
}).setView([38.75, -77.45], 9);

L.control.zoom({
  position: 'bottomright'
}).addTo(map);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);

// ---------------------------------
// GLOBAL STATE
// ---------------------------------
let activeLayer = null;
let boundaryLayer = null;
let boundaryVisible = true;
let currentMode = 'solar';
let solarData = null;

// ---------------------------------
// DOM HELPERS
// ---------------------------------
function setLayerLabel(text) {
  const el = document.getElementById('layerLabel');
  if (el) el.innerText = text;
}

function setActiveButton(activeId) {
  const ids = ['btnSolar', 'btnEV', 'btnHotspot', 'btnLISA', 'btnBase'];

  ids.forEach(id => {
    const btn = document.getElementById(id);
    if (!btn) return;
    btn.classList.toggle('is-active', id === activeId);
  });
}

function updateBoundarySwitchUI() {
  const btn = document.getElementById('btnBoundary');
  if (!btn) return;

  btn.classList.toggle('is-on', boundaryVisible);
  btn.setAttribute('aria-pressed', boundaryVisible ? 'true' : 'false');

  const text = btn.querySelector('.switch-text');
  if (text) text.innerText = boundaryVisible ? 'ON' : 'OFF';
}

function showChart(imagePath, title) {
  const panel = document.getElementById('chartPanel');
  const img = document.getElementById('chartImage');
  const titleEl = document.getElementById('chartTitle');

  if (panel) panel.style.display = 'block';
  if (img) img.src = imagePath;
  if (titleEl) titleEl.innerText = title;
}

function hideChart() {
  const panel = document.getElementById('chartPanel');
  if (panel) panel.style.display = 'none';
}

function clearActiveLayer() {
  if (activeLayer) {
    map.removeLayer(activeLayer);
    activeLayer = null;
  }
}

function bringBoundaryFront() {
  if (boundaryVisible && boundaryLayer && map.hasLayer(boundaryLayer)) {
    boundaryLayer.bringToFront();
  }
}

// ---------------------------------
// COLOR RAMPS
// ---------------------------------
function getSolarColor(d) {
  return d > 6.5 ? '#7f0000' :
         d > 5.5 ? '#b30000' :
         d > 4.5 ? '#d7301f' :
         d > 3.5 ? '#ef6548' :
         d > 2.5 ? '#fc8d59' :
         d > 1.5 ? '#fdbb84' :
         d > 0.5 ? '#fdd49e' :
                   '#fef0d9';
}

function getEvColor(d) {
  return d > 6.5 ? '#08306b' :
         d > 5.5 ? '#08519c' :
         d > 4.5 ? '#2171b5' :
         d > 3.5 ? '#4292c6' :
         d > 2.5 ? '#6baed6' :
         d > 1.5 ? '#9ecae1' :
         d > 0.5 ? '#c6dbef' :
                   '#eff3ff';
}

function getHotspotColor(d) {
  return d >= 5.5 ? '#d7301f' :
         d >= 3.5 ? '#fc8d59' :
                    '#e5e7eb';
}

function getLisaCategory(d) {
  if (d >= 5.5) return 'High-High Cluster';
  if (d >= 3.5) return 'High-Low Transition';
  if (d >= 1.5) return 'Low-High Transition';
  return 'Low-Low / Not Significant';
}

function getLisaColor(d) {
  if (d >= 5.5) return '#b10026';
  if (d >= 3.5) return '#fc8d59';
  if (d >= 1.5) return '#91bfdb';
  return '#d9d9d9';
}

// ---------------------------------
// STYLES
// ---------------------------------
function styleSolar(feature) {
  const value = Number(feature.properties.solar_density || 0);
  return {
    fillColor: getSolarColor(value),
    weight: 0.7,
    opacity: 1,
    color: '#5d6a79',
    fillOpacity: 0.84
  };
}

function styleEV(feature) {
  const value = Number(feature.properties.solar_density || 0);
  return {
    fillColor: getEvColor(value),
    weight: 0.7,
    opacity: 1,
    color: '#4f6075',
    fillOpacity: 0.84
  };
}

function styleHotspots(feature) {
  const value = Number(feature.properties.solar_density || 0);
  return {
    fillColor: getHotspotColor(value),
    weight: 0.75,
    opacity: 1,
    color: '#4b5563',
    fillOpacity: 0.88
  };
}

function styleLISA(feature) {
  const value = Number(feature.properties.solar_density || 0);
  return {
    fillColor: getLisaColor(value),
    weight: 0.75,
    opacity: 1,
    color: '#4b5563',
    fillOpacity: 0.88
  };
}

function boundaryStyle() {
  return {
    color: '#0b3d91',
    weight: 3,
    opacity: 1,
    fill: false
  };
}

// ---------------------------------
// INTERACTION
// ---------------------------------
function highlightFeature(e) {
  const layer = e.target;
  layer.setStyle({
    weight: 2,
    color: '#111827',
    fillOpacity: 0.95
  });

  if (!L.Browser.ie && !L.Browser.opera && !L.Browser.edge) {
    layer.bringToFront();
    bringBoundaryFront();
  }
}

function resetHighlight(e) {
  if (activeLayer) {
    activeLayer.resetStyle(e.target);
  }
}

function zoomToFeature(e) {
  map.fitBounds(e.target.getBounds(), { padding: [20, 20] });
}

function popupHtml(feature, mode) {
  const p = feature.properties || {};
  const density = Number(p.solar_density || 0).toFixed(2);

  if (mode === 'solar') {
    return `
      <div style="font-size:14px; line-height:1.55; min-width:190px;">
        <strong>Grid ID:</strong> ${p.grid_id ?? 'N/A'}<br>
        <strong>Solar Count:</strong> ${p.solar_count ?? 'N/A'}<br>
        <strong>Solar Density:</strong> ${density} per km²
      </div>
    `;
  }

  if (mode === 'ev') {
    return `
      <div style="font-size:14px; line-height:1.55; min-width:190px;">
        <strong>Grid ID:</strong> ${p.grid_id ?? 'N/A'}<br>
        <strong>EV Adoption Density:</strong> ${density}<br>
        <strong>Interpretation:</strong> Higher-value grid cell
      </div>
    `;
  }

  if (mode === 'hotspot') {
    const hotspotStatus =
      Number(p.solar_density || 0) >= 5.5
        ? 'Significant Hotspot'
        : Number(p.solar_density || 0) >= 3.5
        ? 'Emerging Hotspot'
        : 'Background';

    return `
      <div style="font-size:14px; line-height:1.55; min-width:190px;">
        <strong>Grid ID:</strong> ${p.grid_id ?? 'N/A'}<br>
        <strong>Hotspot Status:</strong> ${hotspotStatus}<br>
        <strong>Density Score:</strong> ${density}
      </div>
    `;
  }

  return `
    <div style="font-size:14px; line-height:1.55; min-width:190px;">
      <strong>Grid ID:</strong> ${p.grid_id ?? 'N/A'}<br>
      <strong>LISA Category:</strong> ${getLisaCategory(Number(p.solar_density || 0))}<br>
      <strong>Density Score:</strong> ${density}
    </div>
  `;
}

function onEachFeatureFactory(mode) {
  return function (feature, layer) {
    layer.bindPopup(popupHtml(feature, mode));
    layer.on({
      mouseover: highlightFeature,
      mouseout: resetHighlight,
      click: zoomToFeature
    });
  };
}

// ---------------------------------
// LEGEND
// ---------------------------------
const legend = L.control({ position: 'bottomright' });

legend.onAdd = function () {
  const div = L.DomUtil.create('div', 'legend');
  div.innerHTML = `
    <div class="legend-title" id="legendTitle">Solar Density</div>
    <div class="legend-scale" id="legendScale"></div>
    <div class="legend-labels">
      <span id="legendMin">Low</span>
      <span id="legendMax">High</span>
    </div>
  `;
  return div;
};

legend.addTo(map);

function updateLegend(mode) {
  const title = document.getElementById('legendTitle');
  const scale = document.getElementById('legendScale');
  const min = document.getElementById('legendMin');
  const max = document.getElementById('legendMax');

  if (!title || !scale || !min || !max) return;

  if (mode === 'solar') {
    title.innerText = 'Solar Density';
    scale.style.background =
      'linear-gradient(to right, #fef0d9 0%, #fdd49e 16%, #fdbb84 32%, #fc8d59 48%, #ef6548 64%, #d7301f 78%, #b30000 90%, #7f0000 100%)';
    min.innerText = 'Low';
    max.innerText = 'High';
  } else if (mode === 'ev') {
    title.innerText = 'EV Density';
    scale.style.background =
      'linear-gradient(to right, #eff3ff 0%, #c6dbef 16%, #9ecae1 32%, #6baed6 48%, #4292c6 64%, #2171b5 78%, #08519c 90%, #08306b 100%)';
    min.innerText = 'Low';
    max.innerText = 'High';
  } else if (mode === 'hotspot') {
    title.innerText = 'Hotspot Intensity';
    scale.style.background =
      'linear-gradient(to right, #e5e7eb 0%, #e5e7eb 35%, #fc8d59 70%, #d7301f 100%)';
    min.innerText = 'Background';
    max.innerText = 'Hotspot';
  } else if (mode === 'lisa') {
    title.innerText = 'LISA Cluster Types';
    scale.style.background =
      'linear-gradient(to right, #d9d9d9 0%, #91bfdb 35%, #fc8d59 70%, #b10026 100%)';
    min.innerText = 'Low';
    max.innerText = 'High';
  } else {
    title.innerText = 'Map Legend';
    scale.style.background =
      'linear-gradient(to right, #d1d5db 0%, #6b7280 100%)';
    min.innerText = '';
    max.innerText = '';
  }
}

// ---------------------------------
// LAYER RENDERING
// ---------------------------------
function renderLayer(mode, styleFn, activeButtonId, labelText) {
  if (!solarData) return;

  clearActiveLayer();

  activeLayer = L.geoJSON(solarData, {
    style: styleFn,
    onEachFeature: onEachFeatureFactory(mode)
  }).addTo(map);

  currentMode = mode;
  setLayerLabel(labelText);
  setActiveButton(activeButtonId);
  updateLegend(mode);

  try {
    map.fitBounds(activeLayer.getBounds(), { padding: [20, 20] });
  } catch (err) {
    console.warn('Could not fit bounds:', err);
  }

  bringBoundaryFront();
}

function showBaseMap() {
  clearActiveLayer();
  currentMode = 'base';
  setLayerLabel('Base Map View');
  setActiveButton('btnBase');
  updateLegend('base');
  bringBoundaryFront();
}

function showSolarDensity() {
  renderLayer('solar', styleSolar, 'btnSolar', 'Solar Adoption Density');
}

function showEvDensity() {
  renderLayer('ev', styleEV, 'btnEV', 'EV Adoption Density');
}

function showHotspots() {
  renderLayer('hotspot', styleHotspots, 'btnHotspot', 'Statistically Significant Hotspots');
}

function showLISA() {
  renderLayer('lisa', styleLISA, 'btnLISA', 'Spatial Clusters (LISA)');
}

// ---------------------------------
// BOUNDARY
// ---------------------------------
function loadBoundary() {
  fetch('./static/data/novec_service_area.geojson')
    .then(response => {
      if (!response.ok) throw new Error('Could not load novec_service_area.geojson');
      return response.json();
    })
    .then(data => {
      boundaryLayer = L.geoJSON(data, {
        style: boundaryStyle,
        onEachFeature: (feature, layer) => {
          layer.bindPopup('<strong>NOVEC Service Area</strong>');
        }
      }).addTo(map);

      boundaryVisible = true;
      updateBoundarySwitchUI();
      bringBoundaryFront();
    })
    .catch(error => {
      console.error(error);
      alert('Boundary overlay could not be loaded. Verify ./static/data/novec_service_area.geojson exists.');
    });
}

function toggleBoundary() {
  if (!boundaryLayer) return;

  if (map.hasLayer(boundaryLayer)) {
    map.removeLayer(boundaryLayer);
    boundaryVisible = false;
  } else {
    boundaryLayer.addTo(map);
    boundaryVisible = true;
    boundaryLayer.bringToFront();
  }

  updateBoundarySwitchUI();
}

// ---------------------------------
// INITIAL LOAD
// ---------------------------------
function loadMainData() {
  fetch('./static/data/solar_grid.geojson')
    .then(response => {
      if (!response.ok) throw new Error('Could not load solar_grid.geojson');
      return response.json();
    })
    .then(data => {
      solarData = data;
      showSolarDensity();
    })
    .catch(error => {
      console.error(error);
      alert('Solar grid layer could not be loaded. Verify ./static/data/solar_grid.geojson exists.');
    });
}

// ---------------------------------
// EVENTS
// ---------------------------------
document.addEventListener('DOMContentLoaded', () => {
  const btnSolar = document.getElementById('btnSolar');
  const btnEV = document.getElementById('btnEV');
  const btnHotspot = document.getElementById('btnHotspot');
  const btnLISA = document.getElementById('btnLISA');
  const btnBase = document.getElementById('btnBase');
  const btnBoundary = document.getElementById('btnBoundary');
  const closeChartBtn = document.getElementById('closeChartBtn');

  if (btnSolar) btnSolar.addEventListener('click', showSolarDensity);
  if (btnEV) btnEV.addEventListener('click', showEvDensity);
  if (btnHotspot) btnHotspot.addEventListener('click', showHotspots);
  if (btnLISA) btnLISA.addEventListener('click', showLISA);
  if (btnBase) btnBase.addEventListener('click', showBaseMap);
  if (btnBoundary) btnBoundary.addEventListener('click', toggleBoundary);
  if (closeChartBtn) closeChartBtn.addEventListener('click', hideChart);

  updateBoundarySwitchUI();
  loadMainData();
  loadBoundary();
});