const map = L.map("map", {
  zoomControl: false
}).setView([38.75, -77.45], 9);

L.control.zoom({ position: "bottomright" }).addTo(map);

L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
  attribution: "&copy; OpenStreetMap & CARTO"
}).addTo(map);

let solarLayer = null;
let evLayer = null;
let hotspotLayer = null;
let lisaLayer = null;
let boundaryLayer = null;

let solarData = null;
let evData = null;
let hotspotData = null;
let lisaData = null;

const dataFiles = {
  solar: "./static/data/solar_grid.geojson",
  ev: "./static/data/ev_grid.geojson",
  hotspots: "./static/data/hotspots.geojson",
  lisa: "./static/data/lisa_clusters.geojson",
  boundary: "./static/data/novec_service_area.geojson"
};

function safeNumber(value, digits = 2) {
  const n = Number(value);
  return Number.isFinite(n) ? n.toFixed(digits) : "0.00";
}

function safeInt(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.round(n).toLocaleString() : "0";
}

function popupHTML(title, rows) {
  return `
    <div style="font-size:14px; line-height:1.6; min-width:190px; color:#1f2937;">
      <div style="font-size:15px; font-weight:800; color:#102b5c; margin-bottom:6px;">
        ${title}
      </div>
      ${rows.join("<br>")}
    </div>
  `;
}

function getSolarColor(d) {
  return d > 6.5 ? "#7f0000" :
         d > 5.5 ? "#b51f1f" :
         d > 4.5 ? "#d9432f" :
         d > 3.5 ? "#f06a3d" :
         d > 2.5 ? "#ff9c51" :
         d > 1.5 ? "#ffc66d" :
         d > 0.5 ? "#ffe7a3" :
                   "#fff8e1";
}

function getEVDensityColor(d) {
  return d > 1.0 ? "#0b3b8c" :
         d > 0.7 ? "#1553b7" :
         d > 0.4 ? "#2f6fe4" :
         d > 0.2 ? "#5c91ee" :
         d > 0.1 ? "#9cc0f8" :
                   "#e5f0ff";
}

function getHotspotClass(props = {}) {
  return String(
    props.hotspot_class ??
    props.class ??
    props.hotspot ??
    "Neutral"
  ).trim();
}

function getLISAClass(props = {}) {
  return String(
    props.lisa_cluster ??
    props.cluster ??
    props.lisa ??
    "Not Significant"
  ).trim();
}

function getHotspotColor(props = {}) {
  const cls = getHotspotClass(props);
  if (cls === "Hot Spot") return "#d73027";
  if (cls === "Cold Spot") return "#4575b4";
  return "#d9dee7";
}

function getLISAColor(props = {}) {
  const cls = getLISAClass(props);
  if (cls === "High-High") return "#d73027";
  if (cls === "Low-Low") return "#4575b4";
  if (cls === "High-Low") return "#fdae61";
  if (cls === "Low-High") return "#66bd63";
  return "#d9dee7";
}

function setActiveViewLabel() {
  const active = [];
  if (solarLayer && map.hasLayer(solarLayer)) active.push("Solar Density");
  if (evLayer && map.hasLayer(evLayer)) active.push("EV Density");
  if (hotspotLayer && map.hasLayer(hotspotLayer)) active.push("Hotspots");
  if (lisaLayer && map.hasLayer(lisaLayer)) active.push("LISA Clusters");

  const text = active.length ? active.join(" + ") : "Base Map View";

  const label = document.getElementById("layerLabel");
  const metric = document.getElementById("activeViewText");

  if (label) label.innerText = text;
  if (metric) metric.innerText = text;
}

function keepVisualOrder() {
  if (solarLayer && map.hasLayer(solarLayer)) solarLayer.bringToBack();
  if (evLayer && map.hasLayer(evLayer)) evLayer.bringToFront();
  if (hotspotLayer && map.hasLayer(hotspotLayer)) hotspotLayer.bringToFront();
  if (lisaLayer && map.hasLayer(lisaLayer)) lisaLayer.bringToFront();
  if (boundaryLayer && map.hasLayer(boundaryLayer)) boundaryLayer.bringToFront();
}

function highlightFeature(e) {
  const layer = e.target;
  layer.setStyle({
    weight: 1.5,
    color: "#111827",
    fillOpacity: 1
  });

  if (!L.Browser.ie && !L.Browser.opera && !L.Browser.edge) {
    layer.bringToFront();
  }

  if (boundaryLayer && map.hasLayer(boundaryLayer)) {
    boundaryLayer.bringToFront();
  }
}

function resetFeatureStyle(e) {
  const layer = e.target;

  if (solarLayer && solarLayer.hasLayer(layer)) solarLayer.resetStyle(layer);
  if (evLayer && evLayer.hasLayer(layer)) evLayer.resetStyle(layer);
  if (hotspotLayer && hotspotLayer.hasLayer(layer)) hotspotLayer.resetStyle(layer);
  if (lisaLayer && lisaLayer.hasLayer(layer)) lisaLayer.resetStyle(layer);

  keepVisualOrder();
}

function zoomToFeature(e) {
  map.fitBounds(e.target.getBounds(), { padding: [20, 20] });
}

function bindInteractiveEvents(layer) {
  layer.on({
    mouseover: highlightFeature,
    mouseout: resetFeatureStyle,
    click: zoomToFeature
  });
}

function addLayerToMap(layer) {
  if (layer && !map.hasLayer(layer)) layer.addTo(map);
  keepVisualOrder();
  setActiveViewLabel();
  updateLegend();
  updateAnalysisPanel();
}

function removeLayerFromMap(layer) {
  if (layer && map.hasLayer(layer)) map.removeLayer(layer);
  keepVisualOrder();
  setActiveViewLabel();
  updateLegend();
  updateAnalysisPanel();
}

function layerIsVisible(layer) {
  return !!(layer && map.hasLayer(layer));
}

function getActiveLayerName() {
  if (layerIsVisible(lisaLayer)) return "lisa";
  if (layerIsVisible(hotspotLayer)) return "hotspots";
  if (layerIsVisible(evLayer)) return "ev";
  if (layerIsVisible(solarLayer)) return "solar";
  return "base";
}

function createStatCard(label, value) {
  return `
    <div class="analysis-stat">
      <div class="analysis-stat-label">${label}</div>
      <div class="analysis-stat-value">${value}</div>
    </div>
  `;
}

function mean(values) {
  if (!values.length) return 0;
  return values.reduce((a, b) => a + b, 0) / values.length;
}

function countBy(features, getter) {
  const counts = {};
  features.forEach(f => {
    const key = getter(f);
    counts[key] = (counts[key] || 0) + 1;
  });
  return counts;
}

function topValue(features, getter) {
  let top = null;
  features.forEach(f => {
    const val = Number(getter(f));
    if (!Number.isFinite(val)) return;
    if (!top || val > top.value) {
      top = {
        value: val,
        gridId: f?.properties?.grid_id ?? "N/A"
      };
    }
  });
  return top;
}

function updateAnalysisPanel() {
  const titleEl = document.getElementById("analysisTitle");
  const subtitleEl = document.getElementById("analysisSubtitle");
  const gridEl = document.getElementById("analysisGrid");
  const insightEl = document.getElementById("analysisInsight");

  if (!titleEl || !subtitleEl || !gridEl || !insightEl) return;

  const active = getActiveLayerName();

  if (active === "solar" && solarData?.features?.length) {
    const features = solarData.features;
    const densities = features.map(f => Number(f?.properties?.solar_density)).filter(Number.isFinite);
    const counts = features.map(f => Number(f?.properties?.solar_count)).filter(Number.isFinite);
    const top = topValue(features, f => f?.properties?.solar_density);

    titleEl.innerText = "Solar Analytical Summary";
    subtitleEl.innerText = "Grid-based adoption surface";
    gridEl.innerHTML = [
      createStatCard("Grid Cells", safeInt(features.length)),
      createStatCard("Mean Density", safeNumber(mean(densities))),
      createStatCard("Total Solar", safeInt(counts.reduce((a, b) => a + b, 0))),
      createStatCard("Top Grid", top ? `#${top.gridId}` : "N/A")
    ].join("");

    insightEl.innerText = top
      ? `Highest solar density appears in grid ${top.gridId} at approximately ${safeNumber(top.value)} installs per km².`
      : "Solar density metrics are available once the layer is loaded.";
    return;
  }

  if (active === "ev" && evData?.features?.length) {
    const features = evData.features;
    const densities = features.map(f => Number(f?.properties?.ev_density)).filter(Number.isFinite);
    const top = topValue(features, f => f?.properties?.ev_density);

    titleEl.innerText = "EV Analytical Summary";
    subtitleEl.innerText = "Weighted EV density surface";
    gridEl.innerHTML = [
      createStatCard("Grid Cells", safeInt(features.length)),
      createStatCard("Mean Density", safeNumber(mean(densities))),
      createStatCard("Max Density", top ? safeNumber(top.value) : "0.00"),
      createStatCard("Top Grid", top ? `#${top.gridId}` : "N/A")
    ].join("");

    insightEl.innerText = top
      ? `Grid ${top.gridId} has the highest estimated EV density at ${safeNumber(top.value)} per km², suggesting a strong candidate area for infrastructure planning.`
      : "EV density metrics are available once the layer is loaded.";
    return;
  }

  if (active === "hotspots" && hotspotData?.features?.length) {
    const features = hotspotData.features;
    const counts = countBy(features, f => getHotspotClass(f.properties || {}));
    const hot = counts["Hot Spot"] || 0;
    const cold = counts["Cold Spot"] || 0;
    const neutral = counts["Neutral"] || 0;

    titleEl.innerText = "Hotspot Analytical Summary";
    subtitleEl.innerText = "Local spatial concentration classes";
    gridEl.innerHTML = [
      createStatCard("Hot Spots", safeInt(hot)),
      createStatCard("Cold Spots", safeInt(cold)),
      createStatCard("Neutral", safeInt(neutral)),
      createStatCard("Total Cells", safeInt(features.length))
    ].join("");

    insightEl.innerText = `Hot spots (${safeInt(hot)}) indicate stronger local clustering, while cold spots (${safeInt(cold)}) indicate weaker surrounding density patterns.`;
    return;
  }

  if (active === "lisa" && lisaData?.features?.length) {
    const features = lisaData.features;
    const counts = countBy(features, f => getLISAClass(f.properties || {}));
    const hh = counts["High-High"] || 0;
    const ll = counts["Low-Low"] || 0;
    const hl = counts["High-Low"] || 0;
    const lh = counts["Low-High"] || 0;

    titleEl.innerText = "LISA Analytical Summary";
    subtitleEl.innerText = "Cluster cores and spatial outliers";
    gridEl.innerHTML = [
      createStatCard("High-High", safeInt(hh)),
      createStatCard("Low-Low", safeInt(ll)),
      createStatCard("High-Low", safeInt(hl)),
      createStatCard("Low-High", safeInt(lh))
    ].join("");

    insightEl.innerText = `High-High clusters (${safeInt(hh)}) indicate stable adoption cores, while High-Low and Low-High cells suggest local spatial outliers.`;
    return;
  }

  titleEl.innerText = "Analytical Summary";
  subtitleEl.innerText = "Active layer metrics";
  gridEl.innerHTML = [
    createStatCard("Solar", "Off"),
    createStatCard("EV", "Off"),
    createStatCard("Hotspots", "Off"),
    createStatCard("LISA", "Off")
  ].join("");
  insightEl.innerText = "Turn on a layer to see analytical context and summary counts.";
}

function loadSolarLayer() {
  return fetch(dataFiles.solar)
    .then(r => {
      if (!r.ok) throw new Error("Could not load solar_grid.geojson");
      return r.json();
    })
    .then(data => {
      solarData = data;
      solarLayer = L.geoJSON(data, {
        style: feature => ({
          fillColor: getSolarColor(Number(feature?.properties?.solar_density || 0)),
          weight: 0.5,
          color: "#a8b2c0",
          fillOpacity: 0.55,
          opacity: 1
        }),
        onEachFeature: (feature, layer) => {
          const p = feature.properties || {};
          layer.bindPopup(
            popupHTML("Solar Density Cell", [
              `<strong>Grid ID:</strong> ${p.grid_id ?? "N/A"}`,
              `<strong>Solar Count:</strong> ${p.solar_count ?? 0}`,
              `<strong>Density:</strong> ${safeNumber(p.solar_density)} / km²`
            ])
          );
          bindInteractiveEvents(layer);
        }
      });

      addLayerToMap(solarLayer);

      try {
        map.fitBounds(solarLayer.getBounds(), { padding: [20, 20] });
      } catch (err) {}
    })
    .catch(err => {
      console.error(err);
      alert("Solar layer could not be loaded.");
    });
}

function loadEVLayer() {
  return fetch(dataFiles.ev)
    .then(r => {
      if (!r.ok) throw new Error("Could not load ev_grid.geojson");
      return r.json();
    })
    .then(data => {
      evData = data;
      evLayer = L.geoJSON(data, {
        style: feature => ({
          fillColor: getEVDensityColor(Number(feature?.properties?.ev_density || 0)),
          weight: 0.5,
          color: "#9ab0cf",
          fillOpacity: 0.72,
          opacity: 1
        }),
        onEachFeature: (feature, layer) => {
          const p = feature.properties || {};
          layer.bindPopup(
            popupHTML("EV Density Cell", [
              `<strong>Grid ID:</strong> ${p.grid_id ?? "N/A"}`,
              `<strong>EV Density:</strong> ${safeNumber(p.ev_density)} / km²`
            ])
          );
          bindInteractiveEvents(layer);
        }
      });
    })
    .catch(err => {
      console.error(err);
      alert("EV layer could not be loaded.");
    });
}

function loadHotspotLayer() {
  return fetch(dataFiles.hotspots)
    .then(r => {
      if (!r.ok) throw new Error("Could not load hotspots.geojson");
      return r.json();
    })
    .then(data => {
      hotspotData = data;
      hotspotLayer = L.geoJSON(data, {
        style: feature => ({
          fillColor: getHotspotColor(feature.properties || {}),
          weight: 0.35,
          color: "#ffffff",
          fillOpacity: 0.9,
          opacity: 1
        }),
        onEachFeature: (feature, layer) => {
          const p = feature.properties || {};
          layer.bindPopup(
            popupHTML("Hotspot Analysis", [
              `<strong>Grid ID:</strong> ${p.grid_id ?? "N/A"}`,
              `<strong>Class:</strong> ${getHotspotClass(p)}`
            ])
          );
          bindInteractiveEvents(layer);
        }
      });
    })
    .catch(err => {
      console.error(err);
      alert("Hotspot layer could not be loaded.");
    });
}

function loadLISALayer() {
  return fetch(dataFiles.lisa)
    .then(r => {
      if (!r.ok) throw new Error("Could not load lisa_clusters.geojson");
      return r.json();
    })
    .then(data => {
      lisaData = data;
      lisaLayer = L.geoJSON(data, {
        style: feature => ({
          fillColor: getLISAColor(feature.properties || {}),
          weight: 0.35,
          color: "#ffffff",
          fillOpacity: 0.92,
          opacity: 1
        }),
        onEachFeature: (feature, layer) => {
          const p = feature.properties || {};
          layer.bindPopup(
            popupHTML("LISA Cluster", [
              `<strong>Grid ID:</strong> ${p.grid_id ?? "N/A"}`,
              `<strong>Cluster:</strong> ${getLISAClass(p)}`
            ])
          );
          bindInteractiveEvents(layer);
        }
      });
    })
    .catch(err => {
      console.error(err);
      alert("LISA layer could not be loaded.");
    });
}

function loadBoundary() {
  return fetch(dataFiles.boundary)
    .then(r => {
      if (!r.ok) throw new Error("Could not load novec_service_area.geojson");
      return r.json();
    })
    .then(data => {
      boundaryLayer = L.geoJSON(data, {
        style: {
          color: "#123d87",
          weight: 2.2,
          opacity: 1,
          fill: false,
          dashArray: "4,4"
        },
        onEachFeature: (feature, layer) => {
          layer.bindPopup(popupHTML("NOVEC Service Area", ["Boundary reference overlay"]));
        }
      }).addTo(map);

      keepVisualOrder();
      setActiveViewLabel();
      updateLegend();
      updateAnalysisPanel();
    })
    .catch(err => {
      console.error(err);
      alert("Boundary layer could not be loaded.");
    });
}

function showChart(imagePath, title) {
  const panel = document.getElementById("chartPanel");
  const image = document.getElementById("chartImage");
  const text = document.getElementById("chartTitle");

  if (!panel || !image || !text) return;

  panel.style.display = "block";
  image.src = imagePath;
  image.alt = title;
  text.innerText = title;
}

function hideChart() {
  const panel = document.getElementById("chartPanel");
  if (panel) panel.style.display = "none";
}

const legend = L.control({ position: "bottomright" });

legend.onAdd = function () {
  const div = L.DomUtil.create("div", "legend");
  div.id = "mapLegend";
  div.innerHTML = "";
  return div;
};

//legend.addTo(map);

function updateLegend() {
  const div = document.getElementById("mapLegend");
  if (!div) return;

  const solarVisible = solarLayer && map.hasLayer(solarLayer);
  const evVisible = evLayer && map.hasLayer(evLayer);
  const hotspotVisible = hotspotLayer && map.hasLayer(hotspotLayer);
  const lisaVisible = lisaLayer && map.hasLayer(lisaLayer);

  if (lisaVisible) {
    div.innerHTML = `
      <div class="legend-title">LISA Clusters</div>
      <div class="legend-row"><span class="legend-swatch" style="background:#d73027"></span>High-High</div>
      <div class="legend-row"><span class="legend-swatch" style="background:#4575b4"></span>Low-Low</div>
      <div class="legend-row"><span class="legend-swatch" style="background:#fdae61"></span>High-Low</div>
      <div class="legend-row"><span class="legend-swatch" style="background:#66bd63"></span>Low-High</div>
    `;
    return;
  }

  if (hotspotVisible) {
    div.innerHTML = `
      <div class="legend-title">Hotspot Classes</div>
      <div class="legend-row"><span class="legend-swatch" style="background:#d73027"></span>Hot Spot</div>
      <div class="legend-row"><span class="legend-swatch" style="background:#4575b4"></span>Cold Spot</div>
      <div class="legend-row"><span class="legend-swatch" style="background:#d9dee7"></span>Neutral</div>
    `;
    return;
  }

  if (evVisible) {
    div.innerHTML = `
      <div class="legend-title">EV Density</div>
      <div class="legend-gradient" style="background:linear-gradient(to right,#e5f0ff 0%,#9cc0f8 25%,#5c91ee 50%,#2f6fe4 75%,#0b3b8c 100%);"></div>
      <div class="legend-labels"><span>Low</span><span>High</span></div>
    `;
    return;
  }

  if (solarVisible) {
    div.innerHTML = `
      <div class="legend-title">Solar Density</div>
      <div class="legend-gradient" style="background:linear-gradient(to right,#fff8e1 0%,#ffe7a3 16%,#ffc66d 32%,#ff9c51 48%,#f06a3d 64%,#d9432f 78%,#b51f1f 90%,#7f0000 100%);"></div>
      <div class="legend-labels"><span>Low</span><span>High</span></div>
    `;
    return;
  }

  div.innerHTML = `
    <div class="legend-title">Map View</div>
    <div class="legend-note">Turn on a layer from the control panel.</div>
  `;
}

function wireToggle(id, onEnable, onDisable) {
  const el = document.getElementById(id);
  if (!el) return;

  el.addEventListener("change", async (e) => {
    try {
      if (e.target.checked) {
        await onEnable();
      } else {
        onDisable();
      }
      keepVisualOrder();
      setActiveViewLabel();
      updateLegend();
      updateAnalysisPanel();
    } catch (err) {
      console.error(err);
    }
  });
}

async function enableSolar() {
  if (!solarLayer) await loadSolarLayer();
  else addLayerToMap(solarLayer);
}

async function enableEV() {
  if (!evLayer) await loadEVLayer();
  addLayerToMap(evLayer);
}

async function enableHotspots() {
  if (!hotspotLayer) await loadHotspotLayer();
  addLayerToMap(hotspotLayer);
}

async function enableLISA() {
  if (!lisaLayer) await loadLISALayer();
  addLayerToMap(lisaLayer);
}

async function enableBoundary() {
  if (!boundaryLayer) await loadBoundary();
  else if (!map.hasLayer(boundaryLayer)) boundaryLayer.addTo(map);
  keepVisualOrder();
  setActiveViewLabel();
  updateLegend();
  updateAnalysisPanel();
}

function disableBoundary() {
  if (boundaryLayer && map.hasLayer(boundaryLayer)) {
    map.removeLayer(boundaryLayer);
  }
  setActiveViewLabel();
  updateLegend();
  updateAnalysisPanel();
}

wireToggle("toggleSolar", enableSolar, () => removeLayerFromMap(solarLayer));
wireToggle("toggleEV", enableEV, () => removeLayerFromMap(evLayer));
wireToggle("toggleHotspots", enableHotspots, () => removeLayerFromMap(hotspotLayer));
wireToggle("toggleLISA", enableLISA, () => removeLayerFromMap(lisaLayer));
wireToggle("toggleBoundary", enableBoundary, disableBoundary);

async function initializeDashboard() {
  await Promise.all([
    loadBoundary(),
    loadSolarLayer(),
    loadEVLayer(),
    loadHotspotLayer(),
    loadLISALayer()
  ]);

  removeLayerFromMap(evLayer);
  removeLayerFromMap(hotspotLayer);
  removeLayerFromMap(lisaLayer);

  const toggleSolar = document.getElementById("toggleSolar");
  const toggleEV = document.getElementById("toggleEV");
  const toggleHotspots = document.getElementById("toggleHotspots");
  const toggleLISA = document.getElementById("toggleLISA");
  const toggleBoundary = document.getElementById("toggleBoundary");

  if (toggleSolar) toggleSolar.checked = true;
  if (toggleEV) toggleEV.checked = false;
  if (toggleHotspots) toggleHotspots.checked = false;
  if (toggleLISA) toggleLISA.checked = false;
  if (toggleBoundary) toggleBoundary.checked = true;

  keepVisualOrder();
  setActiveViewLabel();
  updateLegend();
  updateAnalysisPanel();
}

initializeDashboard();