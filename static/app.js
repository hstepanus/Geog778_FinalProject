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
    props.hotspotClass ??
    "Neutral"
  ).trim();
}

function getLISAClass(props = {}) {
  return String(
    props.lisa_cluster ??
    props.cluster ??
    props.lisa ??
    props.lisaClass ??
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
  if (layer && !map.hasLayer(layer)) {
    layer.addTo(map);
  }
  keepVisualOrder();
  setActiveViewLabel();
  updateLegend();
}

function removeLayerFromMap(layer) {
  if (layer && map.hasLayer(layer)) {
    map.removeLayer(layer);
  }
  keepVisualOrder();
  setActiveViewLabel();
  updateLegend();
}

function loadSolarLayer() {
  return fetch(dataFiles.solar)
    .then(r => {
      if (!r.ok) throw new Error("Could not load solar_grid.geojson");
      return r.json();
    })
    .then(data => {
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
      console.log("LISA sample properties:", data?.features?.[0]?.properties);

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
          layer.bindPopup(
            popupHTML("NOVEC Service Area", ["Boundary reference overlay"])
          );
        }
      }).addTo(map);

      keepVisualOrder();
      setActiveViewLabel();
      updateLegend();
    })
    .catch(err => {
      console.error(err);
      alert("Boundary layer could not be loaded.");
    });
}

const legend = L.control({ position: "bottomright" });

legend.onAdd = function () {
  const div = L.DomUtil.create("div", "legend");
  div.id = "mapLegend";
  div.innerHTML = "";
  return div;
};

legend.addTo(map);

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
      <div class="legend-row"><span class="legend-swatch" style="background:#d9dee7"></span>Not Significant</div>
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
      <div class="legend-note">Estimated EV density per square kilometer.</div>
    `;
    return;
  }

  if (solarVisible) {
    div.innerHTML = `
      <div class="legend-title">Solar Density</div>
      <div class="legend-gradient" style="background:linear-gradient(to right,#fff8e1 0%,#ffe7a3 16%,#ffc66d 32%,#ff9c51 48%,#f06a3d 64%,#d9432f 78%,#b51f1f 90%,#7f0000 100%);"></div>
      <div class="legend-labels"><span>Low</span><span>High</span></div>
      <div class="legend-note">Grid-based solar adoption density surface.</div>
    `;
    return;
  }

  div.innerHTML = `
    <div class="legend-title">Map View</div>
    <div class="legend-note">Turn on a layer from the control panel to view symbology.</div>
  `;
}

function wireToggle(id, onEnable, onDisable) {
  const el = document.getElementById(id);
  if (!el) return;

  el.addEventListener("change", async (e) => {
    const checked = e.target.checked;
    try {
      if (checked) {
        await onEnable();
      } else {
        onDisable();
      }
      keepVisualOrder();
      setActiveViewLabel();
      updateLegend();
    } catch (err) {
      console.error(err);
    }
  });
}

async function enableSolar() {
  if (!solarLayer) {
    await loadSolarLayer();
  } else {
    addLayerToMap(solarLayer);
  }
}

async function enableEV() {
  if (!evLayer) {
    await loadEVLayer();
  }
  addLayerToMap(evLayer);
}

async function enableHotspots() {
  if (!hotspotLayer) {
    await loadHotspotLayer();
  }
  addLayerToMap(hotspotLayer);
}

async function enableLISA() {
  if (!lisaLayer) {
    await loadLISALayer();
  }
  addLayerToMap(lisaLayer);
}

async function enableBoundary() {
  if (!boundaryLayer) {
    await loadBoundary();
  } else if (!map.hasLayer(boundaryLayer)) {
    boundaryLayer.addTo(map);
  }
  keepVisualOrder();
  setActiveViewLabel();
  updateLegend();
}

function disableBoundary() {
  if (boundaryLayer && map.hasLayer(boundaryLayer)) {
    map.removeLayer(boundaryLayer);
  }
  setActiveViewLabel();
  updateLegend();
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
}

initializeDashboard();