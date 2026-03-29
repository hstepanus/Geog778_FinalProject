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
let boundaryVisible = true;
let currentLayerType = "solar";

const layerFiles = {
  solar: "./static/data/solar_grid.geojson",
  ev: "./static/data/ev_grid.geojson",
  hotspots: "./static/data/hotspots.geojson",
  lisa: "./static/data/lisa_clusters.geojson",
  boundary: "./static/data/novec_service_area.geojson"
};

function setLayerLabel(text) {
  const el = document.getElementById("layerLabel");
  const metric = document.getElementById("activeMetric");
  if (el) el.innerText = text;
  if (metric) metric.innerText = text;
}

function syncToggles(activeLayer) {
  const ids = {
    base: "toggleBaseMap",
    solar: "toggleSolar",
    ev: "toggleEV",
    hotspots: "toggleHotspots",
    lisa: "toggleLISA"
  };

  Object.entries(ids).forEach(([key, id]) => {
    const el = document.getElementById(id);
    if (el) el.checked = key === activeLayer;
  });

  const boundaryToggle = document.getElementById("toggleBoundaryLayer");
  if (boundaryToggle) boundaryToggle.checked = boundaryVisible;
}

function getSolarColor(value) {
  return value > 6.5 ? "#7f0000" :
         value > 5.5 ? "#b51f1f" :
         value > 4.5 ? "#d9432f" :
         value > 3.5 ? "#f06a3d" :
         value > 2.5 ? "#ff9c51" :
         value > 1.5 ? "#ffc66d" :
         value > 0.5 ? "#ffe7a3" :
                       "#fff8e1";
}

function getEVColor(value) {
  return value > 6.5 ? "#0b3b8c" :
         value > 5.5 ? "#1553b7" :
         value > 4.5 ? "#2f6fe4" :
         value > 3.5 ? "#5d93f0" :
         value > 2.5 ? "#8bb4f7" :
         value > 1.5 ? "#b7d0fb" :
         value > 0.5 ? "#dbe8ff" :
                       "#f2f7ff";
}

function getHotspotStyle(feature) {
  const p = feature.properties || {};
  const gi = Number(p.gi_bin || p.hotspot_bin || 0);

  if (gi >= 3) {
    return {
      fillColor: "#c62828",
      color: "#8e1f1f",
      weight: 1,
      fillOpacity: 0.78
    };
  }

  if (gi === 2) {
    return {
      fillColor: "#ef5350",
      color: "#b53a37",
      weight: 1,
      fillOpacity: 0.74
    };
  }

  if (gi === 1) {
    return {
      fillColor: "#ffb3b3",
      color: "#d66a6a",
      weight: 1,
      fillOpacity: 0.68
    };
  }

  if (gi <= -3) {
    return {
      fillColor: "#1565c0",
      color: "#114b8d",
      weight: 1,
      fillOpacity: 0.78
    };
  }

  if (gi === -2) {
    return {
      fillColor: "#42a5f5",
      color: "#2d7fbe",
      weight: 1,
      fillOpacity: 0.74
    };
  }

  if (gi === -1) {
    return {
      fillColor: "#bbdefb",
      color: "#7aa9d6",
      weight: 1,
      fillOpacity: 0.68
    };
  }

  return {
    fillColor: "#e5e7eb",
    color: "#b0b7c3",
    weight: 0.8,
    fillOpacity: 0.45
  };
}

function getLISAStyle(feature) {
  const p = feature.properties || {};
  const cluster = String(p.cluster || p.lisa_type || p.quad || "").toLowerCase();

  if (cluster.includes("high-high") || cluster === "hh") {
    return { fillColor: "#d73027", color: "#9f1f1a", weight: 1, fillOpacity: 0.78 };
  }
  if (cluster.includes("low-low") || cluster === "ll") {
    return { fillColor: "#4575b4", color: "#2e5382", weight: 1, fillOpacity: 0.78 };
  }
  if (cluster.includes("high-low") || cluster === "hl") {
    return { fillColor: "#fdae61", color: "#c8853f", weight: 1, fillOpacity: 0.78 };
  }
  if (cluster.includes("low-high") || cluster === "lh") {
    return { fillColor: "#66c2a5", color: "#439078", weight: 1, fillOpacity: 0.78 };
  }

  return { fillColor: "#e5e7eb", color: "#b0b7c3", weight: 0.8, fillOpacity: 0.4 };
}

function solarStyle(feature) {
  const density = Number(feature?.properties?.solar_density || 0);
  return {
    fillColor: getSolarColor(density),
    weight: 0.8,
    opacity: 1,
    color: "#94a3b8",
    fillOpacity: 0.85
  };
}

function evStyle(feature) {
  const density = Number(feature?.properties?.ev_density || feature?.properties?.density || 0);
  return {
    fillColor: getEVColor(density),
    weight: 0.8,
    opacity: 1,
    color: "#8ca2c0",
    fillOpacity: 0.82
  };
}

function boundaryStyle() {
  return {
    color: "#123d87",
    weight: 3,
    opacity: 1,
    fill: false,
    dashArray: "4,4"
  };
}

function highlightFeature(event) {
  const layer = event.target;
  layer.setStyle({
    weight: 2,
    color: "#111827",
    fillOpacity: 0.95
  });
  if (layer.bringToFront) layer.bringToFront();
}

function resetHighlight(event) {
  const layer = event.target;

  if (solarLayer && solarLayer.hasLayer(layer)) solarLayer.resetStyle(layer);
  else if (evLayer && evLayer.hasLayer(layer)) evLayer.resetStyle(layer);
  else if (hotspotLayer && hotspotLayer.hasLayer(layer)) hotspotLayer.resetStyle(layer);
  else if (lisaLayer && lisaLayer.hasLayer(layer)) lisaLayer.resetStyle(layer);
}

function zoomToFeature(event) {
  map.fitBounds(event.target.getBounds(), { padding: [20, 20] });
}

function bindPolygonPopup(layer, title, rows) {
  const content = `
    <div style="font-size:14px; line-height:1.6; min-width:190px; color:#1f2937;">
      <div style="font-size:15px; font-weight:800; color:#102b5c; margin-bottom:6px;">${title}</div>
      ${rows.join("<br>")}
    </div>
  `;
  layer.bindPopup(content);
}

function onEachSolarFeature(feature, layer) {
  const p = feature.properties || {};
  bindPolygonPopup(layer, "Solar Density Cell", [
    `<strong>Grid ID:</strong> ${p.grid_id ?? "N/A"}`,
    `<strong>Solar Count:</strong> ${p.solar_count ?? 0}`,
    `<strong>Density:</strong> ${Number(p.solar_density ?? 0).toFixed(2)} / km²`
  ]);

  layer.on({
    mouseover: highlightFeature,
    mouseout: resetHighlight,
    click: zoomToFeature
  });
}

function onEachEVFeature(feature, layer) {
  const p = feature.properties || {};
  bindPolygonPopup(layer, "EV Density Cell", [
    `<strong>Grid ID:</strong> ${p.grid_id ?? "N/A"}`,
    `<strong>EV Count:</strong> ${p.ev_count ?? p.count ?? 0}`,
    `<strong>Density:</strong> ${Number(p.ev_density ?? p.density ?? 0).toFixed(2)} / km²`
  ]);

  layer.on({
    mouseover: highlightFeature,
    mouseout: resetHighlight,
    click: zoomToFeature
  });
}

function onEachHotspotFeature(feature, layer) {
  const p = feature.properties || {};
  bindPolygonPopup(layer, "Hotspot Analysis", [
    `<strong>Grid ID:</strong> ${p.grid_id ?? "N/A"}`,
    `<strong>Gi* Bin:</strong> ${p.gi_bin ?? p.hotspot_bin ?? "N/A"}`,
    `<strong>Z-Score:</strong> ${p.z_score ?? "N/A"}`,
    `<strong>P-Value:</strong> ${p.p_value ?? "N/A"}`
  ]);

  layer.on({
    mouseover: highlightFeature,
    mouseout: resetHighlight,
    click: zoomToFeature
  });
}

function onEachLISAFeature(feature, layer) {
  const p = feature.properties || {};
  bindPolygonPopup(layer, "LISA Cluster", [
    `<strong>Grid ID:</strong> ${p.grid_id ?? "N/A"}`,
    `<strong>Cluster:</strong> ${p.cluster ?? p.lisa_type ?? p.quad ?? "N/A"}`,
    `<strong>Moran's I:</strong> ${p.moran_i ?? "N/A"}`,
    `<strong>P-Value:</strong> ${p.p_value ?? "N/A"}`
  ]);

  layer.on({
    mouseover: highlightFeature,
    mouseout: resetHighlight,
    click: zoomToFeature
  });
}

function onEachBoundaryFeature(feature, layer) {
  layer.bindPopup(`
    <div style="font-size:14px; line-height:1.6; min-width:180px; color:#1f2937;">
      <div style="font-size:15px; font-weight:800; color:#102b5c; margin-bottom:6px;">NOVEC Service Area</div>
      Boundary reference layer
    </div>
  `);
}

function removeAnalysisLayers() {
  [solarLayer, evLayer, hotspotLayer, lisaLayer].forEach(layer => {
    if (layer && map.hasLayer(layer)) map.removeLayer(layer);
  });

  solarLayer = null;
  evLayer = null;
  hotspotLayer = null;
  lisaLayer = null;
}

function bringBoundaryToFront() {
  if (boundaryVisible && boundaryLayer && map.hasLayer(boundaryLayer)) {
    boundaryLayer.bringToFront();
  }
}

function showBaseMap() {
  removeAnalysisLayers();
  currentLayerType = "base";
  setLayerLabel("Base Map View");
  syncToggles("base");
  bringBoundaryToFront();
  updateLegend("base");
}

function loadGeoJSONLayer(path, options, onSuccess, onErrorMessage) {
  fetch(path)
    .then(response => {
      if (!response.ok) {
        throw new Error(`Failed to load ${path}`);
      }
      return response.json();
    })
    .then(data => {
      const layer = L.geoJSON(data, options).addTo(map);
      try {
        map.fitBounds(layer.getBounds(), { padding: [20, 20] });
      } catch (e) {}
      onSuccess(layer);
      bringBoundaryToFront();
    })
    .catch(error => {
      console.error(error);
      alert(onErrorMessage);
    });
}

function showSolarDensity() {
  removeAnalysisLayers();
  currentLayerType = "solar";

  loadGeoJSONLayer(
    layerFiles.solar,
    { style: solarStyle, onEachFeature: onEachSolarFeature },
    (layer) => {
      solarLayer = layer;
      setLayerLabel("Solar Density");
      syncToggles("solar");
      updateLegend("solar");
    },
    "Solar density layer could not be loaded. Verify ./static/data/solar_grid.geojson exists."
  );
}

function showEVDensity() {
  removeAnalysisLayers();
  currentLayerType = "ev";

  loadGeoJSONLayer(
    layerFiles.ev,
    { style: evStyle, onEachFeature: onEachEVFeature },
    (layer) => {
      evLayer = layer;
      setLayerLabel("EV Density");
      syncToggles("ev");
      updateLegend("ev");
    },
    "EV density layer could not be loaded. Verify ./static/data/ev_grid.geojson exists."
  );
}

function showHotspots() {
  removeAnalysisLayers();
  currentLayerType = "hotspots";

  loadGeoJSONLayer(
    layerFiles.hotspots,
    { style: getHotspotStyle, onEachFeature: onEachHotspotFeature },
    (layer) => {
      hotspotLayer = layer;
      setLayerLabel("Statistically Significant Hotspots");
      syncToggles("hotspots");
      updateLegend("hotspots");
    },
    "Hotspot layer could not be loaded. Verify ./static/data/hotspots.geojson exists."
  );
}

function showLISA() {
  removeAnalysisLayers();
  currentLayerType = "lisa";

  loadGeoJSONLayer(
    layerFiles.lisa,
    { style: getLISAStyle, onEachFeature: onEachLISAFeature },
    (layer) => {
      lisaLayer = layer;
      setLayerLabel("Spatial Clusters (LISA)");
      syncToggles("lisa");
      updateLegend("lisa");
    },
    "LISA layer could not be loaded. Verify ./static/data/lisa_clusters.geojson exists."
  );
}

function loadBoundary() {
  fetch(layerFiles.boundary)
    .then(response => {
      if (!response.ok) {
        throw new Error("Could not load boundary GeoJSON");
      }
      return response.json();
    })
    .then(data => {
      boundaryLayer = L.geoJSON(data, {
        style: boundaryStyle,
        onEachFeature: onEachBoundaryFeature
      }).addTo(map);

      bringBoundaryToFront();
    })
    .catch(error => {
      console.error(error);
      alert("Boundary overlay could not be loaded. Verify ./static/data/novec_service_area.geojson exists.");
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

  const boundaryToggle = document.getElementById("toggleBoundaryLayer");
  if (boundaryToggle) boundaryToggle.checked = boundaryVisible;
}

function showChart(path, title) {
  const panel = document.getElementById("chartPanel");
  const image = document.getElementById("chartImage");
  const text = document.getElementById("chartTitle");

  if (!panel || !image || !text) return;

  panel.style.display = "block";
  image.src = path;
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

legend.addTo(map);

function updateLegend(type) {
  const div = document.getElementById("mapLegend");
  if (!div) return;

  if (type === "solar") {
    div.innerHTML = `
      <div class="legend-title">Solar Density</div>
      <div class="legend-scale" style="background:linear-gradient(to right,#fff8e1 0%,#ffe7a3 16%,#ffc66d 32%,#ff9c51 48%,#f06a3d 64%,#d9432f 78%,#b51f1f 90%,#7f0000 100%);"></div>
      <div class="legend-labels"><span>Low</span><span>High</span></div>
    `;
    return;
  }

  if (type === "ev") {
    div.innerHTML = `
      <div class="legend-title">EV Density</div>
      <div class="legend-scale" style="background:linear-gradient(to right,#f2f7ff 0%,#dbe8ff 16%,#b7d0fb 32%,#8bb4f7 48%,#5d93f0 64%,#2f6fe4 78%,#1553b7 90%,#0b3b8c 100%);"></div>
      <div class="legend-labels"><span>Low</span><span>High</span></div>
    `;
    return;
  }

  if (type === "hotspots") {
    div.innerHTML = `
      <div class="legend-title">Hotspots</div>
      <div class="legend-note">Red = hotspot, blue = cold spot, gray = not significant.</div>
    `;
    return;
  }

  if (type === "lisa") {
    div.innerHTML = `
      <div class="legend-title">LISA Clusters</div>
      <div class="legend-note">Red = High-High, Blue = Low-Low, Orange = High-Low, Green = Low-High.</div>
    `;
    return;
  }

  div.innerHTML = `
    <div class="legend-title">Map View</div>
    <div class="legend-note">Turn on a data layer from the left panel.</div>
  `;
}

function wireUI() {
  const base = document.getElementById("toggleBaseMap");
  const solar = document.getElementById("toggleSolar");
  const boundary = document.getElementById("toggleBoundaryLayer");
  const ev = document.getElementById("toggleEV");
  const hotspots = document.getElementById("toggleHotspots");
  const lisa = document.getElementById("toggleLISA");

  if (base) {
    base.addEventListener("change", (e) => {
      if (e.target.checked) showBaseMap();
      else showSolarDensity();
    });
  }

  if (solar) {
    solar.addEventListener("change", (e) => {
      if (e.target.checked) showSolarDensity();
      else showBaseMap();
    });
  }

  if (boundary) {
    boundary.addEventListener("change", () => {
      toggleBoundary();
    });
  }

  if (ev) {
    ev.addEventListener("change", (e) => {
      if (e.target.checked) showEVDensity();
      else showBaseMap();
    });
  }

  if (hotspots) {
    hotspots.addEventListener("change", (e) => {
      if (e.target.checked) showHotspots();
      else showBaseMap();
    });
  }

  if (lisa) {
    lisa.addEventListener("change", (e) => {
      if (e.target.checked) showLISA();
      else showBaseMap();
    });
  }
}

wireUI();
updateLegend("solar");
showSolarDensity();
loadBoundary();
syncToggles("solar");