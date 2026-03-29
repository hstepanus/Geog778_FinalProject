const map = L.map("map", {
  zoomControl: true
}).setView([38.75, -77.45], 9);

L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  attribution: "&copy; OpenStreetMap contributors"
}).addTo(map);

let solarLayer = null;
let boundaryLayer = null;
let boundaryVisible = true;
let currentLayerType = "solar";

function setLayerLabel(text) {
  const label = document.getElementById("layerLabel");
  if (label) {
    label.innerText = text;
  }
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

function solarStyle(feature) {
  const density = Number(feature?.properties?.solar_density) || 0;

  return {
    fillColor: getSolarColor(density),
    weight: 0.9,
    opacity: 1,
    color: "#6b7280",
    fillOpacity: 0.82
  };
}

function boundaryStyle() {
  return {
    color: "#123d87",
    weight: 3,
    opacity: 1,
    fill: false
  };
}

function highlightFeature(event) {
  const layer = event.target;

  layer.setStyle({
    weight: 2.2,
    color: "#0f172a",
    fillOpacity: 0.95
  });

  if (!L.Browser.ie && !L.Browser.opera && !L.Browser.edge) {
    layer.bringToFront();
  }
}

function resetHighlight(event) {
  if (solarLayer) {
    solarLayer.resetStyle(event.target);
  }
}

function zoomToFeature(event) {
  map.fitBounds(event.target.getBounds(), { padding: [20, 20] });
}

function onEachSolarFeature(feature, layer) {
  const props = feature.properties || {};
  const gridId = props.grid_id ?? "N/A";
  const solarCount = props.solar_count ?? 0;
  const solarDensity = Number(props.solar_density ?? 0).toFixed(2);

  layer.bindPopup(`
    <div style="font-size:14px; line-height:1.6; min-width:190px; color:#1f2937;">
      <div style="font-size:15px; font-weight:800; color:#102b5c; margin-bottom:6px;">Solar Density Cell</div>
      <strong>Grid ID:</strong> ${gridId}<br>
      <strong>Solar Count:</strong> ${solarCount}<br>
      <strong>Solar Density:</strong> ${solarDensity} per km²
    </div>
  `);

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
  if (solarLayer) {
    map.removeLayer(solarLayer);
    solarLayer = null;
  }
}

function showBaseMap() {
  removeAnalysisLayers();
  currentLayerType = "base";
  setLayerLabel("Base Map View");

  if (boundaryVisible && boundaryLayer) {
    boundaryLayer.bringToFront();
  }
}

function showSolarDensity() {
  removeAnalysisLayers();
  currentLayerType = "solar";

  fetch("./static/data/solar_grid.geojson")
    .then((response) => {
      if (!response.ok) {
        throw new Error("Could not load solar_grid.geojson");
      }
      return response.json();
    })
    .then((data) => {
      solarLayer = L.geoJSON(data, {
        style: solarStyle,
        onEachFeature: onEachSolarFeature
      }).addTo(map);

      if (boundaryVisible && boundaryLayer) {
        boundaryLayer.bringToFront();
      }

      try {
        map.fitBounds(solarLayer.getBounds(), { padding: [20, 20] });
      } catch (error) {
        console.warn("Could not fit bounds for solar layer.", error);
      }

      setLayerLabel("Solar Adoption Density");
    })
    .catch((error) => {
      console.error(error);
      alert("Solar density layer could not be loaded. Verify ./static/data/solar_grid.geojson exists.");
    });
}

function loadBoundary() {
  fetch("./static/data/novec_service_area.geojson")
    .then((response) => {
      if (!response.ok) {
        throw new Error("Could not load novec_service_area.geojson");
      }
      return response.json();
    })
    .then((data) => {
      boundaryLayer = L.geoJSON(data, {
        style: boundaryStyle,
        onEachFeature: onEachBoundaryFeature
      }).addTo(map);

      if (solarLayer) {
        boundaryLayer.bringToFront();
      }
    })
    .catch((error) => {
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
}

function showPlaceholderLayer(layerName) {
  removeAnalysisLayers();
  currentLayerType = "placeholder";
  setLayerLabel(layerName);

  if (boundaryVisible && boundaryLayer) {
    boundaryLayer.bringToFront();
  }

  alert(
    `${layerName}\n\nThis view is reserved for the next analytical stage. The current live map layer is Solar Adoption Density.`
  );
}

function showChart(imagePath, title) {
  const chartPanel = document.getElementById("chartPanel");
  const chartImage = document.getElementById("chartImage");
  const chartTitle = document.getElementById("chartTitle");

  if (!chartPanel || !chartImage || !chartTitle) return;

  chartPanel.style.display = "block";
  chartImage.src = imagePath;
  chartTitle.innerText = title;
}

function hideChart() {
  const chartPanel = document.getElementById("chartPanel");
  if (chartPanel) {
    chartPanel.style.display = "none";
  }
}

const legend = L.control({ position: "bottomright" });

legend.onAdd = function () {
  const div = L.DomUtil.create("div", "legend");
  div.innerHTML = `
    <div class="legend-title">Solar Density</div>
    <div class="legend-scale"></div>
    <div class="legend-labels">
      <span>Low</span>
      <span>High</span>
    </div>
    <div class="legend-note">
      Grid-based solar adoption density calculated from clipped NOVEC-area solar points.
    </div>
  `;
  return div;
};

legend.addTo(map);

showSolarDensity();
loadBoundary();