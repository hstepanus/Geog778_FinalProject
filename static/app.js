const map = L.map('map', {
  zoomControl: true
}).setView([38.75, -77.45], 9);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);

let solarLayer = null;
let boundaryLayer = null;
let boundaryVisible = true;
let currentLayerType = "solar";

function setLayerLabel(text) {
  document.getElementById("layerLabel").innerText = text;
}

function getColor(d) {
  return d > 6.5 ? '#7f0000' :
         d > 5.5 ? '#b30000' :
         d > 4.5 ? '#d7301f' :
         d > 3.5 ? '#ef6548' :
         d > 2.5 ? '#fc8d59' :
         d > 1.5 ? '#fdbb84' :
         d > 0.5 ? '#fdd49e' :
                   '#fef0d9';
}

function solarStyle(feature) {
  return {
    fillColor: getColor(feature.properties.solar_density),
    weight: 0.6,
    opacity: 1,
    color: '#586474',
    fillOpacity: 0.82
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

function highlightFeature(e) {
  const layer = e.target;
  layer.setStyle({
    weight: 2,
    color: '#111827',
    fillOpacity: 0.95
  });

  if (!L.Browser.ie && !L.Browser.opera && !L.Browser.edge) {
    layer.bringToFront();
  }
}

function resetHighlight(e) {
  if (solarLayer) {
    solarLayer.resetStyle(e.target);
  }
}

function zoomToFeature(e) {
  map.fitBounds(e.target.getBounds(), { padding: [20, 20] });
}

function onEachSolarFeature(feature, layer) {
  const p = feature.properties;

  layer.bindPopup(`
    <div style="font-size:14px; line-height:1.55; min-width:180px;">
      <strong>Grid ID:</strong> ${p.grid_id}<br>
      <strong>Solar Count:</strong> ${p.solar_count}<br>
      <strong>Solar Density:</strong> ${Number(p.solar_density).toFixed(2)} per km²
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
    <div style="font-size:14px; line-height:1.5;">
      <strong>NOVEC Service Area</strong><br>
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

  fetch('./static/data/solar_grid.geojson')
    .then(response => {
      if (!response.ok) {
        throw new Error('Could not load solar_grid.geojson');
      }
      return response.json();
    })
    .then(data => {
      solarLayer = L.geoJSON(data, {
        style: solarStyle,
        onEachFeature: onEachSolarFeature
      }).addTo(map);

      if (boundaryVisible && boundaryLayer) {
        boundaryLayer.bringToFront();
      }

      map.fitBounds(solarLayer.getBounds(), { padding: [20, 20] });
      setLayerLabel("Solar Adoption Density");
    })
    .catch(error => {
      console.error(error);
      alert("Solar density layer could not be loaded. Verify ./static/data/solar_grid.geojson exists.");
    });
}

function loadBoundary() {
  fetch('./static/data/novec_service_area.geojson')
    .then(response => {
      if (!response.ok) {
        throw new Error('Could not load novec_service_area.geojson');
      }
      return response.json();
    })
    .then(data => {
      boundaryLayer = L.geoJSON(data, {
        style: boundaryStyle,
        onEachFeature: onEachBoundaryFeature
      }).addTo(map);

      if (solarLayer) {
        boundaryLayer.bringToFront();
      }
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
}

function showPlaceholderLayer(layerName) {
  removeAnalysisLayers();
  currentLayerType = "placeholder";
  setLayerLabel(layerName);

  if (boundaryVisible && boundaryLayer) {
    boundaryLayer.bringToFront();
  }

  alert(
    layerName +
    "\n\nThis view is reserved for the next analytical stage. The current live map layer is Solar Adoption Density."
  );
}

function showChart(imagePath, title) {
  document.getElementById("chartPanel").style.display = "block";
  document.getElementById("chartImage").src = imagePath;
  document.getElementById("chartTitle").innerText = title;
}

function hideChart() {
  document.getElementById("chartPanel").style.display = "none";
}

const legend = L.control({ position: 'bottomright' });

legend.onAdd = function () {
  const div = L.DomUtil.create('div', 'legend');
  div.innerHTML = `
    <div class="legend-title">Solar Density</div>
    <div class="legend-scale"></div>
    <div class="legend-labels">
      <span>Low</span>
      <span>High</span>
    </div>
    <div class="legend-note">
      Color ramp represents grid-based solar adoption density calculated from clipped NOVEC-area solar points.
    </div>
  `;
  return div;
};

legend.addTo(map);

// Load layers on startup
showSolarDensity();
loadBoundary();