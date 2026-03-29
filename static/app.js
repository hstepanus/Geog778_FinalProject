let map = L.map('map').setView([38.75, -77.45], 9);

// Basemap
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);

// Color ramp for solar density
function getColor(d) {
  return d > 6.5 ? '#800026' :
         d > 5.5 ? '#BD0026' :
         d > 4.5 ? '#E31A1C' :
         d > 3.5 ? '#FC4E2A' :
         d > 2.5 ? '#FD8D3C' :
         d > 1.5 ? '#FEB24C' :
         d > 0.5 ? '#FED976' :
                   '#FFEDA0';
}

// Polygon style
function style(feature) {
  return {
    fillColor: getColor(feature.properties.solar_density),
    weight: 0.8,
    opacity: 1,
    color: '#444',
    fillOpacity: 0.75
  };
}

// Highlight on hover
function highlightFeature(e) {
  const layer = e.target;
  layer.setStyle({
    weight: 2,
    color: '#111',
    fillOpacity: 0.9
  });

  if (!L.Browser.ie && !L.Browser.opera && !L.Browser.edge) {
    layer.bringToFront();
  }
}

// Reset hover style
function resetHighlight(e) {
  geojson.resetStyle(e.target);
}

// Zoom to feature
function zoomToFeature(e) {
  map.fitBounds(e.target.getBounds());
}

// Popup + interactions
function onEachFeature(feature, layer) {
  const p = feature.properties;

  layer.bindPopup(`
    <div style="font-size:14px;">
      <strong>Grid ID:</strong> ${p.grid_id}<br>
      <strong>Solar Count:</strong> ${p.solar_count}<br>
      <strong>Solar Density:</strong> ${Number(p.solar_density).toFixed(2)} / km²
    </div>
  `);

  layer.on({
    mouseover: highlightFeature,
    mouseout: resetHighlight,
    click: zoomToFeature
  });
}

let geojson;

// Load GeoJSON
fetch('/static/data/solar_grid.geojson')
  .then(response => response.json())
  .then(data => {
    geojson = L.geoJSON(data, {
      style: style,
      onEachFeature: onEachFeature
    }).addTo(map);

    map.fitBounds(geojson.getBounds());
  })
  .catch(error => {
    console.error('Error loading GeoJSON:', error);
  });

// Legend
const legend = L.control({ position: 'bottomright' });

legend.onAdd = function () {
  const div = L.DomUtil.create('div', 'info legend');
  const grades = [0, 0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5];

  div.innerHTML += '<h4>Solar Density</h4>';

  for (let i = 0; i < grades.length; i++) {
    const from = grades[i];
    const to = grades[i + 1];

    div.innerHTML +=
      '<i style="background:' + getColor(from + 0.01) + '"></i> ' +
      from + (to ? '&ndash;' + to : '+') + '<br>';
  }

  return div;
};

legend.addTo(map);