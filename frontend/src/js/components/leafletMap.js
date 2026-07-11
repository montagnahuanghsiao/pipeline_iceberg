import { heatColor } from "../utils/colorScale.js";

const LAND_URL = new URL(
  "../../../assets/map/northwest_pacific_land.geojson",
  import.meta.url,
);

const LABELS = [
  { label: "台灣", lat: 23.7, lon: 120.9 },
  { label: "中國", lat: 29.2, lon: 114.5 },
  { label: "日本", lat: 33.6, lon: 132.2 },
  { label: "菲律賓", lat: 17.2, lon: 121.5 },
];

function cellBounds(cell) {
  const step = Math.max(1, Number(cell.resolution_km ?? 4) / 4);
  const north = 90 - Number(cell.grid_row) / 24;
  const south = 90 - (Number(cell.grid_row) + step) / 24;
  const west = Number(cell.grid_col) / 24 - 180;
  const east = (Number(cell.grid_col) + step) / 24 - 180;
  return { north, south, west, east };
}

function createGridCanvasLayer(L) {
  return L.Layer.extend({
    initialize(options = {}) {
      L.setOptions(this, options);
      this._grid = [];
      this._metricMeta = { domain: [0, 100], higherIsBetter: true };
    },

    onAdd(map) {
      this._map = map;
      this._canvas = L.DomUtil.create("canvas", "ocean-grid-canvas");
      this._canvas.setAttribute("aria-hidden", "true");
      map.getPane(this.options.pane).appendChild(this._canvas);
      map.on("moveend zoomend resize", this._reset, this);
      this._reset();
    },

    onRemove(map) {
      map.off("moveend zoomend resize", this._reset, this);
      this._canvas.remove();
      this._canvas = null;
      this._map = null;
    },

    setData(grid = [], metricMeta) {
      this._grid = grid;
      this._metricMeta = metricMeta;
      this._reset();
    },

    _reset() {
      if (!this._map || !this._canvas) return;
      const L = window.L;
      const size = this._map.getSize();
      const dpr = window.devicePixelRatio || 1;
      const topLeft = this._map.containerPointToLayerPoint([0, 0]);
      L.DomUtil.setPosition(this._canvas, topLeft);
      this._canvas.width = Math.max(1, Math.round(size.x * dpr));
      this._canvas.height = Math.max(1, Math.round(size.y * dpr));
      this._canvas.style.width = `${size.x}px`;
      this._canvas.style.height = `${size.y}px`;

      const ctx = this._canvas.getContext("2d");
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, size.x, size.y);
      const visibleBounds = this._map.getBounds();

      this._grid.forEach((cell) => {
        if (!Number.isFinite(Number(cell.value))) return;
        const bounds = cellBounds(cell);
        const cellLatLngBounds = L.latLngBounds(
          [bounds.south, bounds.west],
          [bounds.north, bounds.east],
        );
        if (!visibleBounds.intersects(cellLatLngBounds)) return;

        const northwest = this._map.latLngToContainerPoint([
          bounds.north,
          bounds.west,
        ]);
        const southeast = this._map.latLngToContainerPoint([
          bounds.south,
          bounds.east,
        ]);
        const width = Math.max(1, southeast.x - northwest.x);
        const height = Math.max(1, southeast.y - northwest.y);
        const coverage = Math.max(0, Math.min(1, Number(cell.data_coverage ?? 1)));

        ctx.globalAlpha = 0.34 + coverage * 0.52;
        // 配色、線性／近似對數曲線與零值遮罩由 metric metadata 決定。
        ctx.fillStyle = heatColor(Number(cell.value), this._metricMeta);
        ctx.fillRect(northwest.x, northwest.y, width + 0.35, height + 0.35);
      });
      ctx.globalAlpha = 1;
    },
  });
}

function addGraticules(map, L) {
  // 經緯網：每 5 度一條，放在熱力圖下方作為空間位置參考。
  // opacity 控制清晰度；dashArray 控制虛線的線段與間隔長度。
  for (let lat = 15; lat <= 35; lat += 5) {
    L.polyline([[lat, 103], [lat, 137]], {
      pane: "referencePane",
      color: "#b8d8d2",
      opacity: 0.28,
      weight: 1,
      dashArray: "3 6",
      interactive: false,
    }).addTo(map);
  }
  for (let lon = 105; lon <= 135; lon += 5) {
    L.polyline([[13, lon], [37, lon]], {
      pane: "referencePane",
      color: "#b8d8d2",
      opacity: 0.28,
      weight: 1,
      dashArray: "3 6",
      interactive: false,
    }).addTo(map);
  }
}

function addLabels(map, L) {
  LABELS.forEach(({ label, lat, lon }) => {
    L.marker([lat, lon], {
      pane: "labelPane",
      interactive: false,
      icon: L.divIcon({
        className: "map-country-label",
        html: label,
        iconSize: null,
      }),
    }).addTo(map);
  });
}

export function createOceanMap(container) {
  const L = window.L;
  if (!L) throw new Error("Leaflet library is not available");

  const map = L.map(container, {
    preferCanvas: true,
    zoomControl: true,
    attributionControl: false,
    minZoom: 4,
    maxZoom: 9,
    zoomSnap: 0.25,
    maxBounds: [[10, 100], [40, 140]],
    maxBoundsViscosity: 0.75,
  });

  [
    // 經緯線要高於熱力圖才看得到，但仍低於陸地與 AOI 邊界。
    ["referencePane", 260],
    ["heatPane", 240],
    ["landPane", 280],
    ["aoiPane", 330],
    ["labelPane", 360],
  ].forEach(([name, zIndex]) => {
    map.createPane(name);
    map.getPane(name).style.zIndex = zIndex;
  });

  addGraticules(map, L);
  addLabels(map, L);

  const GridCanvasLayer = createGridCanvasLayer(L);
  const gridLayer = new GridCanvasLayer({ pane: "heatPane" }).addTo(map);
  const landRenderer = L.svg({ pane: "landPane", padding: 0.5 }).addTo(map);
  let aoiLayer = null;
  let currentAoi = null;

  fetch(LAND_URL)
    .then((response) => {
      if (!response.ok) throw new Error(`GeoJSON ${response.status}`);
      return response.json();
    })
    .then((geojson) => {
      L.geoJSON(geojson, {
        pane: "landPane",
        renderer: landRenderer,
        style: {
          color: "#a0aaa5",
          weight: 1,
          opacity: 0.88,
          fillColor: "#202725",
          fillOpacity: 1,
        },
        onEachFeature(feature, layer) {
          if (feature.properties?.name) {
            layer.bindTooltip(feature.properties.name, {
              direction: "center",
              className: "land-tooltip",
              sticky: true,
            });
          }
        },
      }).addTo(map);
    })
    .catch((error) => {
      console.error("Unable to load local Natural Earth GeoJSON", error);
    });

  L.control.attribution({ prefix: false, position: "bottomright" })
    .addAttribution("Map: Natural Earth · Leaflet")
    .addTo(map);

  function focusAoi(mapConfig) {
    if (currentAoi === mapConfig.id) return;
    currentAoi = mapConfig.id;
    if (aoiLayer) aoiLayer.remove();
    const bounds = L.latLngBounds(
      [mapConfig.minLat, mapConfig.minLon],
      [mapConfig.maxLat, mapConfig.maxLon],
    );
    aoiLayer = L.rectangle(bounds, {
      pane: "aoiPane",
      color: "#ccff00",
      weight: 1.4,
      opacity: 0.78,
      dashArray: "5 7",
      fill: false,
      interactive: false,
    }).addTo(map);
    map.fitBounds(bounds, {
      padding: [26, 26],
      animate: false,
    });
  }

  return {
    render({ grid, metricMeta, mapConfig }) {
      focusAoi(mapConfig);
      gridLayer.setData(grid, metricMeta);
    },
    invalidateSize() {
      map.invalidateSize({ animate: false });
    },
  };
}
