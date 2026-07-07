# OceanGrid Frontend

The dashboard supports four query dimensions:

- date
- AOI
- product
- metric

The map uses Leaflet with a bundled, offline Natural Earth 1:50m GeoJSON
basemap. It does not request OpenStreetMap or any other external tile service.
The bundled regional file is generated from Natural Earth's public-domain
`ne_50m_admin_0_countries` dataset:

```bash
node frontend/tools/build-map-assets.mjs \
  /path/to/ne_50m_admin_0_countries.geojson \
  frontend/assets/map/northwest_pacific_land.geojson
```

Start the static frontend:

```bash
python -m http.server 8766 --bind 127.0.0.1
```

Open `http://127.0.0.1:8766/frontend/`.

The default data source is deterministic mock data so the interface can be
reviewed without a cluster. To use Flask locally, edit
`frontend/runtime-config.js`:

```js
window.OCEAN_CONFIG = {
  dataSource: "api",
  apiBaseUrl: "http://localhost:8000/api/v1",
};
```

API contract:

```text
GET /api/v1/gold/daily-grid?date=2024-12-12&aoi=taiwan&product=CHL&metric=chlor_a&resolution=4
GET /api/v1/gold/summary?date=2024-12-12&aoi=taiwan&product=CHL&metric=chlor_a&resolution=4
GET /api/v1/gold/trend?aoi=taiwan&product=CHL&metric=chlor_a&resolution=4
```

Map cells require `grid_id`, `grid_row`, `grid_col`, `relative_score`,
`display_level` and `data_coverage`. The compatibility field `value` must
equal `relative_score`. All display metrics use the 0–100 range; the UI
renders qualitative levels and does not show raw scientific values or units.
The browser never reads Bronze or Silver data directly.

Map attribution: Natural Earth and Leaflet.

Kubernetes uses `pipeline_iceberg/deploy/kubernetes/08-frontend.yaml`. Its
Nginx container replaces the runtime configuration with `dataSource: "api"`
and proxies `/api/` to the Flask Service.
