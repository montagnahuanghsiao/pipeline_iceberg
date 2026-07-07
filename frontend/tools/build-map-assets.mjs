import fs from "node:fs";
import path from "node:path";

const [, , inputPath, outputPath] = process.argv;
if (!inputPath || !outputPath) {
  console.error("usage: node build-map-assets.mjs <source.geojson> <output.geojson>");
  process.exit(2);
}

const region = { minLon: 103, minLat: 13, maxLon: 137, maxLat: 37 };
const source = JSON.parse(fs.readFileSync(inputPath, "utf8"));

function intersectsRegion(feature) {
  const [minLon, minLat, maxLon, maxLat] = feature.bbox ?? [];
  if ([minLon, minLat, maxLon, maxLat].some((value) => value == null)) return false;
  return !(
    maxLon < region.minLon
    || minLon > region.maxLon
    || maxLat < region.minLat
    || minLat > region.maxLat
  );
}

const output = {
  type: "FeatureCollection",
  name: "natural_earth_50m_northwest_pacific",
  source: "Natural Earth ne_50m_admin_0_countries",
  license: "Public domain",
  bbox: [region.minLon, region.minLat, region.maxLon, region.maxLat],
  features: source.features
    .filter(intersectsRegion)
    .map((feature) => ({
      type: "Feature",
      properties: {
        name: feature.properties.NAME_ZHT
          || feature.properties.NAME_EN
          || feature.properties.NAME,
        name_en: feature.properties.NAME_EN || feature.properties.NAME,
        iso_a3: feature.properties.ADM0_A3,
      },
      geometry: feature.geometry,
    })),
};

fs.mkdirSync(path.dirname(outputPath), { recursive: true });
fs.writeFileSync(outputPath, `${JSON.stringify(output)}\n`, "utf8");
console.log(`wrote ${output.features.length} features to ${outputPath}`);
