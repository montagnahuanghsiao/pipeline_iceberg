import { AOIS, METRICS } from "../config.js";

function noise(row, col, day, salt = 0) {
  const value = Math.sin(row * 12.9898 + col * 78.233 + day * 37.719 + salt) * 43758.5453;
  return value - Math.floor(value);
}
function makeRawValue(metric, nx, ny, row, col, day) {
  const n = noise(row, col, day, metric.length);
  const current = Math.exp(-((nx - 0.72) ** 2) / 0.025) * Math.exp(-((ny - 0.52) ** 2) / 0.16);
  if (metric === "fishing_hours") return n < 0.08 ? 0 : n * 35 + current * 65;
  if (metric === "sea_temperature") return Math.min(100, ny * 65 + n * 35);
  if (metric === "chlor_a") return Math.min(100, current * 75 + n * 25);
  if (metric === "poc") return Math.min(100, current * 68 + n * 32);
  if (metric === "nflh") return Math.min(100, current * 72 + n * 28);
  return Math.min(100, 20 + current * 60 + n * 20);
}
function buildGrid({ date, aoi, product, metric, resolution = 4 }) {
  const area = AOIS.find((item) => item.id === aoi) ?? AOIS[0];
  const day = Number(date.slice(-2));
  const rowMin = Math.floor((90 - area.maxLat) * area.gridScale);
  const rowMax = Math.floor((90 - area.minLat) * area.gridScale) - 1;
  const colMin = Math.floor((area.minLon + 180) * area.gridScale);
  const colMax = Math.floor((area.maxLon + 180) * area.gridScale) - 1;
  const step = Math.max(1, resolution / 4);
  const grid = [];
  for (let row = rowMin; row <= rowMax; row += step) {
    for (let col = colMin; col <= colMax; col += step) {
      const nx = (col - colMin) / Math.max(1, colMax - colMin);
      const ny = (row - rowMin) / Math.max(1, rowMax - rowMin);
      const rawValue = makeRawValue(metric, nx, ny, row, col, day);
      grid.push({
        date,
        product,
        metric,
        grid_id: `nasa4km_r${String(row).padStart(4, "0")}_c${String(col).padStart(4, "0")}`,
        grid_row: row,
        grid_col: col,
        rawValue,
        data_coverage: Number((0.68 + noise(col, row, day) * 0.3).toFixed(3)),
      });
    }
  }
  const rankable = grid
    .filter((cell) => metric !== "fishing_hours" || cell.rawValue > 0)
    .sort((left, right) => left.rawValue - right.rawValue);
  const scores = new Map(
    rankable.map((cell, index) => [
      cell.grid_id,
      rankable.length === 1 ? 100 : (index / (rankable.length - 1)) * 100,
    ]),
  );
  return grid.map(({ rawValue, ...cell }) => {
    const score = metric === "fishing_hours" && rawValue <= 0 ? 0 : scores.get(cell.grid_id);
    const relativeScore = Number(score.toFixed(3));
    return {
      ...cell,
      resolution_km: resolution,
      relative_score: relativeScore,
      display_level: relativeScore >= 80 ? "very_high"
        : relativeScore >= 60 ? "high"
          : relativeScore >= 40 ? "medium"
            : relativeScore >= 20 ? "low"
              : "very_low",
      value_source: "observed",
      value: relativeScore,
    };
  });
}
export const mockOceanApi = {
  async getAvailability(filters) {
    return {
      ...filters,
      dates: Array.from({ length: 31 }, (_, index) => `2024-12-${String(index + 1).padStart(2, "0")}`),
      partitions: [],
    };
  },
  async getDailyGrid(filters) { return { ...filters, source: "mock", grid: buildGrid(filters) }; },
  async getSummary(filters) {
    const grid = buildGrid(filters); const values = grid.map((cell) => cell.value);
    return { ...filters, partition: `event_date=${filters.date}/aoi_id=${filters.aoi}`, nasa_coverage: grid.reduce((sum, cell) => sum + cell.data_coverage, 0) / grid.length, cells: grid.length, average: values.reduce((a, b) => a + b, 0) / values.length, maximum: Math.max(...values), components: [] };
  },
  async getTrend(filters) {
    return { ...filters, points: Array.from({ length: 31 }, (_, index) => ({ date: `2024-12-${String(index + 1).padStart(2, "0")}`, value: 48 + Math.sin(index / 3) * 17 + noise(index, 1, 4) * 12 })) };
  },
};
