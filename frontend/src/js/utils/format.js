export function formatNumber(value, digits = 0) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "--";
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  }).format(Number(value));
}

export function clamp(value, min = 0, max = 1) {
  return Math.max(min, Math.min(max, value));
}

export function metricDomain(metricId) {
  if (metricId === "sea_temperature") return [20, 31];
  if (metricId === "sst_front_strength") return [0, 3.5];
  if (metricId === "sustainability_pressure") return [0, 100];
  return [0, 100];
}
