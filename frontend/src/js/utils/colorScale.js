import { clamp } from "./format.js";

// 純前端熱力圖顯示設定。
// 注意：API 現在提供 0–100 的 relative_score，不是原始物理量；
// 因此 log-like 是視覺對比曲線，不等同科學繪圖的 LogNorm。
const HEAT_STYLES = {
  chlor_a: {
    scale: "log-like",
    colors: ["#440154", "#3b528b", "#21918c", "#5ec962", "#fde725"],
  },
  sea_temperature: {
    scale: "linear",
    colors: ["#313695", "#74add1", "#ffffbf", "#f46d43", "#a50026"],
  },
  ocean_productivity_score: {
    scale: "linear",
    colors: ["#f7fcb9", "#addd8e", "#41ab5d", "#006837"],
  },
  fishing_hours: {
    scale: "log-like-zero-mask",
    zeroColor: "rgba(148,163,184,0.12)",
    colors: ["#ffffcc", "#fed976", "#fd8d3c", "#e31a1c", "#800026"],
  },
  sustainability_pressure: {
    scale: "log-like-zero-mask",
    zeroColor: "rgba(148,163,184,0.12)",
    colors: ["#ffffb2", "#fecc5c", "#fd8d3c", "#f03b20", "#bd0026"],
  },
};

const DEFAULT_STYLE = {
  scale: "linear",
  colors: ["#dcfce7", "#86efac", "#22c55e", "#14532d"],
};

function styleFor(metricId) {
  return HEAT_STYLES[metricId] ?? DEFAULT_STYLE;
}

function hexToRgb(hex) {
  const value = hex.replace("#", "");
  return [0, 2, 4].map((offset) => Number.parseInt(value.slice(offset, offset + 2), 16));
}

function interpolatePalette(colors, t) {
  const position = clamp(t) * (colors.length - 1);
  const leftIndex = Math.min(colors.length - 2, Math.floor(position));
  const localT = position - leftIndex;
  const left = hexToRgb(colors[leftIndex]);
  const right = hexToRgb(colors[leftIndex + 1]);
  return left.map((channel, index) =>
    Math.round(channel + (right[index] - channel) * localT),
  );
}

function scalePosition(t, scale) {
  if (scale.startsWith("log-like")) {
    // 強化百分位低、中段的色差，近似原始值使用 LogNorm 時的視覺效果。
    return Math.log1p(9 * t) / Math.log(10);
  }
  return t;
}

export function heatColor(value, metricMeta = {}) {
  const style = styleFor(metricMeta.id);
  const [min, max] = metricMeta.domain ?? [0, 100];
  const numericValue = Number(value);

  // 捕魚時數與永續壓力的零值獨立處理，避免混入近似對數色階。
  if (style.scale.endsWith("zero-mask") && numericValue <= 0) {
    return style.zeroColor;
  }

  let t = (numericValue - min) / Math.max(0.000001, max - min);
  t = clamp(metricMeta.higherIsBetter === false ? 1 - t : t);
  const scaled = scalePosition(t, style.scale);
  const rgb = interpolatePalette(style.colors, scaled);
  const alpha = 0.42 + scaled * 0.58;
  return `rgba(${rgb[0]},${rgb[1]},${rgb[2]},${alpha})`;
}

// 供畫面圖例使用，確保圖例與實際熱力圖採用同一組顏色。
export function heatGradient(metricId) {
  const colors = styleFor(metricId).colors;
  return `linear-gradient(90deg, ${colors.join(", ")})`;
}
