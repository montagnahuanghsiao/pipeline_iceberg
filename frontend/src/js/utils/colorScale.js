import { clamp } from "./format.js";

export function heatColor(value, domain, higherIsBetter = true) {
  const [min, max] = domain;
  let t = (value - min) / Math.max(0.000001, max - min);
  t = clamp(higherIsBetter ? t : 1 - t);
  if (t < 0.25) return `rgba(82,82,78,${0.18 + t * 1.4})`;
  if (t < 0.55) return `rgba(45,212,191,${0.22 + t * 0.85})`;
  if (t < 0.78) return `rgba(51,189,248,${0.24 + t * 0.78})`;
  return `rgba(204,255,0,${0.28 + t * 0.72})`;
}
