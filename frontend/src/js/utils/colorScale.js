import { clamp } from "./format.js";

export function heatColor(value, domain, higherIsBetter = true) {
  const [min, max] = domain;
  let t = (value - min) / Math.max(0.000001, max - min);
  t = clamp(higherIsBetter ? t : 1 - t);
  const boosted = Math.pow(t, 0.82);
  if (boosted < 0.22) {
    const alpha = 0.10 + boosted * 0.78;
    return `rgba(210,214,210,${alpha})`;
  }
  if (boosted < 0.45) {
    const alpha = 0.18 + boosted * 0.72;
    return `rgba(126,138,132,${alpha})`;
  }
  if (boosted < 0.66) {
    const alpha = 0.24 + boosted * 0.74;
    return `rgba(45,212,191,${alpha})`;
  }
  if (boosted < 0.82) {
    const alpha = 0.30 + boosted * 0.70;
    return `rgba(51,189,248,${alpha})`;
  }
  const alpha = 0.38 + boosted * 0.62;
  return `rgba(204,255,0,${alpha})`;
}
