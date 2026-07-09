const STATUS_LABELS = {
  high_productivity_low_fishing: "高生產力低捕魚",
  high_productivity_high_fishing: "高生產力高捕魚",
  low_productivity_high_fishing: "低生產力高捕魚",
  low_productivity_low_fishing: "低生產力低捕魚",
};

const STATUS_COLORS = {
  high_productivity_low_fishing: "#2dd4bf",
  high_productivity_high_fishing: "#ccff00",
  low_productivity_high_fishing: "#ff7a45",
  low_productivity_low_fishing: "#64748b",
};

function resizeCanvas(canvas) {
  const rect = canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.max(1, Math.floor(rect.width * dpr));
  canvas.height = Math.max(1, Math.floor(rect.height * dpr));
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return { ctx, width: rect.width, height: rect.height };
}

export function drawStatusPie(canvas, rows = []) {
  const { ctx, width, height } = resizeCanvas(canvas);
  ctx.clearRect(0, 0, width, height);
  if (!rows.length) return;
  const radius = Math.max(32, Math.min(width, height) * 0.3);
  const cx = width * 0.32;
  const cy = height * 0.52;
  let start = -Math.PI / 2;
  rows.forEach((row) => {
    const ratio = Number(row.cell_ratio ?? 0);
    const end = start + ratio * Math.PI * 2;
    ctx.beginPath();
    ctx.moveTo(cx, cy);
    ctx.arc(cx, cy, radius, start, end);
    ctx.closePath();
    ctx.fillStyle = STATUS_COLORS[row.status_class] ?? "#94a3b8";
    ctx.fill();
    start = end;
  });

  ctx.font = "12px ui-monospace, SFMono-Regular, Consolas, monospace";
  ctx.textBaseline = "middle";
  rows.forEach((row, index) => {
    const y = 22 + index * 26;
    const x = width * 0.58;
    ctx.fillStyle = STATUS_COLORS[row.status_class] ?? "#94a3b8";
    ctx.fillRect(x, y - 6, 12, 12);
    ctx.fillStyle = "rgba(245,245,242,.82)";
    const ratio = `${(Number(row.cell_ratio ?? 0) * 100).toFixed(1)}%`;
    ctx.fillText(`${STATUS_LABELS[row.status_class] ?? row.status_class} ${ratio}`, x + 20, y);
  });
}
