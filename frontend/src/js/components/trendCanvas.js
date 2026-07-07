function resizeCanvas(canvas) {
  const rect = canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.max(1, Math.floor(rect.width * dpr));
  canvas.height = Math.max(1, Math.floor(rect.height * dpr));
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return { ctx, width: rect.width, height: rect.height };
}

export function drawTrend(canvas, points = [], selectedDate) {
  const { ctx, width, height } = resizeCanvas(canvas);
  ctx.clearRect(0, 0, width, height);
  ctx.strokeStyle = "rgba(255,255,255,.09)";
  for (let i = 1; i < 4; i += 1) {
    const y = (height * i) / 4;
    ctx.beginPath();
    ctx.moveTo(10, y);
    ctx.lineTo(width - 10, y);
    ctx.stroke();
  }

  if (!points.length) return;
  const max = Math.max(...points.map((point) => point.value));
  const min = Math.min(...points.map((point) => point.value));
  ctx.beginPath();
  points.forEach((point, index) => {
    const x = 14 + index * ((width - 28) / Math.max(1, points.length - 1));
    const y = height - 14 - ((point.value - min) / Math.max(0.000001, max - min)) * (height - 28);
    if (index === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.strokeStyle = "#ccff00";
  ctx.lineWidth = 2;
  ctx.stroke();

  const selectedIndex = points.findIndex((point) => point.date === selectedDate);
  if (selectedIndex >= 0) {
    const x = 14 + selectedIndex * ((width - 28) / Math.max(1, points.length - 1));
    ctx.strokeStyle = "#33bdf8";
    ctx.beginPath();
    ctx.moveTo(x, 10);
    ctx.lineTo(x, height - 10);
    ctx.stroke();
  }
}
