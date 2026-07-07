export function renderComponentBars(container, components = []) {
  container.innerHTML = components.map((item) => `
    <div class="bar-row">
      <span>${item.label}</span>
      <i><b style="width:${Math.max(0, Math.min(100, item.value))}%"></b></i>
      <strong>${item.text ?? ""}</strong>
    </div>
  `).join("");
}
