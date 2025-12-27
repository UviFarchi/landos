const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";

export async function fetchGrid(projectId, layer) {
  const url = layer
    ? `${API_BASE}/api/platform/projects/${projectId}/grid?layer=${layer}&refresh=true`
    : `${API_BASE}/api/platform/projects/${projectId}/grid`;
  const resp = await fetch(url);
  if (!resp.ok) {
    const err = await resp.text().catch(() => resp.statusText);
    throw new Error(`Grid fetch failed: ${resp.status} ${err}`);
  }
  return resp.json();
}
