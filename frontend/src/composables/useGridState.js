import { reactive } from "vue";

const state = reactive({
  project: null,
  grid: {},
  layers: {
    dem: true,
    soil: false,
    border: false,
  },
  inspector: null,
  gridError: "",
});

function setProject(project) {
  state.project = project;
  if (project?.project_id) {
    try {
      sessionStorage.setItem(`project:${project.project_id}`, JSON.stringify(project));
    } catch (e) {
      // ignore storage errors
    }
  }
}

function loadProjectFromSession(id) {
  if (!id) return null;
  try {
    const raw = sessionStorage.getItem(`project:${id}`);
    return raw ? JSON.parse(raw) : null;
  } catch (e) {
    return null;
  }
}

function setGrid(partial) {
  const nextLayers = { ...(state.grid?.layers || {}) };
  if (partial?.layers && typeof partial.layers === "object") {
    Object.assign(nextLayers, partial.layers);
  }
  if (partial?.dem) {
    nextLayers.dem = partial.dem;
  }
  if (partial?.soil) {
    nextLayers.soil = partial.soil;
  }
  const nextGrid = { ...(state.grid || {}), ...(partial || {}) };
  if (Object.keys(nextLayers).length) {
    nextGrid.layers = nextLayers;
    if (nextLayers.dem) nextGrid.dem = nextLayers.dem;
    if (nextLayers.soil) nextGrid.soil = nextLayers.soil;
  }
  state.grid = nextGrid;
  if (state.project?.project_id) {
    try {
      sessionStorage.setItem(`grid:${state.project.project_id}`, JSON.stringify(state.grid));
    } catch (e) {
      // ignore storage errors
    }
  }
}

function setLayer(layer, data) {
  if (!layer) return;
  const layers = { ...(state.grid?.layers || {}), [layer]: data };
  setGrid({ [layer]: data, layers });
}

function loadGridFromSession(id) {
  if (!id) return null;
  try {
    const raw = sessionStorage.getItem(`grid:${id}`);
    return raw ? JSON.parse(raw) : null;
  } catch (e) {
    return null;
  }
}

function toggleLayer(layer) {
  if (typeof state.layers[layer] === "boolean") {
    state.layers[layer] = !state.layers[layer];
  }
}

function setInspector(info) {
  state.inspector = info;
}

export function useGridState() {
  return {
    state,
    setProject,
    loadProjectFromSession,
    loadGridFromSession,
    setGrid,
    setLayer,
    toggleLayer,
    setInspector,
  };
}
