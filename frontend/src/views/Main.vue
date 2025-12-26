<template>
  <main class="main-view">
    <p class="muted" v-if="loading">Loading…</p>
    <p class="muted" v-else-if="!project">No project loaded.</p>
    <div v-else class="details">
      <div class="layout">
        <section class="controls">
          <div class="panel-title">Camera</div>
          <div class="control-grid">
            <label>Lat</label>
            <input type="number" v-model.number="mapState.latitude" step="0.0001" />
            <label>Lon</label>
            <input type="number" v-model.number="mapState.longitude" step="0.0001" />
            <label>Pitch</label>
            <input type="number" v-model.number="mapState.pitch" step="1" min="0" max="89" />
            <label>Bearing</label>
            <input type="number" v-model.number="mapState.bearing" step="1" min="-180" max="180" />
            <label>Zoom</label>
            <input type="number" v-model.number="mapState.zoom" step="0.1" min="2" max="22" />
          </div>
          <div class="panel-title">Layers</div>
          <button
            class="ghost"
            :class="{ active: layers.border }"
            data-test="border-button"
            @click="handleBorder"
          >
            Border
          </button>
          <button
            class="ghost"
            :class="{ active: layers.soil, loading: soilLoading }"
            data-test="soil-button"
            @click="handleSoil"
          >
            Soil
          </button>
          <div v-if="soilLoading" class="muted small">Fetching soil data…</div>
          <div v-if="gridError" class="alert">{{ gridError }}</div>
        </section>

        <section class="map-wrap">
          <map-pane
            :polygon="project.geometry"
            :dem="plainDem"
            :soil="grid.soil"
            :show-dem="true"
            :show-soil="layers.soil"
            :show-border="layers.border"
            v-bind="mapState"
            @pick="handlePick"
          />
        </section>

        <section class="info-column">
          <div class="info-card">
            <div class="panel-title">Project</div>
            <div class="row"><span class="label">Name</span><span class="value text-left">{{ project.name }}</span></div>
            <div class="row"><span class="label">Owner</span><span class="value text-left">{{ project.username }}</span></div>
            <div class="row"><span class="label">Area</span><span class="value text-left">{{ project.area_hectares ? project.area_hectares.toFixed(3) : '—' }} ha</span></div>
            <div class="row"><span class="label">Min Elev</span><span class="value text-left">{{ demStats.min ?? '—' }} m</span></div>
            <div class="row"><span class="label">Max Elev</span><span class="value text-left">{{ demStats.max ?? '—' }} m</span></div>
            <div class="row">
              <span class="label">Location</span>
              <span class="value text-left">{{ project.country || '—' }} · {{ project.subdivision_name || project.subdivision || '—' }}</span>
            </div>
          </div>

          <div class="inspector-card">
            <div class="panel-title">Inspector</div>
            <div v-if="inspector">
              <div class="value small">Lat {{ inspector.lat }}, Lon {{ inspector.lon }}</div>
              <div class="value small" v-if="inspector.row != null && inspector.col != null">Row {{ inspector.row }}, Col {{ inspector.col }}</div>
              <table class="inspector-table">
                <thead><tr><th colspan="2">Topography</th></tr></thead>
                <tbody>
                  <tr><td>Elevation</td><td>{{ inspector.dem ?? '—' }}</td></tr>
                  <tr><td>Slope</td><td>{{ inspector.slope ? inspector.slope.toFixed(2) : '—' }}°</td></tr>
                  <tr><td>Aspect</td><td>{{ inspector.aspect ? inspector.aspect.toFixed(1) : '—' }}°</td></tr>
                </tbody>
              </table>
              <table class="inspector-table" v-if="layers.soil">
                <thead><tr><th colspan="2">Soil</th></tr></thead>
                <tbody>
                  <tr><td>Soil</td><td>{{ inspector.soil ? (inspector.soil.compname || inspector.soil.muname || inspector.soil.mukey || '—') : '—' }}</td></tr>
                  <tr v-if="soilAttr(inspector.soil, 'muname')"><td>Unit</td><td>{{ soilAttr(inspector.soil, 'muname') }}</td></tr>
                  <tr v-if="soilAttr(inspector.soil, 'compname')"><td>Component</td><td>{{ soilAttr(inspector.soil, 'compname') }}</td></tr>
                  <tr v-if="soilAttr(inspector.soil, 'drainagecl')"><td>Drainage</td><td>{{ soilAttr(inspector.soil, 'drainagecl') }}</td></tr>
                  <tr v-if="soilAttr(inspector.soil, 'ph', 'ph1to1h2o_r')"><td>pH</td><td>{{ soilAttr(inspector.soil, 'ph', 'ph1to1h2o_r') }}</td></tr>
                  <tr v-if="soilAttr(inspector.soil, 'organic_matter', 'om_r')"><td>Organic matter</td><td>{{ soilAttr(inspector.soil, 'organic_matter', 'om_r') }}</td></tr>
                  <tr v-if="soilAttr(inspector.soil, 'water_capacity', 'awc_r')"><td>Water capacity</td><td>{{ soilAttr(inspector.soil, 'water_capacity', 'awc_r') }}</td></tr>
                  <tr v-if="soilAttr(inspector.soil, 'sand', 'sandtotal_r')"><td>Sand</td><td>{{ soilAttr(inspector.soil, 'sand', 'sandtotal_r') }}</td></tr>
                  <tr v-if="soilAttr(inspector.soil, 'clay', 'claytotal_r')"><td>Clay</td><td>{{ soilAttr(inspector.soil, 'clay', 'claytotal_r') }}</td></tr>
                </tbody>
              </table>
            </div>
            <div v-else class="muted small">Click on the map to inspect a point.</div>
          </div>
        </section>
      </div>
    </div>
  </main>
</template>

<script>
import { onMounted, computed, ref } from 'vue';
import MapPane from '../components/MapPane.vue';
import { useGridState } from '../composables/useGridState';
import { fetchGrid } from '../utils/gridApi';
import { sampleDem, sampleSoil, sampleTopography } from '../utils/gridSampler';

export default {
  name: 'MainView',
  components: { MapPane },
  props: ['id'],
  setup(props) {
    const { state, setProject, loadProjectFromSession, loadGridFromSession, setGrid, setLayer, toggleLayer, setInspector } = useGridState();
    const loading = ref(false);
    const gridError = ref('');
    const soilLoading = ref(false);
    const username = (localStorage.getItem('username') || '').toLowerCase();
    const mapState = ref({
      latitude: 0,
      longitude: 0,
      pitch: 0,
      bearing: 0,
      zoom: 14,
    });
    const updateMapStateFromProject = () => {
      const geom = state.project?.geometry;
      if (!geom) return;
      const ring =
        geom.type === 'Polygon'
          ? geom.coordinates?.[0]
          : geom.type === 'Feature'
            ? geom.geometry?.coordinates?.[0]
            : geom.type === 'FeatureCollection'
              ? geom.features?.[0]?.geometry?.coordinates?.[0]
              : geom.type === 'MultiPolygon'
                ? geom.coordinates?.[0]?.[0]
                : null;
      if (!ring || !ring.length) return;
      const xs = ring.map((p) => p[0]);
      const ys = ring.map((p) => p[1]);
      const minX = Math.min(...xs);
      const maxX = Math.max(...xs);
      const minY = Math.min(...ys);
      const maxY = Math.max(...ys);
      mapState.value.longitude = (minX + maxX) / 2;
      mapState.value.latitude = (minY + maxY) / 2;
    };

    const project = computed(() => state.project);
    const layers = computed(() => state.layers);
    const inspector = computed(() => state.inspector);
    const grid = computed(() => state.grid || {});
    const demStats = computed(() => {
      const src = grid.value.dem || grid.value;
      const hm =
        src?.elevation_data?.heightmap ||
        src?.data?.elevation_data?.heightmap ||
        src?.heightmap ||
        src?.data?.heightmap ||
        null;
      if (!hm || !Array.isArray(hm) || !hm.length) return { min: null, max: null };
      let min = Infinity;
      let max = -Infinity;
      for (const row of hm) {
        if (!Array.isArray(row)) continue;
        for (const v of row) {
          if (typeof v !== 'number') continue;
          if (v < min) min = v;
          if (v > max) max = v;
        }
      }
      if (!Number.isFinite(min) || !Number.isFinite(max)) return { min: null, max: null };
      return { min, max };
    });
    const plainDem = computed(() => {
      const src = grid.value.dem || grid.value;
      if (!src) return null;
      try {
        return JSON.parse(JSON.stringify(src));
      } catch (e) {
        return src;
      }
    });

    const loadGrid = async (force = false) => {
      gridError.value = '';
      if (!props.id) return true;
      if (!force && state.grid?.dem) {
        // Already have DEM (possibly from session); skip fetch
        console.info('[grid] DEM present in state/session, skipping fetch');
        return true;
      }
      console.info('[grid] Fetching DEM from API…');
      const demResp = await fetchGrid(props.id, 'dem');
      const dem = demResp.data || demResp.layers?.dem || demResp;
      setLayer('dem', dem);
      console.info('[grid] DEM loaded and cached');
      return true;
    };

    const loadGridWithRetry = async () => {
      if (state.grid?.dem) return;
      const maxAttempts = 12;
      const delayMs = 5000;
      for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
        try {
          await loadGrid(true);
          gridError.value = '';
          return;
        } catch (e) {
          if (attempt >= maxAttempts) {
            gridError.value = 'Unable to load DEM data after multiple attempts. Please try again.';
            return;
          }
          gridError.value = `Loading DEM data… retrying (${attempt}/${maxAttempts})`;
          await new Promise((resolve) => setTimeout(resolve, delayMs));
        }
      }
    };

    const loadSoil = async () => {
      gridError.value = '';
      try {
        console.info('[grid] Fetching soil from API…');
        const soilResp = await fetchGrid(props.id, 'soil');
        const soil = soilResp?.data || soilResp?.layers?.soil || soilResp;
        if (soil) {
          setLayer('soil', soil);
          console.info('[grid] Soil loaded and cached');
        } else {
          gridError.value = 'Soil data unavailable.';
        }
      } catch (e) {
        gridError.value = 'Failed to load soil data.';
      }
    };

    const handlePick = ({ lat, lon, dem: demFromMap, row, col, soil: soilFromMap }) => {
      if (!state.grid) return;
      const topo = sampleTopography(lat, lon, state.grid.dem || state.grid) || {};
      const dem = demFromMap ?? topo.elevation ?? sampleDem(lat, lon, state.grid.dem || state.grid);
      const sampledSoil = soilFromMap || sampleSoil(lat, lon, state.grid);
      setInspector({
        lat,
        lon,
        dem,
        soil: sampledSoil,
        row: row ?? topo.row,
        col: col ?? topo.col,
        slope: topo.slope,
        aspect: topo.aspect,
      });
    };

    const toggle = (layer) => {
      toggleLayer(layer);
    };

    const handleSoil = async () => {
      gridError.value = '';
      // Always fetch if missing or still loading data
      if (!state.grid?.soil) {
        soilLoading.value = true;
        try {
          await loadSoil();
          if (state.grid?.soil && !state.layers.soil) {
            toggleLayer('soil');
          }
          if (!state.grid?.soil) {
            gridError.value = gridError.value || 'Soil data not available yet.';
          }
        } finally {
          soilLoading.value = false;
        }
        return;
      }
      console.info('[grid] Soil already present, toggling layer');
      toggleLayer('soil');
    };

    const handleBorder = () => {
      toggleLayer('border');
    };

    const fetchProjectById = async () => {
      if (!props.id || !username) return null;
      const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';
      const resp = await fetch(`${API_BASE}/api/platform/projects`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username }),
      }).catch(() => null);
      const body = resp ? await resp.json().catch(() => []) : [];
      if (Array.isArray(body)) {
        const found = body.find((p) => p.project_id === props.id);
        if (found) setProject(found);
        return found;
      }
      return null;
    };

    onMounted(async () => {
      const cached = loadProjectFromSession(props.id);
      if (cached) setProject(cached);
      // Try to restore grid from session first
      const cachedGrid = loadGridFromSession(props.id);
      if (cachedGrid) setGrid(cachedGrid);
      loading.value = true;
      if (!cached) {
        await fetchProjectById();
      }
      if (state.project) {
        await loadGridWithRetry();
        updateMapStateFromProject();
      }
      loading.value = false;
    });

    return {
      loading,
      project,
      layers,
      inspector,
      grid,
      plainDem,
      demStats,
      gridError,
      soilLoading,
      soilAttr: (soil, ...keys) => {
        if (!soil) return null;
        for (const k of keys) {
          if (Object.prototype.hasOwnProperty.call(soil, k) && soil[k] !== null && typeof soil[k] !== 'undefined') {
            return soil[k];
          }
        }
        return null;
      },
      toggle,
      handlePick,
      loadSoil,
      handleSoil,
      handleBorder,
      mapState,
    };
  },
};
</script>

<style scoped>
.main-view {
  padding: 32px;
  color: #e2e8f0;
}
.muted {
  color: #94a3b8;
}
.label {
  font-size: 12px;
  color: #94a3b8;
}
  .value {
    font-size: 16px;
    font-weight: 600;
    text-align: left;
  }
.geom {
  background: #0f172a;
  border: 1px solid #1e293b;
  border-radius: 12px;
  padding: 12px;
  color: #cbd5e1;
  overflow: auto;
}
.ghost {
  border: 1px solid #334155;
  background: transparent;
  color: #e2e8f0;
  border-radius: 10px;
  padding: 10px 14px;
  font-weight: 600;
  cursor: pointer;
  transition: background 0.2s, border-color 0.2s;
}
.ghost.active {
  background: #1e293b;
  border-color: #38bdf8;
}
.ghost.loading {
  border-style: dashed;
  animation: pulse 1s infinite;
}
@keyframes pulse {
  0% { box-shadow: 0 0 0 0 rgba(56, 189, 248, 0.4); }
  70% { box-shadow: 0 0 0 8px rgba(56, 189, 248, 0); }
  100% { box-shadow: 0 0 0 0 rgba(56, 189, 248, 0); }
}
.details {
  flex: 1;
  padding: 24px;
  min-height: calc(100vh - 120px);
}
.layout {
  display: grid;
  grid-template-columns: 220px 1fr 1fr;
  gap: 16px;
  align-items: start;
  height: calc(100vh - 100px);
}
.controls {
  display: flex;
  flex-direction: column;
  gap: 12px;
  background: #0f172a;
  border: 1px solid #1e293b;
  border-radius: 12px;
  padding: 14px;
}
.map-wrap {
  background: #0b1222;
  border: 1px solid #1e293b;
  border-radius: 14px;
  height: calc(100vh - 120px);
  aspect-ratio: 1 / 1;
  margin: 0;
  position: relative;
}
.inspector .value {
  font-size: 14px;
}
.inspector-card, .info-card {
  background: #0b1222;
  border: 1px solid #1e293b;
  border-radius: 12px;
  padding: 12px;
}
.inspector-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 8px;
}
.inspector-table th {
  text-align: left;
  padding: 4px 6px;
  color: #cbd5e1;
  font-size: 13px;
}
.inspector-table td {
  padding: 4px 6px;
  border-top: 1px solid #1e293b;
  font-size: 13px;
  color: #e2e8f0;
}
.alert {
  background: #1f2937;
  border: 1px solid #f59e0b;
  color: #fbbf24;
  border-radius: 10px;
  padding: 10px 12px;
  margin-top: 8px;
  font-size: 14px;
}
.value.small {
  font-size: 14px;
}
.muted.small {
  font-size: 13px;
}
.info-column {
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.row {
  display: flex;
  justify-content: space-between;
  gap: 8px;
  margin-bottom: 6px;
  text-align: left;
}
.info-card .row .value {
  text-align: left;
}
.panel-title {
  font-weight: 700;
  margin-bottom: 10px;
  color: #e2e8f0;
}
.control-grid {
  display: grid;
  grid-template-columns: 70px 1fr;
  gap: 8px 10px;
  margin-bottom: 12px;
}
.control-grid input {
  width: 100%;
  background: #0b1222;
  border: 1px solid #334155;
  color: #e2e8f0;
  padding: 6px 8px;
  border-radius: 8px;
  font-size: 12px;
}
</style>
