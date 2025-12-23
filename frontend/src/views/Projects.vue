<template>
  <div class="projects-page">
    <header class="topbar">
      <div>
        <h1>Projects</h1>
        <p class="muted">Load existing projects or create a new one from a polygon upload.</p>
      </div>
      <div class="user">
        <div>
          <span class="label">User</span>
          <strong>{{ username || 'Unknown' }}</strong>
        </div>
        <button class="ghost small" data-test="logout" @click="logout">Logout</button>
      </div>
    </header>

    <section class="workspace" v-if="token">
      <aside class="projects">
        <div class="header">
          <h3>Your projects</h3>
          <span class="count">{{ projects.length }}</span>
        </div>
        <ul>
          <li
            v-for="project in projects"
            :key="project.project_id"
            :class="{ active: selectedProject && project.project_id === selectedProject.project_id }"
          >
            <div class="item-top" @click="selectProject(project)">
              <div>
                <div class="name">{{ project.name }}</div>
                <div class="meta">{{ project.country || '—' }} · {{ project.subdivision_name || project.subdivision || '—' }}</div>
              </div>
            </div>
            <div class="item-actions">
              <button class="ghost small" @click.stop="loadProject(project)">Load</button>
              <button class="danger small" @click.stop="deleteProject(project)">Delete</button>
            </div>
          </li>
          <li v-if="projects.length === 0" class="empty">No projects yet.</li>
        </ul>
      </aside>

      <main class="details">
        <div class="split">
          <div class="info-card">
            <h2>{{ selectedProject?.name || 'No project selected' }}</h2>
            <p class="muted">Project overview</p>
            <div v-if="selectedProject" class="grid">
              <div>
                <div class="label">Area</div>
                <div class="value">{{ selectedProject.area_hectares || '—' }} ha</div>
              </div>
              <div>
                <div class="label">Location</div>
                <div class="value">{{ selectedProject.country || '—' }} · {{ selectedProject.subdivision_name || selectedProject.subdivision || '—' }}</div>
              </div>
              <div>
                <div class="label">Owner</div>
                <div class="value">{{ selectedProject.username }}</div>
              </div>
              <div>
                <div class="label">Created</div>
                <div class="value">{{ selectedProject.created || '—' }}</div>
              </div>
              <div class="fullspan">
                <div class="label">Geometry</div>
                <pre class="geom">{{ selectedProject.geometry ? JSON.stringify(selectedProject.geometry, null, 2) : '—' }}</pre>
              </div>
            </div>
            <div v-else class="muted">Select or create a project to see details.</div>
          </div>

          <div class="actions-card">
            <div class="upload">
              <p class="label">Create new project</p>
              <label class="label">Name (optional)</label>
              <input v-model="newProjectName" class="text-input" placeholder="Enter project name" />
              <label class="upload-box">
                <input type="file" accept=".json,.geojson" @change="onFile" />
                <span>Drop polygon file or click to upload</span>
                <svg v-if="previewPath" :viewBox="previewViewBox" class="preview">
                  <path :d="previewPath" fill="rgba(56,189,248,0.35)" stroke="#38bdf8" stroke-width="2" />
                </svg>
              </label>
              <button class="solid full" :disabled="!pendingGeometry" @click="createProject">Create project from polygon</button>
              <p v-if="uploadError" class="error">{{ uploadError }}</p>
              <p v-if="status" class="status">{{ status }}</p>
            </div>
            <div class="next-steps">
              <p class="label">Jump to</p>
              <router-link to="/selection" class="ghost full">Selection view</router-link>
            </div>
          </div>
        </div>
      </main>
    </section>

    <section v-else class="locked">
      <p>Please sign in first.</p>
      <router-link class="solid" to="/">Go to sign in</router-link>
    </section>
  </div>
</template>

<script>
const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';

export default {
  name: 'ProjectsView',
  data() {
    return {
      projects: [],
      selectedProject: null,
      pendingGeometry: null,
      uploadError: '',
      status: '',
      token: localStorage.getItem('token') || null,
      username: (localStorage.getItem('username') || '').toLowerCase(),
      newProjectName: '',
      previewPath: '',
      previewViewBox: '0 0 100 100',
    };
  },
  async created() {
    if (this.token && this.username) {
      await this.fetchProjects();
    }
  },
  methods: {
    async fetchProjects() {
      const resp = await fetch(`${API_BASE}/api/platform/projects`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: this.username }),
      });
      const body = await resp.json().catch(() => []);
      if (Array.isArray(body)) {
        this.projects = body;
        this.selectedProject = this.projects[0] || null;
      }
    },
    selectProject(project) {
      this.selectedProject = project;
    },
    loadProject(project) {
      this.selectedProject = project;
      try {
        sessionStorage.setItem(`project:${project.project_id}`, JSON.stringify(project));
      } catch (e) {
        // ignore storage errors
      }
      this.$router.push({ name: 'Project', params: { id: project.project_id } });
    },
    async deleteProject(project) {
      const resp = await fetch(`${API_BASE}/api/platform/projects/${project.project_id}`, {
        method: 'DELETE',
        headers: {
          'Content-Type': 'application/json',
          ...(this.token ? { Authorization: `Bearer ${this.token}` } : {}),
        },
        body: JSON.stringify({ username: this.username }),
      });
      if (!resp.ok) {
        this.uploadError = 'Delete failed';
        return;
      }
      this.status = 'Project deleted';
      await this.fetchProjects();
    },
    onFile(event) {
      const file = event.target.files?.[0];
      if (file) {
        this.uploadError = '';
        const reader = new FileReader();
        reader.onload = () => {
          try {
            this.pendingGeometry = JSON.parse(reader.result);
            this.buildPreview(this.pendingGeometry);
          } catch (e) {
            this.uploadError = 'Invalid JSON file';
          }
        };
        reader.readAsText(file);
      }
    },
    async createProject() {
      if (!this.pendingGeometry) {
        this.uploadError = 'Please upload a polygon file first.';
        return;
      }
      const payload = {
        username: this.username,
        name: this.newProjectName || undefined,
        geometry: this.pendingGeometry,
      };
      const resp = await fetch(`${API_BASE}/api/platform/projects`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(this.token ? { Authorization: `Bearer ${this.token}` } : {}),
        },
        body: JSON.stringify(payload),
      });
      const body = await resp.json().catch(() => ({}));
      if (!resp.ok) {
        this.uploadError = body.error || body.detail || 'Project creation failed';
        return;
      }
      this.status = 'Project created';
      this.uploadError = '';
      this.pendingGeometry = null;
      this.newProjectName = '';
      this.previewPath = '';
      await this.fetchProjects();
    },
    buildPreview(geom) {
      try {
        const coords = this.extractCoords(geom);
        if (!coords.length) {
          this.previewPath = '';
          return;
        }
        const xs = coords.map(([x]) => x);
        const ys = coords.map(([, y]) => y);
        const minX = Math.min(...xs);
        const maxX = Math.max(...xs);
        const minY = Math.min(...ys);
        const maxY = Math.max(...ys);
        const width = maxX - minX || 1;
        const height = maxY - minY || 1;
        const norm = coords.map(([x, y]) => [
          ((x - minX) / width) * 100,
          100 - ((y - minY) / height) * 100,
        ]);
        const d = ['M', norm[0][0], norm[0][1], ...norm.slice(1).flat(), 'Z'].join(' ');
        this.previewPath = d;
        this.previewViewBox = '0 0 100 100';
      } catch (e) {
        this.previewPath = '';
      }
    },
    extractCoords(geom) {
      if (!geom) return [];
      if (geom.type === 'Feature') return this.extractCoords(geom.geometry);
      if (geom.type === 'FeatureCollection') {
        const feature = geom.features && geom.features[0];
        return feature ? this.extractCoords(feature.geometry) : [];
      }
      if (geom.type === 'Polygon') return geom.coordinates[0];
      if (geom.type === 'MultiPolygon') return geom.coordinates[0][0];
      return [];
    },
    async logout() {
      const token = localStorage.getItem('token');
      if (token) {
        try {
          await fetch(`${API_BASE}/api/platform/logout`, {
            method: 'POST',
            headers: { Authorization: `Bearer ${token}` },
          });
        } catch (e) {
          // ignore errors
        }
      }
      localStorage.removeItem('token');
      localStorage.removeItem('username');
      sessionStorage.clear();
      this.$router.push('/');
    },
  },
};
</script>

<style scoped>
.projects-page {
  padding: 24px;
  max-width: 1200px;
  margin: 0 auto;
  color: #e2e8f0;
}
.topbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}
.muted {
  color: #94a3b8;
}
.user {
  background: #0f172a;
  border: 1px solid #1e293b;
  padding: 10px 14px;
  border-radius: 10px;
  display: flex;
  gap: 8px;
  align-items: center;
}
.label {
  font-size: 12px;
  color: #94a3b8;
}
.workspace {
  background: #0b1222;
  border: 1px solid #1e293b;
  border-radius: 16px;
  display: flex;
  min-height: 520px;
  overflow: hidden;
}
.projects {
  width: 280px;
  border-right: 1px solid #1e293b;
  padding: 20px;
  background: linear-gradient(180deg, #0f172a 0%, #0b1222 100%);
}
.projects .header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 12px;
}
.projects .count {
  background: #1e293b;
  padding: 4px 8px;
  border-radius: 12px;
  font-size: 12px;
  color: #cbd5e1;
}
.projects ul {
  list-style: none;
  padding: 0;
  margin: 0;
}
.projects li {
  padding: 12px;
  border-radius: 12px;
  cursor: pointer;
  transition: background 0.2s, border 0.2s;
  border: 1px solid transparent;
}
.projects .item-actions {
  margin-top: 8px;
  display: flex;
  gap: 8px;
}
.small {
  padding: 6px 10px;
  font-size: 13px;
}
.danger {
  background: #ef4444;
  color: #0b1222;
  border: none;
}
.projects li:hover {
  border-color: #1e293b;
  background: rgba(148, 163, 184, 0.08);
}
.projects li.active {
  border-color: #38bdf8;
  background: rgba(56, 189, 248, 0.12);
}
.projects li.empty {
  color: #94a3b8;
  cursor: default;
}
.projects .name {
  font-weight: 600;
}
.projects .meta {
  color: #94a3b8;
  font-size: 12px;
}
.details {
  flex: 1;
  padding: 24px;
}
.split {
  display: grid;
  grid-template-columns: 1.2fr 1fr;
  gap: 16px;
}
.info-card,
.actions-card {
  background: #0f172a;
  border: 1px solid #1e293b;
  border-radius: 14px;
  padding: 20px;
}
.info-card h2 {
  margin: 0 0 4px;
}
.grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
}
.actions-card .upload {
  margin-bottom: 16px;
}
.upload-box {
  display: block;
  border: 1px dashed #334155;
  border-radius: 12px;
  padding: 0;
  text-align: center;
  color: #cbd5e1;
  cursor: pointer;
  background: rgba(15, 23, 42, 0.7);
  aspect-ratio: 1 / 1;
  position: relative;
  overflow: hidden;
  display: grid;
  place-items: center;
}
.upload-box input {
  display: none;
}
.upload-box .preview {
  position: absolute;
  inset: 0;
  width: 100%;
  height: 100%;
}
.text-input {
  width: 100%;
  padding: 8px 10px;
  border-radius: 8px;
  border: 1px solid #1e293b;
  background: #0b1222;
  color: #e2e8f0;
  margin-bottom: 8px;
}
.full {
  width: 100%;
}
button,
.ghost,
.solid {
  border: none;
  border-radius: 10px;
  padding: 10px 14px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s;
}
.solid {
  background: #38bdf8;
  color: #0b1222;
}
.ghost {
  background: transparent;
  color: #e2e8f0;
  border: 1px solid #334155;
}
.error {
  color: #fca5a5;
  margin-top: 8px;
}
.status {
  color: #22c55e;
  margin-top: 8px;
}
.locked {
  text-align: center;
  margin: 120px auto;
}
@media (max-width: 960px) {
  .workspace {
    flex-direction: column;
  }
  .projects {
    width: 100%;
    border-right: none;
    border-bottom: 1px solid #1e293b;
  }
  .split {
    grid-template-columns: 1fr;
  }
}
</style>
