<template>
  <div class="map-pane" ref="container" data-test="map-pane"></div>
</template>

<script>
import { Deck, COORDINATE_SYSTEM } from '@deck.gl/core';
import { PolygonLayer } from '@deck.gl/layers';
import { isProxy, toRaw } from 'vue';

export default {
  name: 'MapPane',
  props: {
    polygon: { type: Object, default: null },
    dem: { type: Object, default: null },
    soil: { type: Object, default: null },
    landCover: { type: Object, default: null },
    showDem: { type: Boolean, default: true },
    showSoil: { type: Boolean, default: false },
    showLandCover: { type: Boolean, default: false },
    showBorder: { type: Boolean, default: false },
    latitude: { type: Number, default: 0 },
    longitude: { type: Number, default: 0 },
    pitch: { type: Number, default: 0 },
    bearing: { type: Number, default: 0 },
    zoom: { type: Number, default: 14 },
  },
  emits: ['pick'],
  data() {
    return {
      deck: null,
      viewState: {
        longitude: this.longitude,
        latitude: this.latitude,
        zoom: this.zoom,
        pitch: this.pitch,
        bearing: this.bearing,
      },
      overControls: false,
      _layersDirty: true,
      _demCellsFrozen: null,
      _borderCellsFrozen: null,
      _demHeightmap: null,
    };
  },
  created() {
    this._hasFit = false;
    this._soilGrid = null;
    this._soilUnits = null;
    this._soilIndex = null;
    this._soilColorCache = {};
    this._landCoverGrid = null;
    this._landCoverIndex = null;
    this._landCoverUnits = null;
    this._landCoverColorCache = {};
    this._layersCache = null;
  },
  watch: {
    polygon: {
      deep: true,
      handler() {
        this._layersDirty = true;
        this._renderDeck();
      },
    },
    dem: {
      deep: true,
      handler() {
        this._prepareDemData();
        this._renderDeck();
      },
    },
    showDem() { this._layersDirty = true; this._renderDeck(); },
    showBorder() { this._layersDirty = true; this._renderDeck(); },
    showSoil() { this._layersDirty = true; this._renderDeck(); },
    latitude(val) { this._updateView({ latitude: val }); },
    longitude(val) { this._updateView({ longitude: val }); },
    pitch(val) { this._updateView({ pitch: Math.max(0, Math.min(89, val)) }); },
    bearing(val) { this._updateView({ bearing: Math.max(-180, Math.min(180, val)) }); },
    zoom(val) { this._updateView({ zoom: Math.max(2, Math.min(22, val)) }); },
    soil: {
      deep: true,
      handler() {
        this._prepareSoilData();
        this._renderDeck();
      },
    },
    landCover: {
      deep: true,
      handler() {
        this._prepareLandCoverData();
        this._renderDeck();
      },
    },
    showLandCover() { this._layersDirty = true; this._renderDeck(); },
  },
  mounted() {
    this._prepareDemData();
    this._prepareSoilData();
    this._prepareLandCoverData();
    this._renderDeck();
  },
  beforeUnmount() {
    if (this.deck) {
      this.deck.finalize();
      this.deck = null;
    }
  },
  methods: {
    _clone(obj) {
      if (obj == null) return obj;
      const raw = isProxy(obj) ? toRaw(obj) : obj;
      try { return JSON.parse(JSON.stringify(raw)); } catch (e) { return raw; }
    },
    handleMapClick(info) {
      if (this.overControls) return;
      const coord = info?.coordinate || info?.lngLat;
      const lon = Array.isArray(coord) ? coord[0] : info?.lon ?? info?.lngLat?.lon ?? info?.lngLat?.lng;
      const lat = Array.isArray(coord) ? coord[1] : info?.lat ?? info?.lngLat?.lat ?? info?.latlng?.lat;
      if (typeof lat !== 'number' || typeof lon !== 'number') return;
      const row = info?.object?.row ?? null;
      const col = info?.object?.col ?? null;
      const dem = info?.object?.elevation;
      let soilDetail = null;
      if (row != null && col != null && this._soilGrid) {
        const code = this._soilGrid?.[row]?.[col];
        if (code && code !== 0) {
          const key = this._soilIndex?.[code] ?? this._soilIndex?.[String(code)] ?? String(code);
          const unit = (this._soilUnits && (this._soilUnits[key] || this._soilUnits[code] || this._soilUnits[String(code)])) || {};
          soilDetail = { mukey: key, value: code, ...unit };
        }
      }
      let landCoverDetail = null;
      if (row != null && col != null && this._landCoverGrid) {
        const code = this._landCoverGrid?.[row]?.[col];
        if (code && code !== 0) {
          const key = this._landCoverIndex?.[String(code)] ?? String(code);
          const unit = (this._landCoverUnits && (this._landCoverUnits[key] || this._landCoverUnits[code] || this._landCoverUnits[String(code)])) || {};
          landCoverDetail = { code: key, value: code, ...unit };
        }
      }
      this.$emit('pick', { lat, lon, row, col, dem, soil: soilDetail, landCover: landCoverDetail });
    },
    _prepareDemData() {
      const src = this._clone(this.dem);
      const demData =
        src?.elevation_data ||
        src?.data?.layers?.dem ||
        src?.data?.elevation_data ||
        src?.layers?.dem ||
        src?.data ||
        (src?.heightmap ? src : null);
      if (!demData?.heightmap) {
        this._demCellsPlain = [];
        this._demMetaPlain = null;
        this._demHeightmap = null;
        return;
      }
      const hm = this._smoothHeightmap(demData.heightmap);
      const minElev = demData.min_elevation ?? 0;
      const maxElev = demData.max_elevation ?? minElev + 1;
      const t = demData.transform || [1, 0, 0, 0, -1, 0];
      const [a, b, c, d, e, f] = t.length >= 6 ? t : [1, 0, 0, 0, -1, 0];
      const rows = hm.length;
      const cols = hm[0]?.length || 0;
      if (!rows || !cols) {
        this._demCellsPlain = [];
        this._demMetaPlain = null;
        return;
      }
      const cells = [];
      let minX = Infinity; let maxX = -Infinity; let minY = Infinity; let maxY = -Infinity;
      for (let r = 0; r < rows; r += 1) {
        for (let cidx = 0; cidx < cols; cidx += 1) {
          const x0 = a * cidx + b * r + c;
          const y0 = d * cidx + e * r + f;
          const x1 = a * (cidx + 1) + b * r + c;
          const y1 = d * (cidx + 1) + e * (r + 1) + f;
          const h = hm[r][cidx] ?? 0;
          minX = Math.min(minX, x0, x1);
          maxX = Math.max(maxX, x0, x1);
          minY = Math.min(minY, y0, y1);
          maxY = Math.max(maxY, y0, y1);
          cells.push({
            elevation: h,
            row: r,
            col: cidx,
            polygon: [
              [x0, y0],
              [x1, y0],
              [x1, y1],
              [x0, y1],
            ],
          });
        }
      }
      let plainCells = cells;
      try { plainCells = JSON.parse(JSON.stringify(cells)); } catch (e) {}
      this._demCellsPlain = plainCells;
      this._demHeightmap = hm;
      try {
        this._demCellsFrozen = JSON.parse(JSON.stringify(plainCells));
        Object.freeze(this._demCellsFrozen);
      } catch (e) {
        this._demCellsFrozen = plainCells;
      }
      const centerLat = (minY + maxY) / 2;
      const metersPerDegLat = 111320;
      const metersPerDegLon = 111320 * Math.cos((centerLat * Math.PI) / 180);
      const dxMeters = ((maxX - minX) / Math.max(cols, 1)) * metersPerDegLon;
      const dyMeters = ((maxY - minY) / Math.max(rows, 1)) * metersPerDegLat;
      const range = Math.max(1e-6, maxElev - minElev); // avoid zero; deterministic from DEM
      // Reserve a small vertical band above the DEM for overlays; split evenly across layers
      const overlayBand = range * 0.03;
      const overlayStep = overlayBand / 3;
      const overlayThickness = overlayStep * 0.4;
      this._demMetaPlain = {
        minElev,
        maxElev,
        bounds: { left: minX, right: maxX, top: maxY, bottom: minY },
        rows,
        cols,
        dxMeters,
        dyMeters,
        overlayStep,
        overlayThickness,
      };
      this._layersDirty = true;
    },
    _smoothHeightmap(hm) {
      if (!Array.isArray(hm) || !hm.length || !Array.isArray(hm[0])) return hm;
      const rows = hm.length;
      const cols = hm[0].length;
      const out = Array.from({ length: rows }, () => Array(cols).fill(0));
      const neighbors = [
        [0, 0],
        [1, 0],
        [-1, 0],
        [0, 1],
        [0, -1],
        [1, 1],
        [1, -1],
        [-1, 1],
        [-1, -1],
      ];
      for (let r = 0; r < rows; r += 1) {
        for (let c = 0; c < cols; c += 1) {
          let sum = 0;
          let count = 0;
          for (const [dr, dc] of neighbors) {
            const rr = r + dr;
            const cc = c + dc;
            if (rr >= 0 && rr < rows && cc >= 0 && cc < cols && typeof hm[rr][cc] === 'number') {
              sum += hm[rr][cc];
              count += 1;
            }
          }
          out[r][c] = count ? sum / count : hm[r][c];
        }
      }
      return out;
    },
    _calcSlope(row, col) {
      if (!this._demHeightmap || !this._demMetaPlain) return 0;
      const { dxMeters, dyMeters } = this._demMetaPlain;
      const rows = this._demHeightmap.length;
      const cols = this._demHeightmap[0]?.length || 0;
      const r0 = Math.max(0, row - 1);
      const r1 = Math.min(rows - 1, row + 1);
      const c0 = Math.max(0, col - 1);
      const c1 = Math.min(cols - 1, col + 1);
      const north = this._demHeightmap[r0][col];
      const south = this._demHeightmap[r1][col];
      const west = this._demHeightmap[row][c0];
      const east = this._demHeightmap[row][c1];
      if (
        [north, south, west, east].some((v) => typeof v !== 'number') ||
        !dxMeters ||
        !dyMeters
      ) return 0;
      const dzdx = (east - west) / (2 * dxMeters);
      const dzdy = (south - north) / (2 * dyMeters);
      const slopeRad = Math.atan(Math.sqrt(dzdx * dzdx + dzdy * dzdy));
      const slopeDeg = (slopeRad * 180) / Math.PI;
      return slopeDeg;
    },
    _prepareSoilData() {
      const src = this._clone(this.soil);
      const soilData = src?.soil_data || src?.data?.soil_data || src?.layers?.soil || src?.soil || src;
      if (!soilData) {
        this._soilGrid = null;
        this._soilUnits = null;
        this._soilIndex = null;
        this._soilColorCache = {};
        this._layersDirty = true;
        return;
      }
      this._soilGrid = soilData.grid || null;
      this._soilUnits = soilData.units || soilData.map_units || null;
      this._soilIndex = soilData.index_map || soilData.indexMap || null;
       this._soilColorCache = {};
      this._layersDirty = true;
    },
    _prepareLandCoverData() {
      const src = this._clone(this.landCover);
      const lc = src?.land_cover || src?.data?.land_cover || src?.layers?.land_cover || src?.landcover || src;
      if (!lc) {
        this._landCoverGrid = null;
        this._landCoverUnits = null;
        this._landCoverIndex = null;
        this._landCoverColorCache = {};
        this._layersDirty = true;
        return;
      }
      this._landCoverGrid = lc.grid || lc.classification || null;
      this._landCoverIndex = lc.index_map || lc.indexMap || lc.legend || null;
      this._landCoverUnits = lc.units || null;
      this._landCoverColorCache = {};
      this._layersDirty = true;
    },
    _soilColorFor(key) {
      const k = key ?? 'soil';
      if (!this._soilColorCache[k]) {
        // keep colors in red/brown family; vary hue/sat/light per key for contrast
        const hash = Math.abs(
          String(k)
            .split('')
            .reduce((acc, ch) => ((acc << 5) - acc + ch.charCodeAt(0)) | 0, 0),
        );
        const rawHue = (hash % 70) - 10; // allow slight wrap into deep reds
        const hue = rawHue < 0 ? 360 + rawHue : rawHue; // 290-360 or 0-60: reds/browns
        const saturation = 55 + (hash % 35); // 55-89%
        const lightness = 25 + (hash % 45); // 25-69% for browns/reds
        const c = (1 - Math.abs(2 * lightness / 100 - 1)) * (saturation / 100);
        const hPrime = hue / 60;
        const x = c * (1 - Math.abs((hPrime % 2) - 1));
        let r = 0; let g = 0; let b = 0;
        if (hPrime >= 0 && hPrime < 1) { r = c; g = x; b = 0; }
        else if (hPrime < 2) { r = x; g = c; b = 0; }
        else if (hPrime < 3) { r = 0; g = c; b = x; }
        else if (hPrime < 4) { r = 0; g = x; b = c; }
        else if (hPrime < 5) { r = x; g = 0; b = c; }
        else { r = c; g = 0; b = x; }
        const m = lightness / 100 - c / 2;
        this._soilColorCache[k] = [
          Math.round((r + m) * 255),
          Math.round((g + m) * 255),
          Math.round((b + m) * 255),
        ];
      }
      return this._soilColorCache[k];
    },
    _landCoverColorFor(key) {
      const k = key ?? 'landcover';
      if (!this._landCoverColorCache[k]) {
        // blues/purples with wide lightness for contrast
        const hash = Math.abs(
          String(k)
            .split('')
            .reduce((acc, ch) => ((acc << 5) - acc + ch.charCodeAt(0)) | 0, 0),
        );
        const hue = 200 + (hash % 100); // 200-299 blue/purple
        const saturation = 50 + (hash % 45); // 50-94%
        const lightness = 25 + (hash % 55); // 25-79%
        const c = (1 - Math.abs(2 * lightness / 100 - 1)) * (saturation / 100);
        const hPrime = hue / 60;
        const x = c * (1 - Math.abs((hPrime % 2) - 1));
        let r = 0; let g = 0; let b = 0;
        if (hPrime >= 0 && hPrime < 1) { r = c; g = x; b = 0; }
        else if (hPrime < 2) { r = x; g = c; b = 0; }
        else if (hPrime < 3) { r = 0; g = c; b = x; }
        else if (hPrime < 4) { r = 0; g = x; b = c; }
        else if (hPrime < 5) { r = x; g = 0; b = c; }
        else { r = c; g = 0; b = x; }
        const m = lightness / 100 - c / 2;
        this._landCoverColorCache[k] = [
          Math.round((r + m) * 255),
          Math.round((g + m) * 255),
          Math.round((b + m) * 255),
        ];
      }
      return this._landCoverColorCache[k];
    },
    _buildLayers(ring = []) {
      const demLayer = this._demLayer(ring);
      const soilOverlay = this._soilOverlayLayer();
      const landCoverOverlay = this._landCoverOverlayLayer();
      const borderOverlay = this._borderOverlayLayer(ring);
      return [demLayer, soilOverlay, landCoverOverlay, borderOverlay].filter(Boolean);
    },
    _isInsidePolygon(point, ring) {
      // simple ray casting
      let inside = false;
      for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
        const xi = ring[i][0]; const yi = ring[i][1];
        const xj = ring[j][0]; const yj = ring[j][1];
        const intersect = ((yi > point[1]) !== (yj > point[1]))
          && (point[0] < ((xj - xi) * (point[1] - yi)) / (yj - yi + 1e-9) + xi);
        if (intersect) inside = !inside;
      }
      return inside;
    },
    _demLayer(ring = []) {
      if (!this.showDem || !this._demMetaPlain || !this._demCellsPlain?.length) return null;
      const minElev = this._demMetaPlain.minElev ?? 0;
      const maxElev = this._demMetaPlain.maxElev ?? minElev + 1;
      const dxMeters = this._demMetaPlain.dxMeters || 0;
      const dyMeters = this._demMetaPlain.dyMeters || 0;
      const data = this._demCellsFrozen || this._demCellsPlain;
      return new PolygonLayer({
        id: 'dem-layer',
        data,
        coordinateSystem: COORDINATE_SYSTEM.LNGLAT,
        extruded: true,
        wireframe: false,
        dataComparator: () => true,
        getPolygon: (d) => d.polygon,
        getElevation: (d) => d.elevation,
        elevationScale: 1,
        stroked: false,
        filled: true,
        pickable: true,
        material: { ambient: 0.4, diffuse: 0.6, shininess: 12, specularColor: [60, 60, 60] },
        updateTriggers: {
          getFillColor: [this.showBorder],
        },
        getFillColor: (d) => {
          const normRaw = Math.max(0, Math.min(1, (d.elevation - minElev) / Math.max(1, maxElev - minElev)));
          const norm = normRaw * normRaw * (3 - 2 * normRaw);
          const mix = (a, b, t) => Math.round(a + (b - a) * t);
          // Greyscale base for elevation
          const grey = mix(40, 235, norm);
          let base = [grey, grey, grey];
          // Slope shading
          if (dxMeters && dyMeters) {
            const slopeDeg = this._calcSlope(d.row, d.col);
            const slopeFactor = Math.max(0, Math.min(1, slopeDeg / 45));
            base = base.map((v) => Math.round(v * (1 - 0.35 * slopeFactor)));
          }
          if (this.showBorder && ring.length) {
            const cx = (d.polygon[0][0] + d.polygon[1][0] + d.polygon[2][0] + d.polygon[3][0]) / 4;
            const cy = (d.polygon[0][1] + d.polygon[1][1] + d.polygon[2][1] + d.polygon[3][1]) / 4;
            if (this._isInsidePolygon([cx, cy], ring)) {
              // leave base color; border overlay handled in separate layer
              return [...base, 230];
            }
          }
          return [...base, 230];
        },
      });
    },
    _borderOverlayLayer(ring = []) {
      if (!this.showBorder || !ring.length || !this._demCellsPlain?.length) return null;
      const insideCells = [];
      for (const cell of this._demCellsPlain) {
        const cx = (cell.polygon[0][0] + cell.polygon[1][0] + cell.polygon[2][0] + cell.polygon[3][0]) / 4;
        const cy = (cell.polygon[0][1] + cell.polygon[1][1] + cell.polygon[2][1] + cell.polygon[3][1]) / 4;
        if (this._isInsidePolygon([cx, cy], ring)) insideCells.push(cell);
      }
      let data = insideCells;
      try { data = JSON.parse(JSON.stringify(insideCells)); } catch (e) {}
      const step = this._demMetaPlain?.overlayStep || 0;
      const thickness = this._demMetaPlain?.overlayThickness || 0;
      const offset = step * 3; // above other overlays
      this._borderCellsFrozen = data.map((d) => ({
        ...d,
        topPolygon: d.polygon.map(([x, y]) => [x, y, (d.elevation ?? 0) + offset]),
      }));
      return new PolygonLayer({
        id: 'border-overlay',
        data: this._borderCellsFrozen,
        coordinateSystem: COORDINATE_SYSTEM.LNGLAT,
        extruded: true,
        stroked: false,
        filled: true,
        dataComparator: () => true,
        getPolygon: (d) => d.topPolygon || d.polygon,
        getElevation: () => thickness,
        elevationScale: 1.0,
        getFillColor: [245, 220, 40, 170],
        parameters: { depthTest: true, depthMask: false },
        pickable: false,
      });
    },
    _soilOverlayLayer() {
      if (!this.showSoil || !this._soilGrid || !this._demCellsPlain?.length) return null;
      const soilCells = [];
      for (const cell of this._demCellsPlain) {
        const r = cell.row;
        const c = cell.col;
        const code = this._soilGrid?.[r]?.[c];
        if (code === null || typeof code === 'undefined' || code === 0) continue;
        const key = this._soilIndex?.[code] ?? this._soilIndex?.[String(code)] ?? String(code);
        soilCells.push({
          ...cell,
          _soilKey: key,
        });
      }
      if (!soilCells.length) return null;
      const step = this._demMetaPlain?.overlayStep || 0;
      const thickness = this._demMetaPlain?.overlayThickness || 0;
      const offset = step; // first band above DEM
      let data = soilCells.map((d) => ({
        ...d,
        topPolygon: d.polygon.map(([x, y]) => [x, y, (d.elevation) + offset])
      }));
      try { data = JSON.parse(JSON.stringify(data)); } catch (e) {}
      return new PolygonLayer({
        id: 'soil-overlay',
        data,
        coordinateSystem: COORDINATE_SYSTEM.LNGLAT,
        extruded: true,
        stroked: false,
        filled: true,
        dataComparator: () => true,
        getPolygon: (d) => d.topPolygon || d.polygon,
        getElevation: (d) => thickness,
        elevationScale: 1,
        getFillColor: (d) => {
          const base = this._soilColorFor(d._soilKey);
          if (!base || base.length < 3) return [0, 0, 0, 0];
          return [...base, 255];
        },
        parameters: { depthTest: true, depthMask: false },
        pickable: false,
      });
    },
    _landCoverOverlayLayer() {
      if (!this.showLandCover || !this._landCoverGrid || !this._demCellsPlain?.length) return null;
      const lcCells = [];
      for (const cell of this._demCellsPlain) {
        const r = cell.row;
        const c = cell.col;
        const code = this._landCoverGrid?.[r]?.[c];
        if (code === null || typeof code === 'undefined' || code === 0) continue;
        const key = this._landCoverIndex?.[String(code)] ?? String(code);
        lcCells.push({
          ...cell,
          _lcKey: key,
        });
      }
      if (!lcCells.length) return null;
      const step = this._demMetaPlain?.overlayStep || 0;
      const thickness = this._demMetaPlain?.overlayThickness || 0;
      const offset = step * 2; // above soil band
      let data = lcCells.map((d) => ({
        ...d,
        topPolygon: d.polygon.map(([x, y]) => [x, y, (d.elevation) + offset])
      }));
      try { data = JSON.parse(JSON.stringify(data)); } catch (e) {}
      return new PolygonLayer({
        id: 'land-cover-overlay',
        data,
        coordinateSystem: COORDINATE_SYSTEM.LNGLAT,
        extruded: true,
        stroked: false,
        filled: true,
        dataComparator: () => true,
        getPolygon: (d) => d.topPolygon || d.polygon,
        getElevation: () => thickness,
        elevationScale: 1,
        getFillColor: (d) => {
          const base = this._landCoverColorFor(d._lcKey);
          if (!base || base.length < 3) return [0, 0, 0, 0];
          return [...base, 230];
        },
        parameters: { depthTest: true, depthMask: false },
        pickable: false,
      });
    },
    _extractCoords(geom) {
      if (!geom) return [];
      if (geom.type === 'Feature') return this._extractCoords(geom.geometry);
      if (geom.type === 'FeatureCollection') {
        const feature = geom.features && geom.features[0];
        return feature ? this._extractCoords(feature.geometry) : [];
      }
      if (geom.type === 'Polygon') return geom.coordinates[0];
      if (geom.type === 'MultiPolygon') return geom.coordinates[0][0];
      return [];
    },
    _renderDeck() {
      const container = this.$refs.container;
      if (typeof window === 'undefined' || !container) return;
      if (typeof WebGLRenderingContext === 'undefined') throw new Error('WebGL not supported');
      const rect = container.getBoundingClientRect();
      if (!rect.width || !rect.height) return;
      let ring = this._extractCoords(this._clone(this.polygon));
      if (ring.length > 1) {
        const first = ring[0];
        const last = ring[ring.length - 1];
        if (first[0] === last[0] && first[1] === last[1]) {
          ring = ring.slice(0, -1);
        }
      }
      this._ringPlain = ring;
      if (!this._hasFit) {
        if (ring.length) {
          const xs = ring.map((p) => p[0]);
          const ys = ring.map((p) => p[1]);
          const minX = Math.min(...xs);
          const maxX = Math.max(...xs);
          const minY = Math.min(...ys);
          const maxY = Math.max(...ys);
          this.viewState.longitude = (minX + maxX) / 2;
          this.viewState.latitude = (minY + maxY) / 2;
        } else if (this._demMetaPlain?.bounds) {
          const { left, right, top, bottom } = this._demMetaPlain.bounds;
          this.viewState.longitude = (left + right) / 2;
          this.viewState.latitude = (top + bottom) / 2;
        }
        this._hasFit = true;
      }
      const layers = [...this._buildLayers(ring)];
      if (!layers.length) return;
      const viewState = this._clone(this.viewState);
      const controller = {
        dragPan: false,
        dragRotate: false,
        touchRotate: false,
        doubleClickZoom: false,
      };
      const shared = {
        layers,
        onClick: (info) => this.handleMapClick(info),
        viewState,
        controller,
        width: rect.width,
        height: rect.height,
        getCursor: () => 'crosshair',
      };
      if (!this.deck) {
        this.deck = new Deck({ ...shared, parent: container });
      } else {
        this.deck.setProps({ ...shared });
      }
    },
    onLatChange(evt) {
      const val = parseFloat(evt.target.value);
      if (!Number.isNaN(val)) {
        this._updateView({ latitude: val });
      }
    },
    onLonChange(evt) {
      const val = parseFloat(evt.target.value);
      if (!Number.isNaN(val)) {
        this._updateView({ longitude: val });
      }
    },
    onPitchChange(evt) {
      const val = parseFloat(evt.target.value);
      if (!Number.isNaN(val)) {
        const clamped = Math.max(0, Math.min(89, val));
        this._updateView({ pitch: clamped });
      }
    },
    onBearingChange(evt) {
      const val = parseFloat(evt.target.value);
      if (!Number.isNaN(val)) {
        const clamped = Math.max(-180, Math.min(180, val));
        this._updateView({ bearing: clamped });
      }
    },
    onZoomChange(evt) {
      const val = parseFloat(evt.target.value);
      if (!Number.isNaN(val)) {
        const clamped = Math.max(2, Math.min(22, val));
        this._updateView({ zoom: clamped });
      }
    },
    _updateView(next) {
      const merged = { ...(isProxy(this.viewState) ? toRaw(this.viewState) : this.viewState), ...next };
      const clampedZoom = Math.max(2, Math.min(22, merged.zoom ?? this.viewState.zoom));
      this.viewState = { ...merged, zoom: clampedZoom };
      if (this.deck) {
        const plainVS = this._clone(this.viewState);
        this.deck.setProps({ viewState: plainVS });
      } else {
        this._renderDeck();
      }
    },
  },
};
</script>

<style scoped>
.map-pane {
  width: 100%;
  height: 100%;
  min-height: 320px;
  background: #0b1222;
  border-radius: 12px;
  overflow: hidden;
  position: relative;
}
.map-pane canvas {
  cursor: crosshair !important;
}
.hud {
  position: absolute;
  top: 10px;
  left: 10px;
  display: flex;
  gap: 8px;
  align-items: center;
  z-index: 2;
}
.compass {
  width: 32px;
  height: 32px;
  border-radius: 50%;
  background: rgba(15, 23, 42, 0.8);
  border: 1px solid #334155;
  display: grid;
  place-items: center;
  font-weight: 700;
  color: #38bdf8;
}
.compass-arrow {
  font-size: 14px;
  color: #38bdf8;
  transform-origin: 50% 50%;
}
.control-panel {
  position: absolute;
  bottom: 10px;
  left: 50%;
  transform: translateX(-50%);
  background: rgba(15, 23, 42, 0.85);
  border: 1px solid #334155;
  padding: 10px 12px;
  border-radius: 10px;
  z-index: 5;
}
.control-table {
  border-collapse: collapse;
  color: #e2e8f0;
  font-size: 12px;
}
.control-table td {
  padding: 4px 6px;
}
.control-table input {
  width: 120px;
  background: #0b1222;
  border: 1px solid #334155;
  color: #e2e8f0;
  padding: 4px 6px;
  border-radius: 6px;
  font-size: 12px;
}
</style>
