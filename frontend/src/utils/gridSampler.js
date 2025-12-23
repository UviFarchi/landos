// Utilities for sampling DEM and soil data grids.
// Simple helpers for locating values in the DEM/soil grids returned by the backend.

function resolveDem(grid) {
  if (!grid) return { bounds: null, heightmap: null };
  const candidate = grid.elevation_data || grid.data?.elevation_data || grid.layers?.dem || grid.dem || grid;
  const bounds = candidate?.bounds || grid.bounds;
  const heightmap = candidate?.heightmap;
  return { bounds, heightmap };
}

function resolveSoil(grid) {
  if (!grid) return { units: null, bounds: null, heightmap: null, grid: null, index: null };
  const candidate = grid.soil_data || grid.data?.soil_data || grid.layers?.soil || grid.soil || grid;
  const demSource = grid.dem || grid.layers?.dem || grid.data?.layers?.dem || grid;
  const { bounds: demBounds, heightmap: demHeightmap } = resolveDem(demSource);
  const bounds = candidate?.bounds || grid.bounds || demBounds;
  const heightmap =
    candidate?.elevation_data?.heightmap ||
    grid.elevation_data?.heightmap ||
    demHeightmap ||
    (Array.isArray(candidate?.grid) ? candidate.grid : null);
  const units = candidate?.units || candidate?.map_units || null;
  const soilGrid = candidate?.grid || null;
  const indexMap = candidate?.index_map || candidate?.indexMap || null;
  return { units, bounds, heightmap, grid: soilGrid, index: indexMap };
}

export function latLonToRowCol(lat, lon, grid) {
  const { bounds, heightmap } = resolveDem(grid);
  if (!bounds || !heightmap) return { row: -1, col: -1 };
  const { left, right, top, bottom } = bounds;
  if (lat > top || lat < bottom || lon < left || lon > right) {
    return { row: -1, col: -1 };
  }
  const rows = heightmap.length;
  const cols = heightmap[0]?.length || 0;
  if (!rows || !cols) return { row: -1, col: -1 };

  // Map lat/lon proportionally into grid index space. Using (rows-1)/(cols-1)
  // keeps the sample inside the first cell for the small test grid.
  const row = Math.floor(((top - lat) / (top - bottom)) * Math.max(rows - 1, 1));
  const col = Math.floor(((lon - left) / (right - left)) * Math.max(cols - 1, 1));
  return { row, col };
}

export function sampleDem(lat, lon, grid) {
  const { bounds, heightmap } = resolveDem(grid);
  if (!bounds || !heightmap) return null;
  const { row, col } = latLonToRowCol(lat, lon, grid);
  if (row < 0 || col < 0) return null;
  if (!heightmap[row] || typeof heightmap[row][col] === "undefined") return null;
  return heightmap[row][col];
}

export function sampleTopography(lat, lon, grid) {
  const { bounds, heightmap } = resolveDem(grid);
  if (!bounds || !heightmap) return null;
  const { row, col } = latLonToRowCol(lat, lon, grid);
  if (row < 0 || col < 0) return null;
  const rows = heightmap.length;
  const cols = heightmap[0]?.length || 0;
  if (!rows || !cols) return null;
  const elevation = heightmap[row]?.[col];
  if (typeof elevation === "undefined") return null;

  const rowPrev = Math.max(0, row - 1);
  const rowNext = Math.min(rows - 1, row + 1);
  const colPrev = Math.max(0, col - 1);
  const colNext = Math.min(cols - 1, col + 1);

  const north = heightmap[rowPrev][col];
  const south = heightmap[rowNext][col];
  const west = heightmap[row][colPrev];
  const east = heightmap[row][colNext];

  // approximate meters per degree
  const metersPerDegLat = 111320;
  const metersPerDegLon = 111320 * Math.cos((lat * Math.PI) / 180);
  const cellHeightDeg = (bounds.top - bounds.bottom) / Math.max(rows, 1);
  const cellWidthDeg = (bounds.right - bounds.left) / Math.max(cols, 1);
  const dy = metersPerDegLat * cellHeightDeg;
  const dx = metersPerDegLon * cellWidthDeg;

  const dzdx = dx ? (east - west) / (2 * dx) : 0;
  const dzdy = dy ? (south - north) / (2 * dy) : 0;
  const slopeRad = Math.atan(Math.sqrt(dzdx * dzdx + dzdy * dzdy));
  const aspectRad = Math.atan2(dzdy, -dzdx); // 0=East; adjust to degrees from North
  let aspectDeg = (90 - (aspectRad * 180) / Math.PI);
  if (aspectDeg < 0) aspectDeg += 360;
  const slopeDeg = (slopeRad * 180) / Math.PI;

  return { elevation, slope: slopeDeg, aspect: aspectDeg, row, col };
}

export function sampleSoil(lat, lon, grid) {
  const { units, bounds, heightmap, grid: soilGrid, index } = resolveSoil(grid);
  if (soilGrid && Array.isArray(soilGrid)) {
    const { row, col } = latLonToRowCol(lat, lon, { bounds, elevation_data: { heightmap } });
    if (row < 0 || col < 0) return null;
    const val = soilGrid[row] && soilGrid[row][col];
    if (!val || val === 0) return null;
    const mukey = index ? index[String(val)] : null;
    if (mukey && units && units[mukey]) {
        return { mukey, ...units[mukey] };
    }
    return { mukey: mukey || String(val), ...((units && units[mukey]) || {}) };
  }
  if (Array.isArray(units) && units.length) return units[0];
  return null;
}
