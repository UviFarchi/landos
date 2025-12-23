import { describe, it, expect } from "vitest";
import { latLonToRowCol, sampleDem, sampleSoil } from "../../src/utils/gridSampler";

const mockGrid = {
  bounds: { left: 0, right: 2, top: 2, bottom: 0 },
  transform: [1, 0, 0, 0, -1, 2], // a=1, e=-1, c=0, f=2
  elevation_data: {
    heightmap: [
      [10, 20],
      [30, 40],
    ],
  },
  soil_data: {
    map_units: [{ mukey: "mu1", muname: "Unit 1" }],
  },
};

describe("gridSampler", () => {
  it("converts lat/lon to row/col within bounds", () => {
    const { row, col } = latLonToRowCol(1.75, 0.25, mockGrid);
    expect(row).toBe(0);
    expect(col).toBe(0);
  });

  it("samples DEM height for a point inside the grid", () => {
    const value = sampleDem(1.25, 1.25, mockGrid);
    expect(value).toBe(10);
  });

  it("returns null for DEM outside bounds", () => {
    const value = sampleDem(5, 5, mockGrid);
    expect(value).toBeNull();
  });

  it("returns soil map unit when available", () => {
    const soil = sampleSoil(1, 1, mockGrid);
    expect(soil).toEqual(mockGrid.soil_data.map_units[0]);
  });
});
