import { mount, flushPromises } from "@vue/test-utils";
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import MainView from "../../src/views/Main.vue";
import { useGridState } from "../../src/composables/useGridState";

const mockProject = {
  project_id: "p1",
  name: "Test",
  username: "alice",
  area_hectares: 150,
  country: "USA",
  subdivision: "19187",
  subdivision_name: "County",
  geometry: { type: "Polygon", coordinates: [[[0, 0],[0,1],[1,1],[1,0],[0,0]]] },
};

const demResponse = {
  layers: {
    dem: {
      bounds: { left: 0, right: 2, top: 2, bottom: 0 },
      transform: [1, 0, 0, 0, -1, 2],
      elevation_data: { heightmap: [[10, 20],[30, 40]] },
    },
  },
};

const soilResponse = {
  layers: {
    soil: {
      bounds: { left: 0, right: 2, top: 2, bottom: 0 },
      transform: [1, 0, 0, 0, -1, 2],
      grid: [
        [1, 1],
        [1, 1],
      ],
      index_map: { "1": "mu1" },
      units: { mu1: { mukey: "mu1", muname: "Unit 1", compname: "Comp" } },
    },
  },
};

const landCoverResponse = {
  layers: {
    land_cover: {
      bounds: { left: 0, right: 2, top: 2, bottom: 0 },
      transform: [1, 0, 0, 0, -1, 2],
      grid: [
        [7, 8],
        [9, 0],
      ],
      index_map: { "7": "Forest", "8": "Water", "9": "Urban" },
      units: { "7": { name: "Forest" }, "8": { name: "Water" }, "9": { name: "Urban" } },
    },
  },
};

describe("MainView", () => {
  let fetchSpy;

  beforeEach(() => {
    sessionStorage.clear();
    sessionStorage.setItem(`project:${mockProject.project_id}`, JSON.stringify(mockProject));
    fetchSpy = vi.spyOn(global, "fetch").mockImplementation((url) => {
      if (url.includes("layer=soil")) {
        return Promise.resolve({ ok: true, json: async () => soilResponse });
      }
      if (url.includes("layer=land_cover")) {
        return Promise.resolve({ ok: true, json: async () => landCoverResponse });
      }
      return Promise.resolve({ ok: true, json: async () => demResponse });
    });
  });

  afterEach(() => {
    fetchSpy.mockRestore();
  });

  it("loads grid on mount and caches dem/soil", async () => {
    const wrapper = mount(MainView, { props: { id: mockProject.project_id } });
    await flushPromises();
    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const { state } = useGridState();
    expect(state.grid.layers?.dem).toBeTruthy();
    expect(state.grid.layers?.soil).toBeUndefined();
  });

  it("loads soil on button click", async () => {
    const wrapper = mount(MainView, { props: { id: mockProject.project_id } });
    await flushPromises();
    const soilButton = wrapper.get('[data-test="soil-button"]');
    await soilButton.trigger("click");
    await flushPromises();
    const { state } = useGridState();
    expect(state.grid.layers?.soil).toBeTruthy();
  });

  it("loads land cover on button click", async () => {
    const wrapper = mount(MainView, { props: { id: mockProject.project_id } });
    await flushPromises();
    const lcButton = wrapper.get('[data-test="land-cover-button"]');
    await lcButton.trigger("click");
    await flushPromises();
    const { state } = useGridState();
    expect(state.grid.layers?.land_cover).toBeTruthy();
  });

  it("shows inspector data when map emits pick", async () => {
    const wrapper = mount(MainView, { props: { id: mockProject.project_id } });
    await flushPromises();
    await wrapper.get('[data-test="soil-button"]').trigger("click");
    await wrapper.get('[data-test="land-cover-button"]').trigger("click");
    const map = wrapper.getComponent({ name: "MapPane" });
    map.vm.$emit("pick", { lat: 1.25, lon: 1.25 });
    await flushPromises();
    const { state } = useGridState();
    expect(state.inspector?.dem).toBe(10);
    expect(state.inspector?.soil?.muname || state.inspector?.soil?.mukey).toBe("Unit 1");
    expect(state.inspector?.landCover?.code || state.inspector?.landCover?.name).toBe("Forest");
    expect(wrapper.text()).toContain("Elevation");
    expect(wrapper.text()).toContain("10");
  });

  it("toggles border layer with button", async () => {
    const wrapper = mount(MainView, { props: { id: mockProject.project_id } });
    await flushPromises();
    const { state } = useGridState();
    expect(state.layers.border).toBe(false);
    const btn = wrapper.get('[data-test="border-button"]');
    await btn.trigger("click");
    expect(state.layers.border).toBe(true);
  });

});
