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
  data: {
    bounds: { left: 0, right: 2, top: 2, bottom: 0 },
    transform: [1, 0, 0, 0, -1, 2],
    elevation_data: { heightmap: [[10, 20],[30, 40]] },
  },
};

const soilResponse = {
  data: { soil_data: { map_units: [{ mukey: "mu1", muname: "Unit 1" }] } },
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
    expect(state.grid.dem).toBeTruthy();
    expect(state.grid.soil).toBeUndefined();
  });

  it("loads soil on button click", async () => {
    const wrapper = mount(MainView, { props: { id: mockProject.project_id } });
    await flushPromises();
    const soilButton = wrapper.get('[data-test="soil-button"]');
    await soilButton.trigger("click");
    await flushPromises();
    const { state } = useGridState();
    expect(state.grid.soil).toBeTruthy();
  });

  it("shows inspector data when map emits pick", async () => {
    const wrapper = mount(MainView, { props: { id: mockProject.project_id } });
    await flushPromises();
    await wrapper.get('[data-test="soil-button"]').trigger("click");
    const map = wrapper.getComponent({ name: "MapPane" });
    map.vm.$emit("pick", { lat: 1.25, lon: 1.25 });
    await flushPromises();
    expect(wrapper.text()).toContain("DEM: 10");
    expect(wrapper.text()).toContain("Soil: Unit 1");
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
