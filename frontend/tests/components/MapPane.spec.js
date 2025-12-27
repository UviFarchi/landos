import { mount } from "@vue/test-utils";
import { describe, it, expect } from "vitest";
import MapPane from "../../src/components/MapPane.vue";

const polygon = {
  type: "Polygon",
  coordinates: [
    [
      [0, 0],
      [0, 1],
      [1, 1],
      [1, 0],
      [0, 0],
    ],
  ],
};

describe("MapPane", () => {
  it("emits pick event when handleMapClick is called", async () => {
    const wrapper = mount(MapPane, { props: { polygon } });
    const evt = { lngLat: { lat: 10, lon: 20 } };
    await wrapper.vm.handleMapClick(evt);
    const emitted = wrapper.emitted("pick");
    expect(emitted).toBeTruthy();
    expect(emitted[0][0]).toMatchObject({ lat: 10, lon: 20 });
  });

  it("does not emit pick when over controls", async () => {
    const wrapper = mount(MapPane, { props: { polygon } });
    wrapper.vm.overControls = true;
    await wrapper.vm.handleMapClick({ lngLat: { lat: 5, lon: 5 } });
    expect(wrapper.emitted("pick")).toBeFalsy();
  });
});
