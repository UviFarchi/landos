// Basic browser-like globals for component tests.
if (typeof window !== "undefined") {
  if (!window.matchMedia) {
    window.matchMedia = () => ({
      matches: false,
      addListener: () => {},
      removeListener: () => {},
      addEventListener: () => {},
      removeEventListener: () => {},
      dispatchEvent: () => false,
    });
  }

  if (typeof window.WebGLRenderingContext === "undefined") {
    window.WebGLRenderingContext = function WebGLRenderingContext() {};
  }
  if (!HTMLCanvasElement.prototype.getContext) {
    HTMLCanvasElement.prototype.getContext = () => ({
      getExtension: () => null,
      clearColor: () => {},
      clear: () => {},
    });
  }
}
