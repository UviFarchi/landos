// Playwright config for the frontend e2e flow.
// Expects the frontend dev server to be running (e.g., npm run dev) and the backend available at VITE_API_BASE.

import { defineConfig } from "@playwright/test";

const baseURL = process.env.E2E_BASE_URL || "http://localhost:5173";

export default defineConfig({
  testDir: "./tests/e2e",
  timeout: 120000,
  use: {
    baseURL,
    headless: true,
    launchOptions: {
      args: ["--no-sandbox"],
    },
  },
});
