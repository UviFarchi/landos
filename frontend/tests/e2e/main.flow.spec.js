import { test, expect } from "@playwright/test";
import path from "path";

const polygonPath = path.resolve(process.cwd(), "../backend/tests/samples/valid_small.json");
const username = "e2euser";
const password = "e2epass123";

test("login, create project, load grid, inspect dem/soil, toggle layers", async ({ page }) => {
  await page.goto("/");

  // Sign up (idempotent if user exists, errors ignored)
  const signupTab = page.locator('.tabs').getByRole("button", { name: "Sign up" });
  await signupTab.click();
  const signupForm = page.locator('form', { hasText: "Create account" });
  await signupForm.getByLabel("Username").fill(username);
  await signupForm.getByLabel("Password").fill(password);
  await signupForm.getByRole("button", { name: "Sign up" }).click();

  // Sign in
  const signinTab = page.locator('.tabs').getByRole("button", { name: "Sign in" });
  await signinTab.click();
  const signinForm = page.locator('form', { hasText: "Sign in" });
  await signinForm.getByLabel("Username").fill(username);
  await signinForm.getByLabel("Password").fill(password);
  await signinForm.getByRole("button", { name: "Sign in" }).click();

  await page.waitForURL("**/projects");

  // Create a project from polygon file
  const fileInput = page.locator('input[type="file"]');
  await fileInput.setInputFiles(polygonPath);
  await page.getByRole("button", { name: "Create project from polygon" }).click();

  // Wait for project to appear and load it
  const loadButton = page.getByRole("button", { name: "Load" }).first();
  await expect(loadButton).toBeVisible({ timeout: 20000 });
  await Promise.all([
    page.waitForNavigation({ url: "**/projects/**", timeout: 60000 }),
    loadButton.click(),
  ]);
  const map = page.locator('[data-test="map-pane"]');
  await expect(map).toBeVisible();

  // Wait briefly for grid fetch then click map to inspect
  await page.waitForTimeout(3000);
  await map.click({ position: { x: 100, y: 100 } });
  await expect(page.getByText("DEM:")).toBeVisible({ timeout: 5000 });
  await expect(page.getByText("Soil:")).toBeVisible({ timeout: 5000 });

  // Toggle soil layer off/on
  const soilToggle = page.locator('[data-test="toggle-soil"]');
  await soilToggle.click();
  await soilToggle.click();
});
