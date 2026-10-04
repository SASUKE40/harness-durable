import { defineConfig } from "vitest/config";
import { cloudflareTest } from "@cloudflare/vitest-pool-workers";

export default defineConfig({
  plugins: [cloudflareTest({
    wrangler: { configPath: "./wrangler.toml" },
    miniflare: { bindings: { API_TOKEN: "test-token" } },
  })],
  test: { include: ["test/**/*.test.ts"] },
});
