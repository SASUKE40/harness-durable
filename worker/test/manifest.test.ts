import { describe, it, expect } from "vitest";
import { validate } from "../src/index";
import shared from "../../tests/fixtures/manifests.json";

describe("shared manifest rules", () => {
  for (const c of shared.cases) {
    it(`${c.valid ? "accepts" : "rejects"} ${c.name}`, () => {
      const manifest = { ...structuredClone(shared.base), ...structuredClone(c.patch) };
      if (c.valid) expect(() => validate(manifest)).not.toThrow();
      else expect(() => validate(manifest)).toThrow();
    });
  }
});
