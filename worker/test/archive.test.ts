import { SELF } from "cloudflare:test";
import { describe, it, expect } from "vitest";
import type { Manifest } from "../src/index";

const encoder = new TextEncoder();
async function fixture(collector = "collector", batch = crypto.randomUUID()): Promise<{ manifest: Manifest; body: string }> {
  const body = "synthetic lance file";
  const sha256 = [...new Uint8Array(await crypto.subtle.digest("SHA-256", encoder.encode(body)))].map(x => x.toString(16).padStart(2, "0")).join("");
  return { body, manifest: { schema_version: 1, collector_id: collector, batch_id: batch, created_at: "2026-10-01T00:00:00Z",
    sessions: [{ harness: "pi", session_id: "session", records: 1, events: 1, first_timestamp: null, last_timestamp: null }],
    files: ["records.lance/_versions/1.manifest", "events.lance/_versions/1.manifest"].map(path => ({ path, size: body.length, sha256 })) } };
}
function request(archive: string, path: string, method = "GET", body?: string, authorized = true) {
  return SELF.fetch(`https://example.test/v1/archives/${archive}/${path}`, { method, headers: { ...(authorized ? { Authorization: "Bearer test-token" } : {}), ...(body !== undefined ? { "Content-Length": String(encoder.encode(body).length) } : {}) }, body });
}
const batchPath = (m: Manifest) => `batches/${m.collector_id}/${m.batch_id}`;

describe("archive publication", () => {
  it("requires authentication before accessing an archive", async () => {
    expect((await request("auth", "batches", "GET", undefined, false)).status).toBe(401);
  });
  it("hides incomplete uploads and validates the complete inventory", async () => {
    const archive = crypto.randomUUID(); const { manifest: m, body } = await fixture(); const path = batchPath(m);
    expect((await request(archive, path, "PUT", JSON.stringify(m))).status).toBe(201);
    expect(await (await request(archive, "batches")).json()).toEqual({ batches: [], next: null });
    expect((await request(archive, `${path}/commit`, "POST")).status).toBe(409);
    expect((await request(archive, `${path}/files/${m.files[0].path}`)).status).toBe(404);
    for (const file of m.files) expect((await request(archive, `${path}/files/${file.path}`, "PUT", body)).status).toBe(200);
    expect((await request(archive, `${path}/commit`, "POST")).status).toBe(200);
    const listed = await (await request(archive, "batches")).json() as { batches: Manifest[] }; expect(listed.batches).toEqual([m]);
    expect(new TextDecoder().decode(await (await request(archive, `${path}/files/${m.files[0].path}`)).arrayBuffer())).toBe(body);
    const sessions = await (await request(archive, "sessions")).json() as { sessions: unknown[] }; expect(sessions.sessions).toEqual(m.sessions);
  });
  it("rejects a checksum mismatch and does not publish it", async () => {
    const archive = crypto.randomUUID(); const { manifest: m, body } = await fixture(); const path = batchPath(m);
    await request(archive, path, "PUT", JSON.stringify(m));
    expect((await request(archive, `${path}/files/${m.files[0].path}`, "PUT", "x".repeat(body.length))).status).toBe(400);
    expect((await request(archive, `${path}/commit`, "POST")).status).toBe(409);
  });
  it("supports interrupted uploads, identical retries, and rejects changed IDs", async () => {
    const archive = crypto.randomUUID(); const { manifest: m, body } = await fixture(); const path = batchPath(m);
    await request(archive, path, "PUT", JSON.stringify(m));
    await request(archive, `${path}/files/${m.files[0].path}`, "PUT", body);
    expect((await request(archive, path, "PUT", JSON.stringify(m))).status).toBe(200);
    expect((await request(archive, `${path}/files/${m.files[0].path}`, "PUT", "x".repeat(body.length))).status).toBe(409);
    const changed = structuredClone(m); changed.sessions[0].records++;
    expect((await request(archive, path, "PUT", JSON.stringify(changed))).status).toBe(409);
    for (const f of m.files) expect((await request(archive, `${path}/files/${f.path}`, "PUT", body)).status).toBe(200);
    expect((await request(archive, `${path}/commit`, "POST")).status).toBe(200);
    expect((await request(archive, `${path}/commit`, "POST")).status).toBe(200);
    expect((await request(archive, path, "PUT", JSON.stringify(m))).status).toBe(200);
  });
  it("isolates concurrent collectors and archives", async () => {
    const archive = crypto.randomUUID(); const fixtures = await Promise.all([fixture("machine-a", "same-batch"), fixture("machine-b", "same-batch")]);
    await Promise.all(fixtures.map(async ({ manifest: m, body }) => {
      const path = batchPath(m); expect((await request(archive, path, "PUT", JSON.stringify(m))).status).toBe(201);
      await Promise.all(m.files.map(f => request(archive, `${path}/files/${f.path}`, "PUT", body)));
      expect((await request(archive, `${path}/commit`, "POST")).status).toBe(200);
    }));
    const result = await (await request(archive, "batches")).json() as { batches: Manifest[] }; expect(result.batches).toHaveLength(2);
    expect(await (await request(crypto.randomUUID(), "batches")).json()).toEqual({ batches: [], next: null });
  });
  it("rejects unsupported schemas and unsafe inventory paths", async () => {
    const archive = crypto.randomUUID(); const { manifest: m } = await fixture();
    m.files[0].path = "records.lance/../../secret";
    expect((await request(archive, batchPath(m), "PUT", JSON.stringify(m))).status).toBe(400);
    m.schema_version = 99; expect((await request(archive, batchPath(m), "PUT", JSON.stringify(m))).status).toBe(400);
  });
});
