import { DurableObject } from "cloudflare:workers";

export interface Env {
  BUCKET: R2Bucket;
  ARCHIVES: DurableObjectNamespace<Archive>;
  API_TOKEN: string;
}
interface Inventory { path: string; size: number; sha256: string }
interface Session { harness: string; session_id: string; records: number; events: number; first_timestamp: string | null; last_timestamp: string | null }
export interface Manifest {
  schema_version: number; collector_id: string; batch_id: string;
  created_at: string; sessions: Session[]; files: Inventory[];
}
class HttpError extends Error { constructor(public status: number, message: string) { super(message); } }
function check(ok: unknown, status: number, message: string): asserts ok { if (!ok) throw new HttpError(status, message); }
const id = (s: unknown): s is string => typeof s === "string" && /^[a-zA-Z0-9_-]{1,128}$/.test(s);
const safePath = (s: unknown): s is string => typeof s === "string" && s.length < 900 && /^(records|events)\.lance\//.test(s) && !s.includes("\\") && s.split("/").every(p => p !== "" && p !== "." && p !== "..");
function canonical(v: unknown): string {
  if (Array.isArray(v)) return `[${v.map(canonical).join(",")}]`;
  if (v && typeof v === "object") return `{${Object.entries(v).sort(([a], [b]) => a.localeCompare(b)).map(([k, val]) => `${JSON.stringify(k)}:${canonical(val)}`).join(",")}}`;
  return JSON.stringify(v);
}
function validate(value: unknown): asserts value is Manifest {
  check(value && typeof value === "object", 400, "invalid manifest");
  const m = value as Manifest;
  check(m.schema_version === 1 && id(m.collector_id) && id(m.batch_id), 400, "invalid schema or batch identity");
  check(typeof m.created_at === "string" && Number.isFinite(Date.parse(m.created_at)), 400, "invalid created_at");
  check(Array.isArray(m.files) && m.files.length > 0 && m.files.length <= 10000, 400, "invalid inventory");
  const paths = new Set<string>();
  for (const f of m.files) {
    check(f && safePath(f.path) && Number.isSafeInteger(f.size) && f.size >= 0 && /^[a-f0-9]{64}$/.test(f.sha256), 400, "invalid file");
    check(!paths.has(f.path), 400, "duplicate file"); paths.add(f.path);
  }
  for (const dataset of ["records", "events"]) check(m.files.some(f => f.path.startsWith(`${dataset}.lance/_versions/`)), 400, "missing Lance manifest");
  check(Array.isArray(m.sessions) && m.sessions.length <= 10000, 400, "invalid sessions");
  for (const s of m.sessions) check(s && typeof s.harness === "string" && typeof s.session_id === "string" && s.session_id.length <= 1024 && Number.isSafeInteger(s.records) && s.records >= 0 && Number.isSafeInteger(s.events) && s.events >= 0, 400, "invalid session summary");
}
const json = (value: unknown, status = 200) => Response.json(value, { status });

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    try {
      check(env.API_TOKEN?.length > 0, 503, "API token not configured");
      const authorization = request.headers.get("Authorization") ?? "";
      // Hash both values so timing does not depend on token length or prefix.
      const digest = async (s: string) => new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s)));
      const [actual, expected] = await Promise.all([digest(authorization), digest(`Bearer ${env.API_TOKEN}`)]);
      let diff = 0; for (let i = 0; i < actual.length; i++) diff |= actual[i] ^ expected[i];
      check(diff === 0, 401, "unauthorized");
      const url = new URL(request.url);
      const parts = url.pathname.split("/").filter(Boolean);
      check(parts[0] === "v1" && parts[1] === "archives" && id(parts[2]), 404, "unknown route");
      // IDs isolate archives; this deployment intentionally has a single owner.
      const stub = env.ARCHIVES.get(env.ARCHIVES.idFromName(parts[2]));
      return await stub.fetch(request);
    } catch (error) { return failure(error); }
  },
} satisfies ExportedHandler<Env>;

function failure(error: unknown): Response {
  if (error instanceof HttpError) return json({ error: error.message }, error.status);
  console.error("archive operation failed", error instanceof Error ? error.message : "unknown error");
  return json({ error: "archive operation failed" }, 500);
}

export class Archive extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS batches (key TEXT PRIMARY KEY, manifest TEXT NOT NULL, committed INTEGER NOT NULL DEFAULT 0)`);
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS files (batch TEXT NOT NULL, path TEXT NOT NULL, size INTEGER NOT NULL, sha256 TEXT NOT NULL, PRIMARY KEY(batch,path))`);
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS sessions (batch TEXT NOT NULL, harness TEXT NOT NULL, session TEXT NOT NULL, summary TEXT NOT NULL, PRIMARY KEY(batch,harness,session))`);
  }
  private row(key: string): { manifest: string; committed: number } | undefined {
    return this.ctx.storage.sql.exec<{ manifest: string; committed: number }>("SELECT manifest,committed FROM batches WHERE key=?", key).toArray()[0];
  }
  async fetch(request: Request): Promise<Response> {
    try { return await this.route(request); } catch (error) { return failure(error); }
  }
  private async route(request: Request): Promise<Response> {
    const url = new URL(request.url); const parts = url.pathname.split("/").filter(Boolean);
    const archive = parts[2]; const action = parts[3];
    if (request.method === "GET" && parts.length === 4 && action === "batches") {
      const after = url.searchParams.get("after") ?? "";
      const rows = this.ctx.storage.sql.exec<{ key: string; manifest: string }>("SELECT key,manifest FROM batches WHERE committed=1 AND key>? ORDER BY key LIMIT 101", after).toArray();
      return json({ batches: rows.slice(0, 100).map(r => JSON.parse(r.manifest)), next: rows.length > 100 ? rows[99].key : null });
    }
    if (request.method === "GET" && parts.length === 4 && action === "sessions") {
      // Batch summaries are paginated, so a session spanning batches is not
      // incorrectly presented as a deduplicated global count.
      const after = url.searchParams.get("after") ?? "";
      const rows = this.ctx.storage.sql.exec<{ cursor: string; summary: string }>(`SELECT s.batch || ':' || hex(s.harness) || ':' || hex(s.session) AS cursor,s.summary FROM sessions s JOIN batches b ON b.key=s.batch WHERE b.committed=1 AND (s.batch || ':' || hex(s.harness) || ':' || hex(s.session))>? ORDER BY cursor LIMIT 101`, after).toArray();
      return json({ sessions: rows.slice(0, 100).map(r => JSON.parse(r.summary)), next: rows.length > 100 ? rows[99].cursor : null });
    }
    check(action === "batches" && id(parts[4]) && id(parts[5]), 404, "unknown route");
    const key = `${parts[4]}/${parts[5]}`;
    const prefix = `${archive}/${key}`;
    if (request.method === "PUT" && parts.length === 6) {
      // Bound metadata only; binary upload bodies are streamed directly to R2.
      const reader = request.body?.getReader(); check(reader, 400, "missing manifest");
      const chunks: Uint8Array[] = []; let size = 0;
      for (;;) { const { done, value } = await reader.read(); if (done) break; size += value.byteLength; if (size > 1024 * 1024) { await reader.cancel(); throw new HttpError(413, "manifest too large"); } chunks.push(value); }
      const bytes = new Uint8Array(size); let offset = 0; for (const c of chunks) { bytes.set(c, offset); offset += c.length; }
      let m: unknown; try { m = JSON.parse(new TextDecoder().decode(bytes)); } catch { throw new HttpError(400, "invalid JSON"); }
      validate(m); check(`${m.collector_id}/${m.batch_id}` === key, 409, "batch identity mismatch");
      const encoded = canonical(m); const old = this.row(key);
      if (old) { check(old.manifest === encoded, 409, "conflicting batch ID"); return json({ committed: Boolean(old.committed) }); }
      this.ctx.storage.sql.exec("INSERT INTO batches(key,manifest) VALUES(?,?)", key, encoded);
      return json({ committed: false }, 201);
    }
    const row = this.row(key); check(row, 404, "batch not found");
    const m = JSON.parse(row.manifest) as Manifest;
    if (parts[6] === "files" && parts.length > 7) {
      const path = parts.slice(7).join("/"); check(safePath(path), 400, "invalid path");
      const file = m.files.find(f => f.path === path); check(file, 404, "file not registered");
      const objectKey = `${prefix}/${path}`;
      if (request.method === "GET") {
        check(row.committed === 1, 404, "batch not published");
        const object = await this.env.BUCKET.get(objectKey); check(object, 404, "file not found");
        return new Response(object.body, { headers: { "Content-Length": String(object.size), "ETag": object.httpEtag, "Content-Type": "application/octet-stream" } });
      }
      if (request.method === "PUT") {
        check(Number(request.headers.get("Content-Length")) === file.size, 400, "file length mismatch");
        const old = await this.env.BUCKET.head(objectKey);
        if (old) { check(old.size === file.size && old.customMetadata?.sha256 === file.sha256, 409, "conflicting object");
          // A retry still has a streaming body. Consume and validate it before
          // returning, so the client/proxy can safely reuse the connection.
          const digest = new crypto.DigestStream("SHA-256");
          if (request.body) await request.body.pipeTo(digest); else await digest.getWriter().close();
          const actual = [...new Uint8Array(await digest.digest)].map(b => b.toString(16).padStart(2, "0")).join("");
          check(actual === file.sha256, 409, "conflicting upload body");
          this.ctx.storage.sql.exec("INSERT OR IGNORE INTO files VALUES(?,?,?,?)", key, path, file.size, file.sha256); return json({ uploaded: true }); }
        check(row.committed === 0, 409, "published archive is missing an object");
        check(request.body || file.size === 0, 400, "missing file body");
        // R2 validates SHA-256 against the incoming stream before accepting it.
        const checksum = Uint8Array.from(file.sha256.match(/../g)!.map(h => parseInt(h, 16)));
        let result: R2Object | null;
        try { result = await this.env.BUCKET.put(objectKey, request.body ?? new Uint8Array(), { sha256: checksum, customMetadata: { sha256: file.sha256 }, onlyIf: { etagDoesNotMatch: "*" } }); }
        catch (error) {
          if (/checksum|digest|sha.?256/i.test(String(error))) throw new HttpError(400, "checksum mismatch");
          throw error;
        }
        if (!result) { const raced = await this.env.BUCKET.head(objectKey); check(raced?.size === file.size && raced.customMetadata?.sha256 === file.sha256, 409, "conflicting upload"); }
        else check(result.size === file.size, 400, "stored file length mismatch");
        this.ctx.storage.sql.exec("INSERT OR IGNORE INTO files VALUES(?,?,?,?)", key, path, file.size, file.sha256);
        return json({ uploaded: true });
      }
    }
    if (request.method === "POST" && parts.length === 7 && parts[6] === "commit") {
      if (row.committed) return json({ committed: true });
      const uploaded = this.ctx.storage.sql.exec<{ path: string; size: number; sha256: string }>("SELECT path,size,sha256 FROM files WHERE batch=?", key).toArray();
      check(uploaded.length === m.files.length && m.files.every(f => uploaded.some(u => u.path === f.path && u.size === f.size && u.sha256 === f.sha256)), 409, "incomplete inventory");
      // File rows only follow successful checksum-verified R2 writes. No delete
      // API exists, and publication plus metadata is one SQLite transaction.
      this.ctx.storage.transactionSync(() => {
        for (const s of m.sessions) this.ctx.storage.sql.exec("INSERT OR IGNORE INTO sessions VALUES(?,?,?,?)", key, s.harness, s.session_id, JSON.stringify(s));
        this.ctx.storage.sql.exec("UPDATE batches SET committed=1 WHERE key=?", key);
      });
      return json({ committed: true });
    }
    throw new HttpError(404, "unknown route");
  }
}
