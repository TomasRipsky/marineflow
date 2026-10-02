#!/usr/bin/env node
// Full-page screenshots of the Grafana dashboards and the Airflow graph, for the project site.
//
//   node site/tools/capture_shots.mjs                 # all five, into site/assets/shots/
//   node site/tools/capture_shots.mjs overview kafka  # only some
//
// Needs Google Chrome and the stack running (Grafana :3000, Airflow :4080) with data flowing, so no panel says "no data".
// Node 22+ (global WebSocket and fetch); no packages. Chrome runs headless with its own throw-away profile.
//
// Why a script: a dashboard is taller than any screen, so a manual capture cuts it. This one resizes the viewport to the
// full height of the page first. It signs in with the documented local development logins (override with the
// environment variables below). The "Latest alerts" table on the Live traffic dashboard shows MMSI numbers; they are
// blurred before the capture, because the site never shows a full vessel identifier.
//
// Environment: GRAFANA_URL (http://localhost:3000), GRAFANA_USER (admin), GRAFANA_PASSWORD (marineflow_dev),
//              AIRFLOW_URL (http://localhost:4080), AIRFLOW_USER / AIRFLOW_PASSWORD (admin / admin),
//              CHROME (path to the Chrome binary), WIDTH (1600), SETTLE_MS (12000: time panels get to load).
import { spawn } from "node:child_process";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const env = process.env;
const GRAFANA = env.GRAFANA_URL || "http://localhost:3000";
const AIRFLOW = env.AIRFLOW_URL || "http://localhost:4080";
const CHROME = env.CHROME || "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const WIDTH = Number(env.WIDTH || 1600);
const SETTLE_MS = Number(env.SETTLE_MS || 12000);
const OUT = resolve(dirname(fileURLToPath(import.meta.url)), "..", "assets", "shots");
const PORT = 9333;

const SHOTS = {
  "overview": { app: "grafana", path: "/d/marineflow-overview?orgId=1&kiosk&from=now-30m&to=now" },
  "live-traffic": { app: "grafana", path: "/d/marineflow-live?orgId=1&kiosk&from=now-15m&to=now", blurMmsi: true },
  "spark": { app: "grafana", path: "/d/marineflow-spark?orgId=1&kiosk&from=now-30m&to=now" },
  "kafka": { app: "grafana", path: "/d/marineflow-kafka?orgId=1&kiosk&from=now-30m&to=now" },
  "airflow": { app: "airflow", path: "/dags/marineflow_pipeline/graph" },
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function launchChrome() {
  const profile = mkdtempSync(join(tmpdir(), "marineflow-shots-"));
  const proc = spawn(CHROME, [
    "--headless=new", `--remote-debugging-port=${PORT}`, `--user-data-dir=${profile}`, "--hide-scrollbars",
    "--no-first-run", "--no-default-browser-check", "--force-color-profile=srgb",
    // the Live traffic map is a WebGL layer: without a GPU, headless Chrome needs the software renderer or it draws nothing
    "--use-angle=swiftshader", "--enable-unsafe-swiftshader", "--ignore-gpu-blocklist", "about:blank",
  ], { stdio: "ignore" });
  return { proc, profile };
}

async function connect() {
  for (let i = 0; i < 60; i++) {
    try {
      const targets = await (await fetch(`http://127.0.0.1:${PORT}/json/list`)).json();
      const page = targets.find((t) => t.type === "page");
      if (page) return page.webSocketDebuggerUrl;
    } catch { /* Chrome is still starting */ }
    await sleep(500);
  }
  throw new Error("Chrome did not start; set CHROME to the browser binary");
}

class Cdp {
  constructor(url) {
    this.ws = new WebSocket(url);
    this.id = 0;
    this.pending = new Map();
    this.opened = new Promise((ok, fail) => { this.ws.onopen = ok; this.ws.onerror = fail; });
    this.ws.onmessage = (event) => {
      const msg = JSON.parse(event.data);
      if (msg.id && this.pending.has(msg.id)) {
        const { ok, fail } = this.pending.get(msg.id);
        this.pending.delete(msg.id);
        msg.error ? fail(new Error(msg.error.message)) : ok(msg.result);
      }
    };
  }
  send(method, params = {}) {
    const id = ++this.id;
    this.ws.send(JSON.stringify({ id, method, params }));
    return new Promise((ok, fail) => this.pending.set(id, { ok, fail }));
  }
  async eval(expression) {
    const r = await this.send("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true });
    if (r.exceptionDetails) throw new Error(r.exceptionDetails.text + " " + (r.exceptionDetails.exception?.description || ""));
    return r.result.value;
  }
}

async function goto(cdp, url) {
  await cdp.send("Page.navigate", { url });
  for (let i = 0; i < 80; i++) {
    await sleep(250);
    if ((await cdp.eval("document.readyState")) === "complete") return;
  }
}

const fillForm = (userSel, passSel, user, pass) => `(() => {
  const set = (el, v) => { Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value").set.call(el, v); el.dispatchEvent(new Event("input", { bubbles: true })); };
  const u = document.querySelector(${JSON.stringify(userSel)}), p = document.querySelector(${JSON.stringify(passSel)});
  if (!u || !p) return false;
  set(u, ${JSON.stringify(user)}); set(p, ${JSON.stringify(pass)});
  (document.querySelector('button[type=submit], input[type=submit]')).click();
  return true;
})()`;

async function waitFor(cdp, selector, tries = 40) {
  for (let i = 0; i < tries; i++) {
    if (await cdp.eval(`!!document.querySelector(${JSON.stringify(selector)})`)) return true;
    await sleep(500);
  }
  return false;
}

async function login(cdp, app) {
  if (app === "grafana") {
    await goto(cdp, `${GRAFANA}/login`);
    await waitFor(cdp, 'input[type=password]');
    const ok = await cdp.eval(fillForm('input[placeholder="email or username"], input[name=user]', 'input[type=password]', env.GRAFANA_USER || "admin", env.GRAFANA_PASSWORD || "marineflow_dev"));
    if (!ok) throw new Error("Grafana login form not found");
  } else {
    await goto(cdp, `${AIRFLOW}/login/`);
    await waitFor(cdp, "#password");
    const ok = await cdp.eval(fillForm("#username", "#password", env.AIRFLOW_USER || "admin", env.AIRFLOW_PASSWORD || "admin"));
    if (!ok) throw new Error("Airflow login form not found");
  }
  await sleep(3000);
}

// the page's real height: Grafana scrolls an inner container, not the window
const PAGE_HEIGHT = `Math.max(document.documentElement.scrollHeight, ...[...document.querySelectorAll("*")]
  .filter((e) => { const o = getComputedStyle(e).overflowY; return (o === "auto" || o === "scroll") && e.scrollHeight > e.clientHeight; })
  .map((e) => e.getBoundingClientRect().top + e.scrollHeight))`;

const BLUR_MMSI = `(() => {
  const headers = [...document.querySelectorAll('[role=columnheader]')];
  const index = headers.findIndex((h) => h.textContent.trim() === "MMSI");
  if (index < 0) return 0;
  let blurred = 0;
  document.querySelectorAll('[role=row]').forEach((row) => {
    const cell = row.children[index];
    if (cell) { cell.style.filter = "blur(7px)"; blurred++; }
  });
  return blurred;
})()`;

async function capture(cdp, name, spec) {
  await cdp.send("Emulation.setDeviceMetricsOverride", { width: WIDTH, height: 1000, deviceScaleFactor: 1, mobile: false });
  await goto(cdp, (spec.app === "grafana" ? GRAFANA : AIRFLOW) + spec.path);
  await sleep(SETTLE_MS);
  let height = Math.ceil(await cdp.eval(PAGE_HEIGHT));
  height = Math.min(Math.max(height, 700), 4000);
  await cdp.send("Emulation.setDeviceMetricsOverride", { width: WIDTH, height, deviceScaleFactor: 1, mobile: false });
  await sleep(SETTLE_MS / 2);                       // panels re-flow and redraw at the new height
  if (spec.blurMmsi) console.log(`  ${name}: blurred ${await cdp.eval(BLUR_MMSI)} MMSI cells`);
  const loading = await cdp.eval(`document.body.innerText.includes("Loading plugin panel")`);
  if (loading) console.log(`  ${name}: warning, a panel was still loading; raise SETTLE_MS`);
  const { data } = await cdp.send("Page.captureScreenshot", { format: "png", captureBeyondViewport: false });
  const file = join(OUT, `${name}.png`);
  writeFileSync(file, Buffer.from(data, "base64"));
  console.log(`  ${name}: ${WIDTH}x${height} -> ${file}`);
}

const wanted = process.argv.slice(2);
const names = wanted.length ? wanted : Object.keys(SHOTS);
for (const n of names) if (!SHOTS[n]) { console.error(`unknown shot "${n}" (known: ${Object.keys(SHOTS).join(", ")})`); process.exit(2); }

mkdirSync(OUT, { recursive: true });
const { proc, profile } = launchChrome();
try {
  const cdp = new Cdp(await connect());
  await cdp.opened;
  await cdp.send("Page.enable");
  const signedIn = new Set();
  for (const name of names) {
    const spec = SHOTS[name];
    if (!signedIn.has(spec.app)) { await login(cdp, spec.app); signedIn.add(spec.app); }
    console.log(`capturing ${name}`);
    await capture(cdp, name, spec);
  }
} finally {
  proc.kill();
  await new Promise((done) => { proc.once("exit", done); setTimeout(done, 3000); });
  try { rmSync(profile, { recursive: true, force: true }); } catch { /* Chrome may still hold the profile; the OS temp dir clears it */ }
}
