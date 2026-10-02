/* Renders the results sections from window.MARINEFLOW (assets/data.js, generated from BigQuery). No libraries. */
(function () {
  "use strict";
  var D = window.MARINEFLOW;
  var $ = function (id) { return document.getElementById(id); };
  var fmt = function (n) { return Math.round(n).toLocaleString("en-US"); };
  var pretty = function (s) { return String(s).replace(/_/g, " ").replace(/^./, function (c) { return c.toUpperCase(); }); };
  var el = function (tag, cls, html) { var e = document.createElement(tag); if (cls) e.className = cls; if (html != null) e.innerHTML = html; return e; };
  var regionNames = typeof Intl !== "undefined" && Intl.DisplayNames ? new Intl.DisplayNames(["en"], { type: "region" }) : null;
  var countryName = function (code) { try { return regionNames ? regionNames.of(code) : code; } catch (e) { return code; } };

  if (!D) {
    $("snapshot-note").textContent = "The results data failed to load.";
    return;
  }

  /* hero ------------------------------------------------------------- */
  var stats = [
    [fmt(D.silver.positions), "positions processed"],
    [fmt(D.silver.vessels), "distinct vessels"],
    [fmt(D.silver.flags), "flag states"],
    [fmt(D.gold.km), "km sailed (estimated)"]
  ];
  stats.forEach(function (s) { $("hero-stats").appendChild(el("div", "stat", "<b>" + s[0] + "</b><span>" + s[1] + "</span>")); });
  (function snapshotNote() {
    var ms = function (t) { return new Date(t.replace(" ", "T").replace(/\+00$/, "Z")).getTime(); };
    var mins = Math.round((ms(D.silver.last_ts) - ms(D.silver.first_ts)) / 60000);
    $("snapshot-note").textContent = "Snapshot read from BigQuery on " + D.generated_at + ": live positions from " +
      D.silver.first_ts.slice(0, 16) + " to " + D.silver.last_ts.slice(0, 16) + " UTC, " + Math.floor(mins / 60) + " h " + (mins % 60) +
      " min of the feed. Not a 24-hour run.";
  })();
  $("footer-note").textContent = "Data snapshot: " + D.generated_at;

  /* heat map --------------------------------------------------------- */
  (function drawMap() {
    var c = $("map"), ctx = c.getContext("2d"), W = c.width, H = c.height;
    var latTop = 78, latBottom = -58;                      // crop the poles, nobody sails there
    var X = function (lon) { return (lon + 180) / 360 * W; };
    var Y = function (lat) { return (latTop - lat) / (latTop - latBottom) * H; };
    ctx.fillStyle = "#020a14"; ctx.fillRect(0, 0, W, H);
    ctx.strokeStyle = "rgba(56,212,255,.10)"; ctx.lineWidth = 1; ctx.font = "11px ui-monospace, Menlo, monospace"; ctx.fillStyle = "rgba(143,176,200,.55)";
    for (var lon = -150; lon <= 150; lon += 30) { ctx.beginPath(); ctx.moveTo(X(lon), 0); ctx.lineTo(X(lon), H); ctx.stroke(); ctx.fillText(Math.abs(lon) + (lon < 0 ? "W" : "E"), X(lon) + 4, H - 6); }
    for (var lat = -30; lat <= 60; lat += 30) { ctx.beginPath(); ctx.moveTo(0, Y(lat)); ctx.lineTo(W, Y(lat)); ctx.stroke(); ctx.fillText(Math.abs(lat) + (lat < 0 ? "S" : "N"), 6, Y(lat) - 4); }
    var max = Math.log(1 + Math.max.apply(null, D.grid.map(function (g) { return g[2]; })));
    ctx.globalCompositeOperation = "lighter";
    D.grid.slice().sort(function (a, b) { return a[2] - b[2]; }).forEach(function (g) {
      var t = Math.log(1 + g[2]) / max, x = X(g[1]), y = Y(g[0]), r = 7 + t * 26;
      var grad = ctx.createRadialGradient(x, y, 0, x, y, r);
      grad.addColorStop(0, "rgba(" + Math.round(120 + 135 * t) + "," + Math.round(210 + 40 * t) + ",255," + (0.35 + 0.55 * t) + ")");
      grad.addColorStop(0.45, "rgba(56,212,255," + (0.18 + 0.3 * t) + ")");
      grad.addColorStop(1, "rgba(25,181,165,0)");
      ctx.fillStyle = grad; ctx.beginPath(); ctx.arc(x, y, r, 0, 6.2832); ctx.fill();
    });
    ctx.globalCompositeOperation = "source-over";
  })();

  /* ingestion timeline: break the axis where there is no data -------- */
  (function timeline() {
    var host = $("timeline"), peak = Math.max.apply(null, D.minutes.map(function (m) { return m.n; }));
    var toMin = function (t) { return parseInt(t.slice(0, 2), 10) * 60 + parseInt(t.slice(3), 10); };
    D.minutes.forEach(function (m, i) {
      if (i > 0) {
        var gap = toMin(m.t) - toMin(D.minutes[i - 1].t) - 5;            // five-minute buckets
        if (gap >= 5) host.appendChild(el("div", "gap", "no<br>data<br>" + Math.floor(gap / 60) + "h " + (gap % 60) + "m"));
      }
      var col = el("div", "col"), bar = el("div", "bar");
      bar.dataset.h = Math.max(3, Math.round(m.n / peak * 100));
      bar.title = m.t + " UTC, 5 min: " + fmt(m.n) + " positions";
      col.appendChild(bar);
      if (i === 0 || m.t.slice(3) === "00" || (i > 0 && toMin(m.t) - toMin(D.minutes[i - 1].t) > 5)) col.appendChild(el("span", "tick", m.t));
      host.appendChild(col);
    });
  })();

  /* horizontal bars -------------------------------------------------- */
  function hbars(id, rows, label) {
    var host = $(id), top = Math.max.apply(null, rows.map(function (r) { return r.n; }));
    rows.forEach(function (r) {
      var row = el("div", "row"), track = el("div", "track"), fill = el("div", "fill");
      fill.dataset.w = (r.n / top * 100).toFixed(1);
      track.appendChild(fill);
      row.appendChild(el("span", "label", label(r)));
      row.appendChild(track);
      row.appendChild(el("span", "val", fmt(r.n)));
      host.appendChild(row);
    });
  }
  hbars("flags", D.flags, function (r) { return countryName(r.k) + " <span style='color:var(--mist)'>" + r.k + "</span>"; });
  hbars("types", D.types, function (r) { return pretty(r.k); });
  hbars("regions", D.regions, function (r) { return pretty(r.k); });

  /* tables ----------------------------------------------------------- */
  (function ports() {
    var t = $("ports");
    t.innerHTML = "<thead><tr><th>Port</th><th class='n'>Vessels</th><th class='n'>Position reports</th><th class='n'>Cargo</th><th class='n'>Tanker</th><th class='n'>Passenger</th><th class='n'>Avg speed (kn)</th></tr></thead>";
    var body = el("tbody");
    D.ports.forEach(function (p) {
      body.appendChild(el("tr", "", "<td>" + p.port_name + " <span style='color:var(--mist)'>" + p.port_country + "</span></td><td class='n'>" + fmt(p.unique_vessels) +
        "</td><td class='n'>" + fmt(p.total_position_reports) + "</td><td class='n'>" + fmt(p.cargo_vessels) + "</td><td class='n'>" + fmt(p.tanker_vessels) +
        "</td><td class='n'>" + fmt(p.passenger_vessels) + "</td><td class='n'>" + p.avg_speed.toFixed(1) + "</td>"));
    });
    t.appendChild(body);
  })();

  (function risk() {
    var t = $("risk");
    t.innerHTML = "<thead><tr><th>Vessel</th><th>Flag</th><th>Type</th><th class='n'>Dark events</th><th class='n'>Speed anomalies</th><th class='n'>Spoofing</th><th class='n'>Loitering</th><th class='n'>Score</th></tr></thead>";
    var body = el("tbody");
    D.top_risk.forEach(function (r) {
      var cls = r.risk_score >= 25 ? "r25" : r.risk_score >= 20 ? "r20" : "r10";
      body.appendChild(el("tr", "", "<td style='font-family:var(--mono)'>" + r.mmsi + "</td><td>" + r.flag_country + "</td><td>" + pretty(r.t) + "</td><td class='n'>" + r.dark +
        "</td><td class='n'>" + r.speed + "</td><td class='n'>" + r.spoof + "</td><td class='n'>" + r.loiter + "</td><td class='n'><span class='pill " + cls + "'>" + r.risk_score + "</span></td>"));
    });
    t.appendChild(body);
  })();

  /* case file -------------------------------------------------------- */
  (function caseFile() {
    var C = D.case, p = C.positions;
    if (!p || p.length < 2) { $("casetext").textContent = "No case available in this snapshot."; return; }
    var rad = function (d) { return d * Math.PI / 180; };
    var km = (function () {
      var a = rad(p[0].lat), b = rad(p[1].lat), dl = rad(p[1].lon - p[0].lon), dp = b - a;
      var h = Math.sin(dp / 2) * Math.sin(dp / 2) + Math.cos(a) * Math.cos(b) * Math.sin(dl / 2) * Math.sin(dl / 2);
      return 2 * 6371 * Math.asin(Math.sqrt(h));
    })();
    var coord = function (q) { return Math.abs(q.lat).toFixed(2) + "°" + (q.lat < 0 ? "S" : "N") + " " + Math.abs(q.lon).toFixed(2) + "°" + (q.lon < 0 ? "W" : "E"); };
    var ts = function (s) { return new Date(s.replace(" ", "T").replace(/\+00$/, "Z")).getTime(); };
    var dt = (ts(p[1].ts) - ts(p[0].ts)) / 1000;

    // fit the map to the two points, with room around them, whatever the case is
    var spanLon = Math.abs(p[1].lon - p[0].lon), spanLat = Math.abs(p[1].lat - p[0].lat);
    var padLon = Math.max(10, spanLon * 0.35), padLat = Math.max(8, spanLat * 0.35);
    var lonMin = Math.min(p[0].lon, p[1].lon) - padLon, lonMax = Math.max(p[0].lon, p[1].lon) + padLon;
    var latMin = Math.min(p[0].lat, p[1].lat) - padLat, latMax = Math.max(p[0].lat, p[1].lat) + padLat, W = 600, H = 320;
    var nice = function (span) { var steps = [1, 2, 5, 10, 20, 30, 45]; for (var k = 0; k < steps.length; k++) if (span / steps[k] <= 5) return steps[k]; return 60; };
    var X = function (lon) { return (lon - lonMin) / (lonMax - lonMin) * W; }, Y = function (lat) { return (latMax - lat) / (latMax - latMin) * H; };
    var svg = $("casemap"), out = "", gl = "stroke='rgba(56,212,255,.12)'", gt = "fill='rgba(143,176,200,.6)' font-size='11' font-family='ui-monospace,monospace'";
    var lonStep = nice(lonMax - lonMin), latStep = nice(latMax - latMin);
    for (var lo = Math.ceil(lonMin / lonStep) * lonStep; lo <= lonMax; lo += lonStep) out += "<line x1='" + X(lo) + "' y1='0' x2='" + X(lo) + "' y2='" + H + "' " + gl + "/><text x='" + (X(lo) + 4) + "' y='" + (H - 6) + "' " + gt + ">" + Math.abs(lo) + (lo < 0 ? "W" : "E") + "</text>";
    for (var la = Math.ceil(latMin / latStep) * latStep; la <= latMax; la += latStep) out += "<line x1='0' y1='" + Y(la) + "' x2='" + W + "' y2='" + Y(la) + "' " + gl + "/><text x='6' y='" + (Y(la) - 4) + "' " + gt + ">" + Math.abs(la) + (la < 0 ? "S" : "N") + "</text>";
    var x1 = X(p[0].lon), y1 = Y(p[0].lat), x2 = X(p[1].lon), y2 = Y(p[1].lat), mx = (x1 + x2) / 2, my = Math.max(24, Math.min(y1, y2) - 46);
    out += "<path d='M" + x1 + " " + y1 + " Q" + mx + " " + my + " " + x2 + " " + y2 + "' fill='none' stroke='#f0b44c' stroke-width='2' stroke-dasharray='7 6'/>";
    out += "<text x='" + mx + "' y='" + (my + 18) + "' text-anchor='middle' fill='#f0b44c' font-size='15' font-family='ui-monospace,monospace'>" + fmt(km) + " km in " + dt.toFixed(1) + " s</text>";
    [[x1, y1, p[0], "first report"], [x2, y2, p[1], "second report"]].forEach(function (d) {
      out += "<circle cx='" + d[0] + "' cy='" + d[1] + "' r='16' fill='rgba(56,212,255,.15)'/><circle cx='" + d[0] + "' cy='" + d[1] + "' r='5.5' fill='#38d4ff'/>";
      out += "<text x='" + d[0] + "' y='" + (d[1] + 30) + "' text-anchor='middle' fill='#e8f4fb' font-size='12' font-family='ui-monospace,monospace'>" + d[3] + "</text>";
      out += "<text x='" + d[0] + "' y='" + (d[1] + 45) + "' text-anchor='middle' fill='#8fb0c8' font-size='11' font-family='ui-monospace,monospace'>" + (d[2].status || "").replace(/_/g, " ") + "</text>";
    });
    svg.innerHTML = out;

    var statusOf = function (q) { return (q.status || "no status").replace(/_/g, " "); };
    var sameStatus = statusOf(p[0]) === statusOf(p[1]);
    var sameLon = Math.abs(p[1].lon - p[0].lon) < 0.01, sameLat = Math.abs(p[1].lat - p[0].lat) < 0.01;
    var hint = sameLon || sameLat ? " Here the " + (sameLon ? "longitude" : "latitude") + " is identical and only the other coordinate changes, which is what a corrupted position report looks like." : "";
    $("casetext").innerHTML =
      "<span class='big'>" + fmt(C.speed_knots) + " kn</span><p style='color:var(--mist)'>the speed the pipeline computed for MMSI " + C.mmsi + "</p>" +
      "<ol><li>Two position reports with the <b>same identifier</b>, " + dt.toFixed(1) + " seconds apart.</li>" +
      "<li>One is at " + coord(p[0]) + ", the other at " + coord(p[1]) + ": " + fmt(km) + " km apart. " + (sameStatus ? "Both say <b>" + statusOf(p[0]) + "</b>." : "One says <b>" + statusOf(p[0]) + "</b>, the other <b>" + statusOf(p[1]) + "</b>.") + "</li>" +
      "<li>Dividing distance by time gives a figure of the same order (the dbt model counts whole seconds, which is why it is not identical). No ship does that.</li></ol>" +
      "<p class='verdict'>Several explanations fit: two transmitters sharing one MMSI, a corrupted position report, or deliberate spoofing." + hint + " The detector cannot tell them apart from the data alone, and a single row like this raises a vessel's risk score: the score is a lead to check, not a verdict.</p>";
  })();

  /* gallery: real screenshots when the file exists, an honest placeholder when not */
  (function gallery() {
    var shots = [
      ["overview", "Overview", "Health tiles, throughput per topic, Spark input vs processing, service availability.", "gallery"],
      ["live-traffic", "Live traffic", "Vessel density and the latest alerts, read straight from Kafka.", "gallery"],
      ["spark", "Spark streaming", "Rates, batch latency against the 30 s trigger, headroom, JVM heap.", "gallery"],
      ["kafka", "Kafka", "Topics and partitions, messages per second, retained messages, partition balance and a topic inventory.", "gallery"],
      ["airflow", "Airflow DAG", "The nine-task hourly run: staging view, models in parallel, tests.", "dag-shot"]
    ];
    shots.forEach(function (s) {
      var fig = el("figure", "shot"), cap = el("figcaption", "", "<b>" + s[1] + "</b> — " + s[2]);
      // The <img> goes into the page right away (hidden until it has loaded) instead of being built detached:
      // a detached image with lazy loading can stay unloaded for ever in some browsers, which left the
      // "pending" frame on screen although the file was there.
      var img = document.createElement("img");
      img.alt = "Screenshot of the " + s[1] + " view"; img.loading = "eager"; img.decoding = "async";
      img.style.display = "none";
      var pending = el("div", "pending", "<b>Screenshot pending</b><span>assets/shots/" + s[0] + ".png</span>");
      img.onload = function () { pending.remove(); img.style.display = ""; };
      img.onerror = function () { pending.innerHTML = "<b>Screenshot not available</b><span>assets/shots/" + s[0] + ".png could not be loaded</span>"; };
      fig.appendChild(pending); fig.appendChild(img); fig.appendChild(cap);
      img.src = "assets/shots/" + s[0] + ".png?v=__VERSION__";
      $(s[3]).appendChild(fig);
    });
  })();

  /* the logo and the name always take you back to the top (the header is sticky, so "#top" on it would do nothing) */
  var brand = document.querySelector(".brand");
  if (brand) brand.addEventListener("click", function (e) {
    e.preventDefault();
    var calm = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    window.scrollTo({ top: 0, behavior: calm ? "auto" : "smooth" });
    if (history.replaceState) history.replaceState(null, "", location.pathname + location.search);
  });

  /* animate bars once they scroll into view -------------------------- */
  var animate = function (root) {
    root.querySelectorAll(".fill").forEach(function (f) { f.style.width = f.dataset.w + "%"; });
    root.querySelectorAll(".bar").forEach(function (b) { b.style.height = b.dataset.h + "%"; });
  };
  if ("IntersectionObserver" in window) {
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) { if (e.isIntersecting) { animate(e.target); io.unobserve(e.target); } });
    }, { threshold: 0.25 });
    document.querySelectorAll(".panel").forEach(function (p) { io.observe(p); });
  } else { animate(document); }
})();
