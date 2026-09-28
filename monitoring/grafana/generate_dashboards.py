#!/usr/bin/env python3
"""Generate the MarineFlow Grafana dashboards as code.

The dashboards are JSON files under dashboards/json/, provisioned into Grafana at startup. Editing
that JSON by hand is painful and error-prone, so it is generated from this file:

    python3 monitoring/grafana/generate_dashboards.py            # write the files
    python3 monitoring/grafana/generate_dashboards.py --check    # fail if the files are out of date

Only the standard library is used. tests/test_grafana_dashboards.py runs --check, so the committed
JSON can never drift from this generator.

What the data sources really expose (measured against a running stack, not assumed):
  * Spark drivers (Prometheus): metrics_local_<appId>_driver_spark_streaming_<query>_<metric>, where
    <metric> is inputRate_total_Value, processingRate_total_Value or latency_Value, plus
    metrics_local_<appId>_driver_jvm_heap_{used,max}_Value. Series carry the labels job, job_role
    (bronze | silver) and entity (positions | metadata). The rate and latency series only exist for
    jobs started with --conf spark.sql.streaming.metricsEnabled=true.
  * kafka-exporter (Prometheus): kafka_brokers, kafka_topic_partitions and
    kafka_topic_partition_{current,oldest}_offset, all labelled by topic. It exposes NO
    kafka_consumergroup_* metrics: Spark's Kafka source does not commit offsets to Kafka, so a
    "consumer lag" panel would stay empty.
  * The Kafka datasource plugin reads messages straight from a topic (live map and alerts table). Its query
    field is topicName (a query with "topic" is silently dropped) and an alias renames every field, so
    none is set. It streams rows, and the geomap markers layer cannot draw a growing frame (blank map),
    so the live map uses the heatmap layer.
"""
import argparse
import json
import pathlib
import sys

OUT_DIR = pathlib.Path(__file__).resolve().parent / "dashboards" / "json"

PROM = {"type": "prometheus", "uid": "prometheus"}
KAFKA = {"type": "hamedkarbasi93-kafka-datasource", "uid": "kafka"}

TOPIC_COLORS = {
    "vessel-positions": "blue",
    "vessel-positions-bronze": "orange",
    "vessel-metadata": "purple",
    "vessel-metadata-bronze": "light-purple",
    "vessel-alerts": "red",
    "dead-letter-queue": "yellow",
}
TRIGGER_MS = 30000          # every streaming job runs on a 30-second trigger


# ---------------------------------------------------------------------------------------------
# PromQL building blocks
# ---------------------------------------------------------------------------------------------
def spark(metric, extra=""):
    """A Spark streaming series, whatever the application id and the query name are."""
    return '{__name__=~"metrics_.*_driver_spark_streaming_.*_%s"%s}' % (metric, extra)


def jvm(metric, extra=""):
    return '{__name__=~"metrics_.*_driver_jvm_heap_%s_Value"%s}' % (metric, extra)


def topic_rate(topic_regex, window="1m"):
    return 'sum by (topic) (rate(kafka_topic_partition_current_offset{topic=~"%s"}[%s]))' % (topic_regex, window)


ALL_TOPICS = "vessel-.*|dead-letter-queue"
# Kafka's own internal topics (__consumer_offsets, __transaction_state) are not part of the
# pipeline. __consumer_offsets in particular carries 50 partitions by default, so leaving it
# unfiltered inflates "total partitions" panels and shows up as a stray row everywhere else —
# something in the stack (the Grafana Kafka plugin, a console consumer run with --group) is
# enough to make the broker create it.
NOT_INTERNAL = 'topic!~"__.*"' 


# ---------------------------------------------------------------------------------------------
# Panel builders
# ---------------------------------------------------------------------------------------------
def thresholds(base, *steps):
    return {"mode": "absolute",
            "steps": [{"color": base, "value": None}] + [{"color": c, "value": v} for v, c in steps]}


def target(expr, legend="", ref="A", instant=False, ds=PROM, fmt=None):
    t = {"refId": ref, "datasource": ds, "expr": expr, "legendFormat": legend, "editorMode": "code",
         "instant": instant, "range": not instant}
    if fmt:
        t["format"] = fmt
    return t


def grid(x, y, w, h):
    return {"x": x, "y": y, "w": w, "h": h}


def text(title, content, x, y, w, h, transparent=True):
    return {"type": "text", "title": title, "transparent": transparent, "gridPos": grid(x, y, w, h),
            "options": {"mode": "markdown", "content": content}}


def stat(title, expr, x, y, w=4, h=4, unit="short", th=None, decimals=None, description="", mappings=None,
         suffix="", spark_line=True, color_mode="background", min_=None, max_=None, no_value="0"):
    defaults = {"unit": unit, "color": {"mode": "thresholds"}, "thresholds": th or thresholds("green"),
                "mappings": mappings or [], "noValue": no_value}
    if decimals is not None:
        defaults["decimals"] = decimals
    if suffix:
        defaults["unit"] = "suffix:" + suffix
    if min_ is not None:
        defaults["min"] = min_
    if max_ is not None:
        defaults["max"] = max_
    return {"type": "stat", "title": title, "description": description, "datasource": PROM,
            "gridPos": grid(x, y, w, h), "targets": [target(expr)],
            "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": {"colorMode": color_mode, "graphMode": "area" if spark_line else "none",
                        "justifyMode": "center", "textMode": "value", "wideLayout": True,
                        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "orientation": "auto"}}


def timeseries(title, targets, x, y, w, h, unit="short", description="", fill=22, stack=False, overrides=None,
               th=None, th_style=None, min_=None, max_=None, legend_calcs=None, ds=PROM, line_width=2):
    custom = {"drawStyle": "line", "lineInterpolation": "smooth", "lineWidth": line_width, "fillOpacity": fill,
              "gradientMode": "opacity", "showPoints": "never", "spanNulls": True, "axisPlacement": "auto",
              "stacking": {"mode": "normal" if stack else "none", "group": "A"},
              "thresholdsStyle": {"mode": th_style or "off"}}
    defaults = {"unit": unit, "custom": custom, "color": {"mode": "palette-classic"}, "mappings": [],
                "thresholds": th or thresholds("green")}
    if min_ is not None:
        defaults["min"] = min_
    if max_ is not None:
        defaults["max"] = max_
    return {"type": "timeseries", "title": title, "description": description, "datasource": ds,
            "gridPos": grid(x, y, w, h), "targets": targets,
            "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
            "options": {"legend": {"displayMode": "table" if legend_calcs else "list", "placement": "bottom",
                                   "showLegend": True, "calcs": legend_calcs or []},
                        "tooltip": {"mode": "multi", "sort": "desc"}}}


def bargauge(title, expr, x, y, w, h, legend="{{topic}}", unit="short", description="", th=None, overrides=None,
             min_=0, max_=None, instant=True):
    defaults = {"unit": unit, "min": min_, "color": {"mode": "thresholds"}, "thresholds": th or thresholds("blue"),
                "mappings": [], "noValue": "0"}
    if max_ is not None:
        defaults["max"] = max_
    return {"type": "bargauge", "title": title, "description": description, "datasource": PROM,
            "gridPos": grid(x, y, w, h), "targets": [target(expr, legend, instant=instant)],
            "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
            "options": {"displayMode": "gradient", "orientation": "horizontal", "showUnfilled": True,
                        "valueMode": "color", "namePlacement": "auto", "sizing": "auto", "minVizHeight": 16,
                        "minVizWidth": 8, "maxVizHeight": 300,
                        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}}}


def gauge(title, expr, x, y, w, h, legend, unit="percentunit", description="", th=None, min_=0, max_=1):
    return {"type": "gauge", "title": title, "description": description, "datasource": PROM,
            "gridPos": grid(x, y, w, h), "targets": [target(expr, legend, instant=True)],
            "fieldConfig": {"defaults": {"unit": unit, "min": min_, "max": max_, "color": {"mode": "thresholds"},
                                         "thresholds": th or thresholds("green"), "mappings": []},
                            "overrides": []},
            "options": {"showThresholdLabels": False, "showThresholdMarkers": True, "orientation": "auto",
                        "sizing": "auto", "minVizHeight": 75, "minVizWidth": 75,
                        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}}}


def state_timeline(title, expr, x, y, w, h, legend, description=""):
    up_down = [{"type": "value", "options": {"0": {"text": "DOWN", "color": "red", "index": 0},
                                              "1": {"text": "UP", "color": "green", "index": 1}}}]
    return {"type": "state-timeline", "title": title, "description": description, "datasource": PROM,
            "gridPos": grid(x, y, w, h), "targets": [target(expr, legend)],
            "fieldConfig": {"defaults": {"mappings": up_down, "color": {"mode": "thresholds"},
                                         "thresholds": thresholds("red", (1, "green")),
                                         "custom": {"fillOpacity": 85, "lineWidth": 0}}, "overrides": []},
            "options": {"mergeValues": True, "showValue": "never", "alignValue": "center", "rowHeight": 0.85,
                        "legend": {"showLegend": False, "displayMode": "list", "placement": "bottom"},
                        "tooltip": {"mode": "single", "sort": "none"}}}


def topic_overrides():
    return [{"matcher": {"id": "byName", "options": topic},
             "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": color}}]}
            for topic, color in TOPIC_COLORS.items()]


# ---------------------------------------------------------------------------------------------
# Dashboard container
# ---------------------------------------------------------------------------------------------
NAV = [{"asDropdown": False, "icon": "external link", "includeVars": False, "keepTime": True,
        "tags": ["marineflow"], "targetBlank": False, "title": "MarineFlow dashboards", "tooltip": "",
        "type": "dashboards", "url": ""}]


class Dashboard:
    def __init__(self, uid, title, description, refresh="10s", time_from="now-30m", templating=None):
        self.data = {"uid": uid, "title": title, "description": description, "tags": ["marineflow"],
                     "timezone": "browser", "editable": True, "graphTooltip": 1, "schemaVersion": 39, "version": 1,
                     "refresh": refresh, "time": {"from": time_from, "to": "now"}, "links": NAV,
                     "templating": {"list": templating or []}, "annotations": {"list": []}, "panels": []}
        self._id = 0

    def add(self, panel):
        self._id += 1
        panel["id"] = self._id
        panel.setdefault("datasource", PROM)
        self.data["panels"].append(panel)
        return self

    def render(self):
        return json.dumps(self.data, indent=2, ensure_ascii=False) + "\n"


# ---------------------------------------------------------------------------------------------
# 1. Overview
# ---------------------------------------------------------------------------------------------
def overview():
    d = Dashboard("marineflow-overview", "MarineFlow: Overview",
                  "Health of the whole pipeline at a glance: ingestion, jobs, alerts and rejections.")
    d.add(text("", "## MarineFlow · pipeline health\n"
                   "Live AIS from aisstream.io flows through Kafka and Spark. The **hot path** raises alerts within "
                   "seconds; the **cold path** lands in GCS and BigQuery. Metrics come from the Spark drivers and "
                   "kafka-exporter.", 0, 0, 24, 3))

    up_map = [{"type": "value", "options": {"1": {"text": "UP", "color": "green", "index": 0}}},
              {"type": "special", "options": {"match": "null+nan", "result": {"text": "DOWN", "color": "red", "index": 1}}}]
    d.add(stat("Kafka broker", "kafka_brokers", 0, 3, th=thresholds("red", (1, "green")), mappings=up_map,
               spark_line=False, description="Brokers reported by kafka-exporter."))
    d.add(stat("Spark jobs up", 'count(up{job=~"spark-.*"} == 1)', 4, 3, suffix=" / 4",
               th=thresholds("red", (1, "orange"), (4, "green")), spark_line=False,
               description="The four GCS-writing jobs that Prometheus scrapes. Hot Alerts has no scrape target."))
    d.add(stat("Positions / s", topic_rate("vessel-positions").replace("sum by (topic) ", "sum "), 8, 3,
               th=thresholds("blue"), decimals=1,
               description="Position messages per second entering Kafka."))
    # Divide two 15-minute increases, not two rates: Bronze trails the raw topic by a batch or two, so short
    # windows swing wildly. "> 0" drops the series while nothing arrives (0/0 used to render as -Inf%), and
    # clamp_min stops a Bronze catch-up burst from showing a negative share.
    #
    # Renamed from "Bronze rejects": this is the gap between the raw and Bronze write rates, and in practice
    # Bronze's own validate() almost never rejects a record (aisstream.io's coordinates are clean — a sample
    # of 3000 raw messages had zero nulls, zero out-of-range, zero AIS "not available" sentinels). A sustained
    # high value here means Bronze fell behind or crashed — check "Spark jobs up" and the container's
    # OOMKilled flag (`docker inspect <container> --format '{{.State.OOMKilled}}'`) before assuming bad data.
    d.add(stat("Bronze gap",
               'clamp_min(1 - sum(increase(kafka_topic_partition_current_offset{topic="vessel-positions-bronze"}[15m])) '
               '/ (sum(increase(kafka_topic_partition_current_offset{topic="vessel-positions"}[15m])) > 0), 0)',
               12, 3, unit="percentunit", th=thresholds("green", (0.02, "orange"), (0.1, "red")), decimals=1,
               no_value="no traffic", spark_line=False,
               description="How much Bronze's write rate trails the raw topic's, over the last 15 minutes. Almost "
                           "always a lag or crash symptom, not data quality: validate() only drops null/out-of-range "
                           "coordinates and the AIS 'not available' sentinels (91/181), which real feeds rarely send. "
                           "Check 'Spark jobs up' first. Shows 'no traffic' while the producer is not sending."))
    d.add(stat("Alerts (1 h)",
               'sum(increase(kafka_topic_partition_current_offset{topic="vessel-alerts"}[1h]))', 16, 3,
               th=thresholds("green", (1, "orange"), (50, "red")), decimals=0, spark_line=False,
               description="Alerts written by Hot Alerts to vessel-alerts in the last hour."))
    d.add(stat("Dead letters (1 h)",
               'sum(increase(kafka_topic_partition_current_offset{topic="dead-letter-queue"}[1h]))', 20, 3,
               th=thresholds("green", (1, "red")), decimals=0, spark_line=False,
               description="Messages the producer could not publish. Anything above zero deserves a look."))

    d.add(timeseries("Ingest throughput by topic", [target(topic_rate(ALL_TOPICS), "{{topic}}")], 0, 7, 12, 9,
                     unit="short", description="Messages per second written to each Kafka topic.",
                     overrides=topic_overrides()))
    d.add(timeseries("Spark: input vs processing rate",
                     [target("sum(%s)" % spark("inputRate_total_Value"), "Input", "A"),
                      target("sum(%s)" % spark("processingRate_total_Value"), "Processing", "B")],
                     12, 7, 12, 9, unit="short", fill=18, legend_calcs=["mean", "max"],
                     overrides=[{"matcher": {"id": "byName", "options": "Input"},
                                 "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "blue"}}]},
                                {"matcher": {"id": "byName", "options": "Processing"},
                                 "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "purple"}}]}],
                     description="Rows per second read and processed by all Spark jobs together. Processing at or "
                     "above input means the jobs keep up; the per-job view is in the Spark dashboard."))

    d.add(bargauge("Messages retained per topic",
                   'sum by (topic) (kafka_topic_partition_current_offset{%s} - kafka_topic_partition_oldest_offset{%s})'
                   % (NOT_INTERNAL, NOT_INTERNAL),
                   0, 16, 8, 8, description="Messages currently stored in each topic.", overrides=topic_overrides(),
                   th=thresholds("blue")))
    d.add(state_timeline("Service availability", 'up{job=~"spark-.*|kafka-exporter"}', 8, 16, 8, 8, "{{job}}",
                         description="Prometheus scrape status of each target."))
    d.add(gauge("Spark JVM heap used",
                "sum by (job) (%s) / sum by (job) (%s)" % (jvm("used"), jvm("max")), 16, 16, 8, 8, "{{job}}",
                description="Heap used as a share of the maximum, per driver.",
                th=thresholds("green", (0.7, "orange"), (0.9, "red"))))
    return d


# ---------------------------------------------------------------------------------------------
# 2. Live traffic (reads Kafka directly)
# ---------------------------------------------------------------------------------------------
def kafka_target(topic, last_n, ref="A"):
    return {"refId": ref, "datasource": KAFKA, "topicName": topic, "partition": "all", "autoOffsetReset": "lastN",
            "lastN": last_n, "messageFormat": "json", "timestampMode": "message"}


def live():
    d = Dashboard("marineflow-live", "MarineFlow: Live traffic",
                  "Vessel positions and alerts, read straight from Kafka.", refresh="", time_from="now-15m")
    d.add(stat("Positions / s", topic_rate("vessel-positions").replace("sum by (topic) ", "sum "), 0, 0, w=6, h=4,
               th=thresholds("blue"), decimals=1))
    d.add(stat("Alerts (1 h)", 'sum(increase(kafka_topic_partition_current_offset{topic="vessel-alerts"}[1h]))',
               6, 0, w=6, h=4, th=thresholds("green", (1, "orange"), (50, "red")), decimals=0, spark_line=False))
    d.add(stat("Retained positions", 'sum(kafka_topic_partition_current_offset{topic="vessel-positions-bronze"} '
                                     '- kafka_topic_partition_oldest_offset{topic="vessel-positions-bronze"})',
               12, 0, w=6, h=4, th=thresholds("purple"), decimals=0, spark_line=False,
               description="Bronze positions still stored in Kafka."))
    d.add(stat("Dead letters (1 h)",
               'sum(increase(kafka_topic_partition_current_offset{topic="dead-letter-queue"}[1h]))', 18, 0, w=6, h=4,
               th=thresholds("green", (1, "red")), decimals=0, spark_line=False))

    # A heatmap layer, not a markers layer: the Kafka datasource streams rows into the panel, and the markers
    # layer draws with WebGL buffers that break as rows are appended (the whole map goes blank). The heatmap
    # layer copes with streaming data and shows where traffic is dense along the shipping lanes.
    d.add({"type": "geomap", "title": "Traffic density (latest Bronze positions)", "datasource": KAFKA,
           "description": "The last positions read from vessel-positions-bronze. Hot spots are the busiest shipping lanes.",
           "gridPos": grid(0, 4, 24, 15), "targets": [kafka_target("vessel-positions-bronze", 500)],
           "fieldConfig": {"defaults": {"color": {"mode": "thresholds"}, "thresholds": thresholds("green"), "mappings": []},
                           "overrides": []},
           "options": {"view": {"id": "zero", "lat": 25, "lon": 15, "zoom": 2, "allLayers": True},
                       # The wheel scrolls the page; zoom with the +/- buttons or by dragging. A wheel-zooming map traps the
                       # scroll whenever the pointer crosses it.
                       "controls": {"showZoom": True, "mouseWheelZoom": False, "showAttribution": True,
                                    "showScale": False, "showMeasure": False, "showDebug": False},
                       "basemap": {"type": "carto", "name": "Basemap", "config": {"theme": "dark", "showLabels": True}},
                       "tooltip": {"mode": "details"},
                       "layers": [{"type": "heatmap", "name": "Traffic density", "tooltip": True,
                                   "location": {"mode": "coords", "latitude": "Latitude", "longitude": "Longitude"},
                                   "config": {"blur": 12, "radius": 5, "weight": {"fixed": 1, "min": 0, "max": 1}}}]}})

    severity = [{"type": "value", "options": {"high": {"color": "red", "index": 0}, "medium": {"color": "orange", "index": 1}}}]
    kinds = [{"type": "value", "options": {"GPS_SPOOFING": {"color": "dark-red", "index": 0},
                                            "IMPOSSIBLE_SPEED": {"color": "red", "index": 1},
                                            "SUDDEN_ACCELERATION": {"color": "orange", "index": 2},
                                            "AIS_GAP": {"color": "yellow", "index": 3}}}]
    d.add({"type": "table", "title": "Latest alerts", "datasource": KAFKA,
           "description": "The 8 newest alerts raised by the hot path.",
           "gridPos": grid(0, 19, 16, 10), "targets": [kafka_target("vessel-alerts", 50)],
           "transformations": [
               {"id": "organize", "options": {"excludeByName": {"Time": True, "topic": True, "partition": True, "offset": True,
                                                                "key": True, "alias": True},
                                              "renameByName": {"detected_at": "Detected", "alert_type": "Alert", "severity": "Severity",
                                                               "mmsi": "MMSI", "description": "Detail", "latitude": "Lat", "longitude": "Lon"},
                                              "indexByName": {"detected_at": 0, "alert_type": 1, "severity": 2, "mmsi": 3, "description": 4}}},
               {"id": "sortBy", "options": {"sort": [{"field": "Detected", "desc": True}]}},
               # Only what fits the panel: a taller table gets its own scrollbar, which competes with the page's.
               {"id": "limit", "options": {"limitField": 8}}],
           "fieldConfig": {"defaults": {"custom": {"align": "auto", "cellOptions": {"type": "auto"}, "inspect": False}, "mappings": [],
                                        "thresholds": thresholds("green")},
                           "overrides": [
                               {"matcher": {"id": "byName", "options": "Severity"}, "properties": [
                                   {"id": "custom.cellOptions", "value": {"type": "color-background"}}, {"id": "mappings", "value": severity}]},
                               {"matcher": {"id": "byName", "options": "Alert"}, "properties": [
                                   {"id": "custom.cellOptions", "value": {"type": "color-text"}}, {"id": "mappings", "value": kinds}]}]},
           "options": {"showHeader": True, "cellHeight": "sm", "footer": {"show": False}}})

    d.add({"type": "bargauge", "title": "Alerts by type (last 50)", "datasource": KAFKA,
           "description": "How the most recent alerts split by rule.",
           "gridPos": grid(16, 19, 8, 10), "targets": [kafka_target("vessel-alerts", 50)],
           "transformations": [{"id": "groupBy", "options": {"fields": {"alert_type": {"aggregations": [], "operation": "groupby"},
                                                                          "mmsi": {"aggregations": ["count"], "operation": "aggregate"}}}}],
           "fieldConfig": {"defaults": {"min": 0, "color": {"mode": "continuous-YlRd"}, "thresholds": thresholds("blue"), "mappings": []},
                           "overrides": []},
           "options": {"displayMode": "gradient", "orientation": "horizontal", "showUnfilled": True, "valueMode": "color",
                       "namePlacement": "auto", "sizing": "auto", "minVizHeight": 16, "minVizWidth": 8,
                       "reduceOptions": {"calcs": ["lastNotNull"], "fields": "/^mmsi \\(count\\)$/", "values": True}}})
    return d


# ---------------------------------------------------------------------------------------------
# 3. Spark streaming
# ---------------------------------------------------------------------------------------------
def spark_dashboard():
    job_var = {"name": "job", "label": "Job", "type": "query", "datasource": PROM,
               "definition": 'label_values(up{job=~"spark-.*"}, job)',
               "query": {"query": 'label_values(up{job=~"spark-.*"}, job)', "refId": "job"}, "refresh": 2,
               "includeAll": True, "multi": True, "allValue": "spark-.*", "sort": 1,
               "current": {"selected": True, "text": ["All"], "value": ["$__all"]}}
    d = Dashboard("marineflow-spark", "MarineFlow: Spark streaming",
                  "Throughput, latency and memory of the Spark Structured Streaming jobs.", templating=[job_var])
    j = ', job=~"$job"'
    d.add(text("", "## Spark Structured Streaming\nEvery job runs on a **30-second trigger**. Rate and latency series "
                   "require `--conf spark.sql.streaming.metricsEnabled=true`; without it these panels stay empty.",
               0, 0, 24, 3))

    d.add(stat("Input rate", "sum(%s)" % spark("inputRate_total_Value", j), 0, 3, w=6, suffix=" rows/s",
               th=thresholds("blue"), decimals=1, description="Rows per second read, summed over the selected jobs."))
    d.add(stat("Processing rate", "sum(%s)" % spark("processingRate_total_Value", j), 6, 3, w=6, suffix=" rows/s",
               th=thresholds("purple"), decimals=1, description="Rows per second processed."))
    d.add(stat("Batch latency", "max(%s)" % spark("latency_Value", j), 12, 3, w=6, unit="ms",
               th=thresholds("green", (18000, "orange"), (TRIGGER_MS, "red")), decimals=0,
               description="Slowest micro-batch of the selected jobs. Above the 30 s trigger a job falls behind."))
    d.add(stat("Jobs up", 'count(up{job=~"spark-.*"%s} == 1)' % j, 18, 3, w=6, suffix=" jobs",
               th=thresholds("red", (1, "green")), spark_line=False))

    d.add(timeseries("Input rate", [target("sum by (job) (%s)" % spark("inputRate_total_Value", j), "{{job}}")],
                     0, 7, 12, 8, description="Rows per second read from Kafka."))
    d.add(timeseries("Processing rate", [target("sum by (job) (%s)" % spark("processingRate_total_Value", j), "{{job}}")],
                     12, 7, 12, 8, description="Rows per second processed."))
    d.add(timeseries("Batch latency", [target("max by (job) (%s)" % spark("latency_Value", j), "{{job}}")],
                     0, 15, 12, 8, unit="ms", th=thresholds("green", (18000, "orange"), (TRIGGER_MS, "red")),
                     th_style="dashed", min_=0,
                     description="Duration of the latest micro-batch. The dashed lines mark 60% and 100% of the 30 s trigger."))
    d.add(timeseries("Headroom (processing / input)",
                     [target("sum by (job) (%s) / (sum by (job) (%s) > 0)" % (spark("processingRate_total_Value", j),
                                                                            spark("inputRate_total_Value", j)), "{{job}}")],
                     12, 15, 12, 8, unit="suffix:x", th=thresholds("red", (1, "orange"), (2, "green")), th_style="dashed",
                     description="How many times faster than the input the job could go. Below 1x it is falling behind; 2x or more is comfortable."))
    d.add(timeseries("JVM heap used",
                     [target("sum by (job) (%s)" % jvm("used", j), "{{job}} used", "A"),
                      target("sum by (job) (%s)" % jvm("max", j), "{{job}} max", "B")],
                     0, 23, 24, 8, unit="bytes", fill=10, description="Heap used and its maximum per driver."))
    return d


# ---------------------------------------------------------------------------------------------
# 4. Kafka
# ---------------------------------------------------------------------------------------------
def kafka_dashboard():
    d = Dashboard("marineflow-kafka", "MarineFlow: Kafka",
                  "Topics, partitions and throughput of the Kafka broker.")
    d.add(text("", "## Kafka topics\nBronze and Hot Alerts read the same `*-bronze` topic independently. Spark does not "
                   "commit its offsets to Kafka, so consumer-group lag is not available; compare the raw and bronze "
                   "topics instead.", 0, 0, 24, 3))
    d.add(stat("Brokers", "kafka_brokers", 0, 3, w=6, th=thresholds("red", (1, "green")), spark_line=False))
    d.add(stat("Topics", 'count(count by (topic) (kafka_topic_partitions{%s}))' % NOT_INTERNAL, 6, 3, w=6, th=thresholds("blue"),
               spark_line=False))
    d.add(stat("Partitions", 'sum(kafka_topic_partitions{%s})' % NOT_INTERNAL, 12, 3, w=6, th=thresholds("blue"), spark_line=False))
    d.add(stat("Under-replicated", 'sum(kafka_topic_partition_under_replicated_partition{%s})' % NOT_INTERNAL, 18, 3, w=6,
               th=thresholds("green", (1, "red")), spark_line=False))

    d.add(timeseries("Messages per second", [target(topic_rate(ALL_TOPICS), "{{topic}}")], 0, 7, 12, 9,
                     overrides=topic_overrides(),
                     description="Write rate of every topic, over one minute."))
    d.add(timeseries("Total messages written",
                     [target('sum by (topic) (kafka_topic_partition_current_offset{topic=~"%s"})' % ALL_TOPICS, "{{topic}}")],
                     12, 7, 12, 9, overrides=topic_overrides(), fill=8,
                     description="Cumulative offset per topic since it was created."))
    d.add(bargauge("Messages retained per topic",
                   'sum by (topic) (kafka_topic_partition_current_offset{%s} - kafka_topic_partition_oldest_offset{%s})'
                   % (NOT_INTERNAL, NOT_INTERNAL),
                   0, 16, 12, 8, overrides=topic_overrides()))
    d.add(bargauge("Balance across partitions (vessel-positions)",
                   'kafka_topic_partition_current_offset{topic="vessel-positions"}', 12, 16, 12, 8,
                   legend="partition {{partition}}", th=thresholds("light-blue"),
                   description="Messages are keyed by MMSI, so a vessel always lands in the same partition."))

    d.add({"type": "table", "title": "Topic inventory", "datasource": PROM, "gridPos": grid(0, 24, 24, 9),
           "targets": [target('max by (topic) (kafka_topic_partitions{%s})' % NOT_INTERNAL, "", "A", instant=True, fmt="table"),
                       target('sum by (topic) (kafka_topic_partition_current_offset{%s} - kafka_topic_partition_oldest_offset{%s})'
                              % (NOT_INTERNAL, NOT_INTERNAL), "", "B", instant=True, fmt="table"),
                       target(topic_rate(ALL_TOPICS, "5m") + " * 60", "", "C", instant=True, fmt="table")],
           "transformations": [
               {"id": "merge", "options": {}},
               {"id": "organize", "options": {"excludeByName": {"Time": True},
                                              "renameByName": {"topic": "Topic", "Value #A": "Partitions",
                                                               "Value #B": "Messages retained", "Value #C": "Messages / min"},
                                              "indexByName": {"topic": 0, "Value #A": 1, "Value #B": 2, "Value #C": 3}}},
               {"id": "sortBy", "options": {"sort": [{"field": "Messages retained", "desc": True}]}}],
           "fieldConfig": {"defaults": {"custom": {"align": "auto", "cellOptions": {"type": "auto"}}, "decimals": 0,
                                        "thresholds": thresholds("green"), "mappings": []},
                           "overrides": [{"matcher": {"id": "byName", "options": "Messages retained"},
                                          "properties": [{"id": "custom.cellOptions", "value": {"type": "gauge", "mode": "gradient"}},
                                                         {"id": "color", "value": {"mode": "continuous-BlPu"}}]},
                                         {"matcher": {"id": "byName", "options": "Messages / min"},
                                          "properties": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                                                         {"id": "color", "value": {"mode": "fixed", "fixedColor": "light-blue"}}]}]},
           "options": {"showHeader": True, "cellHeight": "sm", "footer": {"show": False}}})
    return d


DASHBOARDS = {"marineflow-overview.json": overview, "marineflow-live.json": live,
              "marineflow-spark.json": spark_dashboard, "marineflow-kafka.json": kafka_dashboard}


def render_all():
    return {name: build().render() for name, build in DASHBOARDS.items()}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--check", action="store_true", help="exit 1 if the committed files differ from the generator")
    args = parser.parse_args(argv)
    rendered = render_all()
    if args.check:
        stale = [n for n, text_ in rendered.items() if not (OUT_DIR / n).exists() or (OUT_DIR / n).read_text() != text_]
        stale += [p.name for p in OUT_DIR.glob("*.json") if p.name not in rendered]
        if stale:
            print("out of date (run generate_dashboards.py): " + ", ".join(sorted(stale)))
            return 1
        print(f"{len(rendered)} dashboards up to date")
        return 0
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, content in rendered.items():
        (OUT_DIR / name).write_text(content)
        print(f"wrote {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
