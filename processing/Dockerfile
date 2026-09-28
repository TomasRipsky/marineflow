# =============================================================================
# MARINEFLOW — Spark image
# processing/Dockerfile
#
# Bakes Python deps into the image at build time instead of pip-installing
# them every time a container starts (or worse, by hand after `docker exec`).
# Shared by all five spark-* services in docker-compose.yml (Bronze and Silver
# for positions and metadata, plus Hot Alerts).
# The base image runs Python 3.8, which caps the pandas/numpy versions pinned in
# spark_streaming/requirements.txt.
# =============================================================================
FROM apache/spark:3.5.0

USER root

COPY spark_streaming/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt
