# =============================================================================
# MARINEFLOW — Spark image (Bronze + Silver Positions + Silver Metadata)
# processing/Dockerfile
#
# Bakes Python deps into the image at build time instead of pip-installing
# them every time a container starts (or worse, by hand after `docker exec`).
# Shared by all three spark-* services in docker-compose.yml.
# =============================================================================
FROM apache/spark:3.5.0

USER root

COPY spark_streaming/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt
