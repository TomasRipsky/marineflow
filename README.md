# MarineFlow 🚢
### Real-Time Global Maritime Intelligence Platform

> *"Designed a system that takes the pulse of global shipping in real time."*

MarineFlow es un pipeline de data engineering end-to-end que procesa señales AIS (Automatic Identification System) de barcos en tiempo real. Ingiere, transforma, enriquece y sirve datos de posicionamiento naval a través de una arquitectura lakehouse moderna sobre GCP.

---

## Tabla de Contenidos

1. [¿Qué es AIS y por qué es interesante?](#qué-es-ais-y-por-qué-es-interesante)
2. [Arquitectura General](#arquitectura-general)
3. [Stack Tecnológico](#stack-tecnológico)
4. [Estructura del Proyecto](#estructura-del-proyecto)
5. [Fases del Pipeline](#fases-del-pipeline)
   - [Fase 1 — Ingesta AIS](#fase-1--ingesta-ais)
   - [Fase 2 — Spark Bronze Layer](#fase-2--spark-bronze-layer)
   - [Fase 3 — Silver, Gold y dbt](#fase-3--silver-gold-y-dbt) *(pendiente)*
   - [Fase 4 — Modelos ML](#fase-4--modelos-ml) *(pendiente)*
   - [Fase 5 — API y Dashboard](#fase-5--api-y-dashboard) *(pendiente)*
6. [Infraestructura GCP (Terraform)](#infraestructura-gcp-terraform)
7. [Configuración Local](#configuración-local)
8. [Decisiones de Diseño](#decisiones-de-diseño)
9. [Limitaciones Conocidas](#limitaciones-conocidas)
10. [Cómo Ejecutar](#cómo-ejecutar)

---

## ¿Qué es AIS y por qué es interesante?

AIS (Automatic Identification System) es un protocolo de radio obligatorio para todos los barcos comerciales de más de 300 toneladas. Cada embarcación emite su posición GPS, velocidad, rumbo, destino y datos de identidad cada 2-10 segundos.

Esto genera un stream global continuo de cientos de miles de mensajes por minuto — exactamente el tipo de dato que un pipeline de streaming moderno está diseñado para manejar.

**¿Por qué es relevante para data engineering?**

- **Volumen real**: ~300 mensajes/segundo con cobertura global
- **Variedad**: posiciones, metadatos estáticos, alertas, estado de navegación
- **Velocidad**: latencia de segundos desde el barco hasta el sistema
- **Casos de uso reales**: logística (Amazon, Maersk), energía (seguimiento de petroleros), seguridad (detección de barcos oscuros)

---

## Arquitectura General

```
┌─────────────────────────────────────────────────────────────────────┐
│                         INGESTA                                      │
│                                                                      │
│  aisstream.io WebSocket  ──►  AIS Producer (Python)  ──►  Pub/Sub  │
│  (señales AIS globales)        ingestion/ais_producer               │
│                                                                      │
│  Simulator (fallback)    ──►  8 rutas sintéticas     ──►  Pub/Sub  │
│  (desarrollo/testing)          ingestion/simulator                  │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                    vessel-positions topic
                    vessel-metadata topic
                              │
┌─────────────────────────────▼───────────────────────────────────────┐
│                      PROCESAMIENTO (Spark)                           │
│                                                                      │
│  Bronze Job  ──►  Pull Pub/Sub  ──►  Validación mínima  ──►  GCS  │
│  (micro-batch)     Python client     sin transformación    Parquet  │
│                                                                      │
│  Silver Job  ──►  Lee Bronze  ──►  Limpieza + Enriquecimiento      │
│                                    geoespacial + deltas  ──►  GCS  │
│                                                           + BigQuery│
└─────────────────────────────┬───────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────┐
│                       GOLD + ANALYTICS (dbt)                         │
│                                                                      │
│  dbt models  ──►  agregaciones  ──►  BigQuery Gold dataset          │
│  Airflow     ──►  orquestación diaria                               │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────┐
│                          ML + SERVING                                │
│                                                                      │
│  MLflow  ──►  Activity Classifier (XGBoost)                        │
│          ──►  Anomaly Detector (Isolation Forest)                   │
│          ──►  ETA Predictor (LSTM)                                  │
│                                                                      │
│  FastAPI + Redis  ──►  Cloud Run  ──►  Dashboard WebSocket         │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Stack Tecnológico

| Capa | Tecnología | Por qué |
|------|-----------|---------|
| **Mensajería** | Google Cloud Pub/Sub | Managed, sin infraestructura que mantener, integración nativa con GCP |
| **Procesamiento** | Apache Spark 3.5 (local Docker) | Standard de la industria para batch y streaming, esencial en portfolios DE |
| **Lakehouse** | Delta Lake + GCS | Formato abierto, ACID transactions, time travel |
| **Transformaciones** | dbt | SQL versionado, lineage automático, tests de datos |
| **Orquestación** | Apache Airflow | Standard de facto para pipelines DE |
| **ML Tracking** | MLflow | Experimentos reproducibles, model registry |
| **Serving** | FastAPI + Redis + Cloud Run | API moderna, caché, serverless |
| **Storage** | GCS (Bronze/Silver/Gold) + BigQuery | Separación lakehouse clásica |
| **IaC** | Terraform | Infraestructura reproducible y versionada |
| **Monitorización** | Prometheus + Grafana | Métricas operacionales del pipeline |
| **Contenedores** | Docker Compose | Entorno local reproducible |

---

## Estructura del Proyecto

```
marineflow/
├── ingestion/
│   ├── ais_producer/           # Producer WebSocket → Pub/Sub
│   │   ├── main.py             # Entrada, retry logic, graceful shutdown
│   │   ├── producer.py         # PubSubPublisher con batching y DLQ
│   │   ├── parser.py           # Parser AIS, modelos Pydantic
│   │   ├── config.py           # Configuración desde variables de entorno
│   │   └── requirements.txt
│   └── simulator/              # Generador sintético de flota
│       ├── main.py             # 8 rutas reales, anomalías, interpolación GPS
│       └── requirements.txt
│
├── processing/
│   ├── spark_streaming/
│   │   ├── bronze_positions.py # Pub/Sub → GCS Parquet (Bronze)
│   │   ├── silver_positions.py # Bronze → GCS + BigQuery (Silver)
│   │   └── requirements.txt
│   └── jars/
│       ├── gcs-connector-hadoop3-latest.jar   # GCS filesystem para Spark
│       └── spark-3.5-bigquery-0.36.1.jar      # BigQuery connector
│
├── infra/
│   └── terraform/
│       ├── main.tf             # Provider GCP con ADC
│       ├── variables.tf
│       ├── outputs.tf
│       ├── terraform.tfvars.example
│       └── modules/
│           ├── gcs/            # Buckets Bronze/Silver/Gold
│           ├── pubsub/         # Topics y subscriptions
│           ├── bigquery/       # Datasets y tablas
│           └── iam/            # Service account y roles
│
├── monitoring/
│   └── prometheus/
│       └── prometheus.yml
│
├── docker-compose.yml          # Stack local completo
├── .env                        # Variables de entorno (gitignored)
└── README.md
```

---

## Fases del Pipeline

### Fase 1 — Ingesta AIS

**Estado: ✅ Completada**

#### Componentes

**`ingestion/ais_producer/`** — Producer real

Conecta vía WebSocket a `wss://stream.aisstream.io/v0/stream`, parsea mensajes AIS y los publica en Pub/Sub.

- `main.py`: punto de entrada con retry exponencial usando `tenacity`. Gestiona señales de sistema (SIGTERM/SIGINT) para shutdown graceful.
- `producer.py`: `PubSubPublisher` con batching (hasta 100 mensajes por batch), callbacks de confirmación y Dead Letter Queue (DLQ) para mensajes fallidos.
- `parser.py`: parsea dos tipos de mensajes AIS — `PositionReport` (posición GPS, velocidad, rumbo) y `ShipStaticData` (nombre, tipo, bandera, destino). Usa modelos Pydantic para validación.
- `config.py`: toda la configuración viene de variables de entorno. No hay valores hardcodeados en código.

**`ingestion/simulator/`** — Simulador sintético

Genera una flota sintética realista para desarrollo y testing sin depender del servicio externo.

- 8 rutas de shipping reales (Trans-Atlántico, Asia-Europa Suez, Trans-Pacífico, etc.)
- Interpolación lineal entre waypoints con ruido GPS simulado
- 5% de barcos anómalos (velocidades imposibles, gaps de AIS) para entrenar el modelo de detección de anomalías
- Configurable: `python main.py --vessels 20 --interval 3.0`

#### Pub/Sub Topics

| Topic | Contenido | Subscription de Spark |
|-------|-----------|----------------------|
| `vessel-positions` | PositionReport (lat/lon/velocidad/rumbo) | `vessel-positions-spark-sub` |
| `vessel-metadata` | ShipStaticData (nombre/tipo/bandera) | `vessel-metadata-spark-sub` |
| `maritime-alerts` | Alertas del detector de anomalías | `maritime-alerts-spark-sub` |
| `dead-letter-queue` | Mensajes fallidos para reprocessing | `dead-letter-queue-spark-sub` |

#### Decisión: Simulador como modo principal de desarrollo

aisstream.io es un servicio en BETA sin SLA garantizado. Durante el desarrollo encontramos que el endpoint WebSocket tiene problemas de handshake SSL en ciertas redes (renegociación TLS que Python no maneja correctamente). El simulador replica exactamente el mismo schema de datos y publica en los mismos topics de Pub/Sub — el resto del pipeline no distingue la fuente.

Para producción o cuando aisstream.io esté estable: `python ingestion/ais_producer/main.py`

Para desarrollo: `python ingestion/simulator/main.py --vessels 20 --interval 3.0`

---

### Fase 2 — Spark Bronze Layer

**Estado: ✅ Completada**

#### ¿Qué es la capa Bronze?

En arquitectura Medallion (Bronze/Silver/Gold), Bronze es la **zona de aterrizaje raw**. Los datos llegan tal cual desde la fuente — mínima transformación, máxima fidelidad. Si algo falla en capas superiores, siempre puedes reprocessar desde Bronze.

Bronze solo hace dos cosas:
1. Filtrar registros obviamente inválidos (lat/lon fuera de rango, MMSI nulo)
2. Añadir columnas de particionado (fecha, hora) para queries eficientes

#### Arquitectura del job Bronze

```
Pub/Sub subscription
      │
      ▼
Python pull (google-cloud-pubsub)
      │  hasta 500 mensajes por batch
      ▼
Schema enforcement (StructType explícito)
      │  evita errores de inferencia de tipos
      ▼
Spark validation
      │  lat entre -90/90, lon entre -180/180
      │  filtra sentinel values AIS (91.0, 181.0)
      ▼
Partition columns
      │  partition_date, partition_hour
      ▼
GCS Parquet
gs://marineflow-lake-{project}/bronze/vessel_positions/
  partition_date=2026-03-11/
    partition_hour=13/
      part-00000.parquet
```

#### ¿Por qué micro-batch y no Spark Structured Streaming nativo?

Intentamos usar `spark.readStream.format("pubsub")` pero no existe un conector oficial de Pub/Sub para Spark publicado en Maven. Google tiene el `pubsub-group-kafka-connector` pero requiere Kafka como intermediario — añadir Kafka solo como puente entre Pub/Sub y Spark es over-engineering injustificado.

El patrón micro-batch (pull Python → createDataFrame → write) es el estándar en pipelines GCP + Spark cuando no se usa Dataproc o Dataflow. En producción con Dataproc existiría el conector nativo o se usaría Dataflow (Apache Beam) directamente.

#### JARs necesarios

Los JARs son librerías Java que extienden las capacidades de Spark:

- **`gcs-connector-hadoop3-latest.jar`**: enseña a Spark a leer/escribir rutas `gs://`. Sin él, Spark no sabe qué es Google Cloud Storage.
- **`spark-3.5-bigquery-0.36.1.jar`**: permite a Spark escribir directamente en tablas de BigQuery (usado en Silver).

Se pasan al job con `--jars`:
```bash
spark-submit --jars jars/gcs-connector.jar,jars/bq-connector.jar bronze_positions.py
```

#### Docker y credenciales

Spark corre dentro de un contenedor Docker (`apache/spark:3.5.0`). El contenedor está completamente aislado de Windows — no ve el venv local ni las credenciales ADC del sistema.

Soluciones implementadas:
- **Dependencias Python**: instaladas manualmente con `pip install` dentro del contenedor (`docker exec -u root`). En la Fase 5 se creará un `Dockerfile` que las incluya permanentemente.
- **Credenciales ADC**: montadas como volumen read-only desde `C:\Users\usuario\AppData\Roaming\gcloud\application_default_credentials.json` → `/tmp/adc.json` dentro del contenedor. La ruta `/tmp/` es accesible por todos los usuarios incluyendo el usuario `spark` con el que corren los procesos internos.

#### Schema Bronze

```python
StructType([
    StructField("mmsi",                StringType()),   # ID único del barco
    StructField("vessel_name",         StringType()),
    StructField("vessel_type",         StringType()),   # código AIS numérico
    StructField("latitude",            DoubleType()),
    StructField("longitude",           DoubleType()),
    StructField("speed_over_ground",   DoubleType()),   # nudos
    StructField("course_over_ground",  DoubleType()),   # grados 0-360
    StructField("heading",             IntegerType()),  # grados 0-359
    StructField("navigational_status", StringType()),   # "underway", "at anchor", etc.
    StructField("destination",         StringType()),   # texto libre del capitán
    StructField("flag_country",        StringType()),   # derivado del MMSI MID
    StructField("event_timestamp",     StringType()),   # timestamp del mensaje AIS
    StructField("ingestion_timestamp", StringType()),   # timestamp de llegada a Pub/Sub
    StructField("source",              StringType()),   # "aisstream_live" o "simulator"
    StructField("pipeline_ingest_time",StringType()),   # timestamp del pull de Pub/Sub
    StructField("pubsub_message_id",   StringType()),   # ID único del mensaje Pub/Sub
])
```

El schema se define explícitamente (no inferido) porque algunos campos pueden ser `None` en todos los mensajes de un batch — Spark no puede inferir el tipo de una columna que solo contiene nulos.

---

### Fase 3 — Silver, Gold y dbt

**Estado: ⏳ Pendiente**

Planificado:
- `silver_positions.py`: deduplicación, normalización de vessel_type, enriquecimiento geoespacial (región oceánica, zona portuaria), deltas de velocidad y rumbo
- dbt models para capa Gold: agregaciones por puerto, por bandera, por ruta
- Airflow DAGs para orquestación diaria

---

### Fase 4 — Modelos ML

**Estado: ⏳ Pendiente**

Tres modelos planificados:

**1. Activity Classifier (XGBoost)**
Clasifica el estado de actividad del barco: en tránsito, pescando, esperando, maniobra portuaria. Features: velocidad, rumbo, tipo de barco, zona geográfica.

**2. Anomaly Detector (Isolation Forest)**
Detecta comportamientos anómalos: velocidades imposibles, gaps de AIS (barcos que "desaparecen"), desviaciones de ruta, barcos oscuros. Publica alertas al topic `maritime-alerts`.

**3. ETA Predictor (LSTM)**
Predice tiempo de llegada a puerto usando velocidad histórica, condiciones meteorológicas y rutas habituales. Reentrenamiento semanal via Airflow.

Todos los modelos se rastrean con MLflow (experimentos, métricas, artefactos).

---

### Fase 5 — API y Dashboard

**Estado: ⏳ Pendiente**

- FastAPI con WebSockets para streaming de posiciones en tiempo real
- Redis como caché de últimas posiciones conocidas
- Dashboard con mapa mundial de tráfico naval
- Deploy en Cloud Run via Terraform
- Prometheus + Grafana para métricas operacionales

---

## Infraestructura GCP (Terraform)

Toda la infraestructura está definida como código en `infra/terraform/`. Se puede reproducir completamente con `terraform apply`.

**Proyecto GCP**: `marineflow-489815`
**Región**: `us-central1`

### Recursos creados (33 en total)

**GCS Buckets** (`modules/gcs/`):
- `marineflow-tfstate`: estado remoto de Terraform
- `marineflow-lake-{project}`: data lake con estructura Bronze/Silver/Gold/checkpoints/models/schemas

**Pub/Sub** (`modules/pubsub/`):
- 5 topics con sus subscriptions correspondientes
- Dead Letter Queue configurada para mensajes fallidos

**BigQuery** (`modules/bigquery/`):
- `marineflow_bronze`: datos raw, tabla `vessel_positions_raw`
- `marineflow_silver`: datos limpios, tabla `vessel_positions_clean`
- `marineflow_gold`: tablas analíticas, tabla `maritime_alerts`
- `marineflow_features`: Feature Store para ML

**IAM** (`modules/iam/`):
- Service account `marineflow-sa` con roles mínimos necesarios

### Autenticación: ADC en lugar de Service Account Keys

La organización GCP tiene la org policy `constraints/iam.disableServiceAccountKeyCreation` que impide crear JSON keys de service accounts. En lugar de keys usamos **Application Default Credentials (ADC)**.

ADC es el método recomendado por Google para desarrollo local. Las credenciales se obtienen del usuario autenticado con `gcloud auth application-default login` y se renuevan automáticamente.

Para configurar:
```bash
gcloud auth application-default login
gcloud auth application-default set-quota-project marineflow-489815
```

---

## Configuración Local

### Prerequisitos

| Herramienta | Versión | Para qué |
|-------------|---------|---------|
| Python | 3.11 | Producer, simulador, jobs Spark |
| Java | 17+ | Spark (JVM) |
| Docker Desktop | 29+ | Spark, MLflow, Grafana |
| gcloud CLI | 372+ | Auth y operaciones GCP |
| Terraform | 1.5+ | Infraestructura |

### Variables de entorno (.env)

```bash
# GCP
GCP_PROJECT_ID=marineflow-489815
GCS_BUCKET=marineflow-lake-marineflow-489815
GOOGLE_CLOUD_PROJECT=marineflow-489815

# Pub/Sub
PUBSUB_TOPIC_POSITIONS=vessel-positions
PUBSUB_TOPIC_METADATA=vessel-metadata
PUBSUB_TOPIC_DLQ=dead-letter-queue
PUBSUB_SUB_POSITIONS=vessel-positions-spark-sub

# AIS (solo para producer real)
AIS_API_KEY=<tu key de aisstream.io>
MESSAGE_SOURCE=aisstream_live

# Spark
SPARK_MASTER=local[*]
SPARK_DRIVER_MEMORY=3g
SPARK_EXECUTOR_MEMORY=2g

# Bronze job
BRONZE_BATCH_SIZE=500
BRONZE_BATCH_INTERVAL=30
BRONZE_MAX_BATCHES=0

# BigQuery
BQ_DATASET_SILVER=marineflow_silver

# MLflow / Postgres
POSTGRES_USER=marineflow
POSTGRES_PASSWORD=marineflow_dev_password
POSTGRES_DB=marineflow
```

### Puertos locales

| Servicio | Puerto | UI |
|---------|--------|-----|
| Spark Master UI | 4040 | http://localhost:4040 |
| Spark Worker UI | 4041 | http://localhost:4041 |
| MLflow | 5001 | http://localhost:5001 |
| Grafana | 3000 | http://localhost:3000 |
| Prometheus | 9090 | http://localhost:9090 |
| PostgreSQL | 5432 | — |
| Redis | 6379 | — |

**Nota sobre puertos en Windows con Hyper-V**: Windows/Hyper-V reserva rangos de puertos (típicamente 8064-8763). Usar puertos por debajo de 8064 o entre 8764-49999. Los puertos 4040/4041 son los puertos nativos de Spark UI y funcionan correctamente.

---

## Decisiones de Diseño

### ¿Por qué GCP y no AWS o Azure?

Continuidad con el proyecto anterior (NYC Mobility). Tener dos proyectos en el mismo cloud demuestra profundidad en un ecosistema específico en lugar de conocimiento superficial de varios.

### ¿Por qué Spark local en Docker y no Dataproc?

Dataproc (Spark managed de GCP) cuesta dinero con el cluster corriendo. Para desarrollo local Docker es gratuito, reproducible y suficiente para demostrar los conceptos. En producción el código es idéntico — solo cambia el `--master`.

### ¿Por qué micro-batch y no Spark Structured Streaming nativo para Pub/Sub?

No existe un conector oficial de Pub/Sub para Spark en Maven. Las alternativas son:
1. Añadir Kafka como intermediario (over-engineering: Pub/Sub → Kafka → Spark)
2. Usar Dataflow (Apache Beam) en lugar de Spark
3. Micro-batch con Python client (nuestra elección)

El patrón micro-batch replica la misma semántica que Structured Streaming para nuestro caso de uso y es el enfoque estándar en pipelines GCP + Spark sin Dataproc/Dataflow.

### ¿Por qué schema explícito en Bronze y no inferencia?

Spark infiere el schema muestreando los datos. Si un campo es `None` en todos los registros del batch, la inferencia falla con `CANNOT_DETERMINE_TYPE`. El schema explícito elimina esta fragilidad y además documenta el contrato de datos del sistema.

### ¿Por qué Bronze solo valida lat/lon y no más campos?

Bronze es la fuente de verdad histórica. Si filtras demasiado en Bronze y luego descubres que el filtro era incorrecto, has perdido datos para siempre. La validación agresiva va en Silver (reversible) no en Bronze (inmutable).

### ¿Por qué ADC y no Service Account Keys?

Org policy lo impide, pero además ADC es la práctica recomendada por Google para desarrollo local. Las keys JSON son un riesgo de seguridad si se filtran accidentalmente en git. ADC rota automáticamente y está ligado al usuario autenticado.

---

## Limitaciones Conocidas

### aisstream.io BETA

El servicio de streaming AIS en tiempo real (`aisstream.io`) está en BETA sin SLA garantizado. Durante el desarrollo se encontraron problemas de conectividad WebSocket relacionados con renegociación TLS en el servidor. El simulador cubre esta limitación completamente para desarrollo y testing.

**Para intentar conexión real**: `python ingestion/ais_producer/main.py`

### Dependencias Python no persistentes en Docker

Las dependencias Python instaladas en el contenedor Spark con `pip install` se pierden al recrear el contenedor. Esto se resolverá en la Fase 5 con un `Dockerfile` propio que las incluya en la imagen.

**Workaround actual**:
```powershell
docker exec -u root marineflow-spark-master pip install `
  pyspark==3.5.0 google-cloud-pubsub==2.21.1 `
  google-cloud-storage==2.16.0 python-dotenv==1.0.1 structlog==24.1.0
```

### Spark en modo local

Spark corre en modo `local[*]` — un solo proceso usando todos los cores disponibles. No hay distribución real entre workers. Para producción se usaría `spark://spark-master:7077` con múltiples workers o Dataproc.

---

## Cómo Ejecutar

### 1. Configuración inicial

```bash
# Clone repo
git clone <repo-url>
cd marineflow

# Create venv and install dependencies
python -m venv venv
source venv/bin/activate  # or .\venv\Scripts\Activate.ps1 on Windows

# Copy and fill env file
cp .env.example .env
# Edit .env with your values

# Authenticate with GCP
gcloud config configurations activate marineflow
gcloud auth application-default login
gcloud auth application-default set-quota-project marineflow-489815
```

### 2. Infraestructura GCP

```bash
cd infra/terraform
terraform init
terraform plan -out marineflow.tfplan
terraform apply marineflow.tfplan
```

### 3. Levantar stack Docker

```powershell
docker compose up spark-master spark-worker -d

# Install Python deps in container (until Dockerfile is ready)
docker exec -u root marineflow-spark-master pip install `
  pyspark==3.5.0 google-cloud-pubsub==2.21.1 `
  google-cloud-storage==2.16.0 python-dotenv==1.0.1 structlog==24.1.0
```

### 4. Arrancar simulador

```powershell
cd ingestion/simulator
python main.py --vessels 20 --interval 3.0
```

### 5. Lanzar Bronze job

```powershell
docker exec `
  -e GCP_PROJECT_ID=marineflow-489815 `
  -e GCS_BUCKET=marineflow-lake-marineflow-489815 `
  -e PUBSUB_SUB_POSITIONS=vessel-positions-spark-sub `
  -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/adc.json `
  -e GOOGLE_CLOUD_PROJECT=marineflow-489815 `
  -e BRONZE_BATCH_INTERVAL=30 `
  -e BRONZE_BATCH_SIZE=500 `
  marineflow-spark-master /opt/spark/bin/spark-submit `
  --master local[*] `
  --driver-memory 3g `
  --jars /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar `
  /opt/spark/processing/spark_streaming/bronze_positions.py
```

### 6. Verificar datos en GCS

```bash
gcloud storage ls gs://marineflow-lake-marineflow-489815/bronze/vessel_positions/
```

---

*MarineFlow — Portfolio project by [tu nombre]*
*Stack: Python · Apache Spark · GCP Pub/Sub · GCS · BigQuery · dbt · Airflow · MLflow · FastAPI · Terraform*