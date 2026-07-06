"""
isolation_forest.py
===================
Detección de anomalías no supervisada sobre comportamiento de buques.

¿Qué detecta?
-------------
Buques cuya combinación de features (velocidad, gaps AIS, giros, risk_score…)
es estadísticamente infrecuente respecto al resto de la flota. No necesita
etiquetas — aprende qué es "normal" y puntúa la rareza de cada observación.

Útil para encontrar buques sospechosos que no dispararon ninguna regla
individual en Gold pero cuyo comportamiento agregado es anómalo.

Flujo
-----
1. build_feature_matrix()  →  df_if con features normalizadas
2. IsolationForest.fit()   →  modelo aprende la distribución "normal"
3. decision_function()     →  score continuo por (mmsi, date_day)
4. Normalizar scores       →  [0, 1] — más alto = más anómalo
5. Persistir modelo + scaler en GCS para serving

Parámetro clave: contamination
--------------------------------
Fracción del dataset que asumimos son anomalías reales. Con datos AIS
reales ~5% es un punto de partida conservador y habitual en literatura
de detección de dark vessels. Ajustar según recall deseado en validación.
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import joblib
import numpy as np
import pandas as pd
from google.cloud import storage
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import MinMaxScaler

from feature_engineering import ISOLATION_FOREST_FEATURES, build_feature_matrix

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

GCS_BUCKET = "marineflow-lake-marineflow-489815"
GCS_MODEL_PREFIX = "ml/models/isolation_forest"

# Nombre del artefacto — incluye versión para no sobreescribir accidentalmente
MODEL_ARTIFACT_NAME = "isolation_forest_v1.joblib"


@dataclass
class IFConfig:
    """
    Parámetros del Isolation Forest.
    Centralizar aquí facilita el tracking de experimentos con MLflow.
    """
    contamination: float = 0.05    # 5% de anomalías asumidas en la flota
    n_estimators: int = 200        # más árboles = más estable; 200 es suficiente a esta escala
    max_samples: str | int = "auto"  # "auto" → min(256, n_samples)
    random_state: int = 42
    n_jobs: int = -1               # usa todos los cores disponibles
    lookback_days: int = 30        # ventana de datos de entrenamiento


@dataclass
class IFArtifact:
    """
    Artefacto completo que se persiste en GCS.
    Empaqueta modelo + scaler juntos para garantizar consistencia en serving.
    El scaler del risk_score debe ser el mismo que se usó en entrenamiento —
    de lo contrario los scores de inferencia no serían comparables.
    """
    model: IsolationForest
    risk_scaler: MinMaxScaler
    feature_columns: list[str] = field(default_factory=lambda: ISOLATION_FOREST_FEATURES)
    config: IFConfig = field(default_factory=IFConfig)


# ---------------------------------------------------------------------------
# Entrenamiento
# ---------------------------------------------------------------------------

def train(config: Optional[IFConfig] = None) -> tuple[IFArtifact, pd.DataFrame]:
    """
    Entrena el Isolation Forest y devuelve el artefacto + DataFrame con scores.

    Retorna
    -------
    artifact : IFArtifact
        Modelo + scaler listos para persistir y para serving.
    results : pd.DataFrame
        (mmsi, date_day, anomaly_score, is_anomaly) — una fila por observación.
    """
    if config is None:
        config = IFConfig()

    logger.info("=== Isolation Forest — inicio entrenamiento ===")
    logger.info("Config: %s", config)

    # 1. Carga y preparación de features
    df_if, _, risk_scaler = build_feature_matrix(lookback_days=config.lookback_days)

    X = df_if[ISOLATION_FOREST_FEATURES].values
    logger.info("Shape del dataset: %s", X.shape)

    # 2. Entrenamiento
    model = IsolationForest(
        contamination=config.contamination,
        n_estimators=config.n_estimators,
        max_samples=config.max_samples,
        random_state=config.random_state,
        n_jobs=config.n_jobs,
    )
    model.fit(X)
    logger.info("Modelo entrenado — n_estimators=%d", config.n_estimators)

    # 3. Scoring
    # decision_function: valores negativos = más anómalos, positivos = más normales
    # Lo invertimos y normalizamos a [0,1]: 1.0 = máxima anomalía
    raw_scores = model.decision_function(X)
    anomaly_scores = _normalize_scores(raw_scores)

    # predict() devuelve -1 (anómalo) o 1 (normal) según contamination
    labels = model.predict(X)
    is_anomaly = labels == -1

    # 4. Ensamblar resultados
    results = df_if[["mmsi", "date_day"]].copy()
    results["anomaly_score"] = anomaly_scores
    results["is_anomaly"] = is_anomaly

    n_anomalies = is_anomaly.sum()
    logger.info(
        "Anomalías detectadas: %d / %d (%.1f%%)",
        n_anomalies, len(results), n_anomalies / len(results) * 100
    )

    artifact = IFArtifact(
        model=model,
        risk_scaler=risk_scaler,
        config=config,
    )

    return artifact, results


def _normalize_scores(raw_scores: np.ndarray) -> np.ndarray:
    """
    Convierte decision_function output a [0, 1] donde 1 = más anómalo.

    decision_function devuelve scores negativos para anomalías.
    Invertimos el signo y aplicamos MinMaxScaler para interpretabilidad.
    """
    inverted = -raw_scores  # ahora positivo = más anómalo
    scaler = MinMaxScaler(feature_range=(0, 1))
    return scaler.fit_transform(inverted.reshape(-1, 1)).flatten()


# ---------------------------------------------------------------------------
# Persistencia en GCS
# ---------------------------------------------------------------------------

def save_artifact(artifact: IFArtifact, local_path: Optional[Path] = None) -> str:
    """
    Serializa el artefacto con joblib y lo sube a GCS.

    Guarda también una copia local si se especifica local_path,
    útil para debugging sin tener que descargar desde GCS.

    Retorna la ruta GCS del artefacto (gs://bucket/prefix/nombre).
    """
    buffer = io.BytesIO()
    joblib.dump(artifact, buffer)
    buffer.seek(0)

    gcs_path = f"{GCS_MODEL_PREFIX}/{MODEL_ARTIFACT_NAME}"
    gcs_uri = f"gs://{GCS_BUCKET}/{gcs_path}"

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_path)
    blob.upload_from_file(buffer, content_type="application/octet-stream")
    logger.info("Artefacto subido a %s", gcs_uri)

    if local_path is not None:
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(artifact, local_path)
        logger.info("Copia local guardada en %s", local_path)

    return gcs_uri


def load_artifact(gcs_uri: Optional[str] = None) -> IFArtifact:
    """
    Carga el artefacto desde GCS (o ruta local si se pasa un path de fichero).
    Usado en serving para inferencia sin reentrenar.
    """
    if gcs_uri is None:
        gcs_uri = f"gs://{GCS_BUCKET}/{GCS_MODEL_PREFIX}/{MODEL_ARTIFACT_NAME}"

    if gcs_uri.startswith("gs://"):
        path_parts = gcs_uri.replace("gs://", "").split("/", 1)
        bucket_name, blob_path = path_parts[0], path_parts[1]

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        buffer = io.BytesIO()
        blob.download_to_file(buffer)
        buffer.seek(0)
        artifact = joblib.load(buffer)
    else:
        artifact = joblib.load(gcs_uri)

    logger.info("Artefacto cargado: %s", gcs_uri)
    return artifact


# ---------------------------------------------------------------------------
# Scoring sobre nuevos datos (inferencia)
# ---------------------------------------------------------------------------

def score(
    artifact: IFArtifact,
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Aplica el modelo entrenado a nuevos datos.
    Usado en serving — los datos ya vienen con features calculadas.

    El risk_scaler del artefacto garantiza que la normalización de
    risk_score usa los mismos rangos que en entrenamiento.
    """
    # Re-normalizar risk_score con el scaler de entrenamiento
    df = df.copy()
    df["risk_score_normalized"] = artifact.risk_scaler.transform(
        df[["risk_score"]].fillna(0)
    )

    X = df[artifact.feature_columns].fillna(0).values
    raw_scores = artifact.model.decision_function(X)
    anomaly_scores = _normalize_scores(raw_scores)
    labels = artifact.model.predict(X)

    df["anomaly_score"] = anomaly_scores
    df["is_anomaly"] = labels == -1

    return df[["mmsi", "date_day", "anomaly_score", "is_anomaly"]]


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    config = IFConfig()
    artifact, results = train(config)

    # Mostrar top 20 buques más anómalos
    top = (
        results[results["is_anomaly"]]
        .sort_values("anomaly_score", ascending=False)
        .head(20)
    )
    print("\n=== Top 20 buques más anómalos ===")
    print(top.to_string(index=False))

    # Persistir
    gcs_uri = save_artifact(
        artifact,
        local_path=Path("ml/models/isolation_forest_v1.joblib"),
    )
    print(f"\nArtefacto guardado en: {gcs_uri}")
