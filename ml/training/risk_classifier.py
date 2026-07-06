"""
risk_classifier.py
==================
Clasificador binario XGBoost: predice si un buque tendrá risk_score > 50
en los próximos 7 días basándose en su comportamiento de los últimos 7.

Corrección de data leakage (v2)
---------------------------------
La versión anterior predecía risk_score > 50 usando risk_score como feature,
produciendo ROC-AUC = 1.0 y best_iteration = 0 — señales inequívocas de
tautología. El modelo no había aprendido nada.

La corrección es estructural, no de hiperparámetros:

    ANTES: features[T] → predice target[T]              ← leakage perfecto
    AHORA: features[T-7..T] → predice target[T+1..T+7]  ← predicción real

Las features son exclusivamente señales de comportamiento de movimiento
(velocidad, gaps AIS, heading) — ninguna columna derivada del risk_score.
El modelo aprende si los patrones de movimiento de esta semana anticipan
riesgo la semana siguiente.

Limitación conocida
--------------------
Con ~11 días de datos el dataset temporal es pequeño. El modelo es
arquitectónicamente correcto pero necesita 60-90 días de histórico para
ser estadísticamente robusto. Esto se documenta explícitamente en el log.

¿Por qué XGBoost?
------------------
- Robusto ante features skewed (típico en datos AIS)
- Invariante a escala — no necesita normalización adicional
- Maneja NULLs / ceros de forma natural
- Feature importance interpretable — útil para explicar alertas
- Rinde bien con datasets pequeños/medianos
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import joblib
import pandas as pd
from google.cloud import storage
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

from feature_engineering import XGBOOST_FEATURES, XGBOOST_TARGET, build_temporal_feature_matrix

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

GCS_BUCKET = "marineflow-lake-marineflow-489815"
GCS_MODEL_PREFIX = "ml/models/risk_classifier"
MODEL_ARTIFACT_NAME = "risk_classifier_v2.joblib"  # v2 — leakage corregido


@dataclass
class XGBConfig:
    """
    Hiperparámetros del clasificador.

    n_estimators / early_stopping_rounds
        Máximo generoso (500) con early stopping — el modelo para cuando
        el AUC en validación deja de mejorar. Evita overfitting sin
        tuning manual. Con pocos datos esperamos que pare pronto; eso es
        correcto, no un problema.

    max_depth=4
        Árboles poco profundos. Con dataset pequeño, profundidad mayor
        solo añade varianza sin reducir sesgo.

    learning_rate=0.05
        Conservador — combinado con early stopping produce modelos más
        estables. Con pocos datos preferimos estabilidad sobre velocidad.

    scale_pos_weight
        Se calcula dinámicamente desde los datos de entrenamiento.
        Compensa el desbalance esperado (pocos buques de alto riesgo).
    """
    n_estimators: int = 500
    early_stopping_rounds: int = 20
    max_depth: int = 4
    learning_rate: float = 0.05
    subsample: float = 0.8
    colsample_bytree: float = 0.8
    random_state: int = 42
    n_jobs: int = -1
    eval_metric: str = "auc"
    scale_pos_weight: Optional[float] = None  # calculado en train()
    test_size: float = 0.2
    feature_window_days: int = 7
    target_horizon_days: int = 7
    lookback_days: int = 30


@dataclass
class XGBArtifact:
    """
    Artefacto completo para persistencia y serving.
    Incluye modelo, lista de features y config — serving no necesita
    ningún otro contexto para reproducir exactamente el mismo scoring.
    """
    model: XGBClassifier
    feature_columns: list[str] = field(default_factory=lambda: XGBOOST_FEATURES)
    config: XGBConfig = field(default_factory=XGBConfig)
    feature_importances: Optional[pd.DataFrame] = None


# ---------------------------------------------------------------------------
# Entrenamiento
# ---------------------------------------------------------------------------

def train(config: Optional[XGBConfig] = None) -> tuple[XGBArtifact, dict]:
    """
    Entrena el clasificador XGBoost con separación temporal estricta.

    Retorna
    -------
    artifact : XGBArtifact
        Modelo listo para persistir y para serving.
    metrics : dict
        ROC-AUC, classification report, feature importances, tamaño del dataset.
        Preparado para logging en MLflow.
    """
    if config is None:
        config = XGBConfig()

    logger.info("=== XGBoost Risk Classifier v2 — inicio entrenamiento ===")
    logger.info("Config: %s", config)
    logger.info(
        "Ventanas: features=[T-%dd..T] | target=[T+1..T+%dd]",
        config.feature_window_days,
        config.target_horizon_days,
    )

    # 1. Dataset con separación temporal estricta
    df = build_temporal_feature_matrix(
        feature_window_days=config.feature_window_days,
        target_horizon_days=config.target_horizon_days,
        lookback_days=config.lookback_days,
    )

    X = df[XGBOOST_FEATURES].fillna(0).values
    y = df[XGBOOST_TARGET].values

    logger.info(
        "Dataset final — muestras: %d | positivos: %d (%.1f%%)",
        len(y), y.sum(), y.mean() * 100,
    )

    # 2. Split estratificado
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=config.test_size,
        stratify=y,
        random_state=config.random_state,
    )

    # 3. scale_pos_weight dinámico
    scale_pos_weight = config.scale_pos_weight or _compute_scale_pos_weight(y_train)
    logger.info("scale_pos_weight calculado: %.3f", scale_pos_weight)

    # 4. Entrenamiento con early stopping
    model = XGBClassifier(
        n_estimators=config.n_estimators,
        max_depth=config.max_depth,
        learning_rate=config.learning_rate,
        subsample=config.subsample,
        colsample_bytree=config.colsample_bytree,
        scale_pos_weight=scale_pos_weight,
        random_state=config.random_state,
        n_jobs=config.n_jobs,
        eval_metric=config.eval_metric,
        early_stopping_rounds=config.early_stopping_rounds,
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=False,
    )

    logger.info(
        "Entrenamiento completo — mejor iteración: %d (de %d máx.)",
        model.best_iteration,
        config.n_estimators,
    )

    # 5. Evaluación
    metrics = _evaluate(model, X_test, y_test)
    metrics["n_samples"] = len(y)
    metrics["n_vessels"] = df["mmsi"].nunique()
    metrics["positive_rate"] = float(y.mean())
    metrics["best_iteration"] = model.best_iteration

    # 6. Feature importances
    importances = _get_feature_importances(model)
    metrics["feature_importances"] = importances

    artifact = XGBArtifact(
        model=model,
        config=config,
        feature_importances=importances,
    )

    return artifact, metrics


def _compute_scale_pos_weight(y_train) -> float:
    """
    n_negativos / n_positivos — compensa el desbalance de clases.
    En vigilancia marítima preferimos recall alto sobre precision:
    es peor no detectar un buque sospechoso que generar una falsa alarma.
    """
    n_neg = (y_train == 0).sum()
    n_pos = (y_train == 1).sum()
    if n_pos == 0:
        logger.warning("Sin positivos en train — scale_pos_weight=1.0")
        return 1.0
    return float(n_neg / n_pos)


def _evaluate(model: XGBClassifier, X_test, y_test) -> dict:
    """
    Evalúa en test set y loggea métricas.

    ROC-AUC es la métrica principal — robusta ante desbalance de clases
    y no depende del umbral de decisión.
    Con pocos datos un AUC entre 0.65 y 0.85 es realista y defendible.
    Un AUC cercano a 1.0 con pocos datos debe interpretarse con cautela.
    """
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    auc = roc_auc_score(y_test, y_proba)
    report = classification_report(y_test, y_pred, output_dict=True)

    logger.info("ROC-AUC en test set: %.4f", auc)
    logger.info("\n%s", classification_report(y_test, y_pred))

    if auc > 0.98:
        logger.warning(
            "ROC-AUC = %.4f — valor sospechosamente alto. "
            "Verificar que no hay leakage residual en las features.",
            auc,
        )

    return {"roc_auc": auc, "classification_report": report}


def _get_feature_importances(model: XGBClassifier) -> pd.DataFrame:
    """
    Feature importances por 'gain' — cuánto mejora la pureza de los splits.
    Más interpretable que frecuencia de uso ('weight').
    """
    importances = pd.DataFrame({
        "feature": XGBOOST_FEATURES,
        "importance": model.feature_importances_,
    }).sort_values("importance", ascending=False).reset_index(drop=True)

    logger.info("\n=== Feature Importances (gain) ===\n%s", importances.to_string(index=False))
    return importances


# ---------------------------------------------------------------------------
# Persistencia en GCS
# ---------------------------------------------------------------------------

def save_artifact(artifact: XGBArtifact, local_path: Optional[Path] = None) -> str:
    """Serializa el artefacto y lo sube a GCS."""
    buffer = io.BytesIO()
    joblib.dump(artifact, buffer)
    buffer.seek(0)

    gcs_path = f"{GCS_MODEL_PREFIX}/{MODEL_ARTIFACT_NAME}"
    gcs_uri = f"gs://{GCS_BUCKET}/{gcs_path}"

    client = storage.Client()
    client.bucket(GCS_BUCKET).blob(gcs_path).upload_from_file(
        buffer, content_type="application/octet-stream"
    )
    logger.info("Artefacto subido a %s", gcs_uri)

    if local_path is not None:
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(artifact, local_path)
        logger.info("Copia local guardada en %s", local_path)

    return gcs_uri


def load_artifact(gcs_uri: Optional[str] = None) -> XGBArtifact:
    """Carga el artefacto desde GCS o ruta local. Usado en serving."""
    if gcs_uri is None:
        gcs_uri = f"gs://{GCS_BUCKET}/{GCS_MODEL_PREFIX}/{MODEL_ARTIFACT_NAME}"

    if gcs_uri.startswith("gs://"):
        parts = gcs_uri.replace("gs://", "").split("/", 1)
        buffer = io.BytesIO()
        storage.Client().bucket(parts[0]).blob(parts[1]).download_to_file(buffer)
        buffer.seek(0)
        artifact = joblib.load(buffer)
    else:
        artifact = joblib.load(gcs_uri)

    logger.info("Artefacto cargado: %s", gcs_uri)
    return artifact


# ---------------------------------------------------------------------------
# Scoring (inferencia)
# ---------------------------------------------------------------------------

def score(artifact: XGBArtifact, df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica el clasificador a nuevos datos.
    Devuelve risk_proba continua además del flag binario.
    En producción la probabilidad es más útil para priorizar alertas.
    """
    X = df[artifact.feature_columns].fillna(0).values
    risk_proba = artifact.model.predict_proba(X)[:, 1]
    risk_flag = artifact.model.predict(X)

    result = df[["mmsi"]].copy()
    if "anchor_date" in df.columns:
        result["anchor_date"] = df["anchor_date"]
    result["risk_proba"] = risk_proba
    result["high_risk_flag"] = risk_flag

    return result


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    config = XGBConfig()

    try:
        artifact, metrics = train(config)

        print(f"\nROC-AUC:          {metrics['roc_auc']:.4f}")
        print(f"Muestras totales: {metrics['n_samples']}")
        print(f"Buques únicos:    {metrics['n_vessels']}")
        print(f"Tasa positivos:   {metrics['positive_rate']:.1%}")
        print(f"Mejor iteración:  {metrics['best_iteration']}")
        print("\n=== Feature Importances ===")
        print(metrics["feature_importances"].to_string(index=False))

        gcs_uri = save_artifact(
            artifact,
            local_path=Path("ml/models/risk_classifier_v2.joblib"),
        )
        print(f"\nArtefacto guardado en: {gcs_uri}")

    except ValueError as e:
        print(f"\n[ESPERADO CON POCOS DATOS] {e}")
        print(
            "\nEl modelo es arquitectónicamente correcto. "
            "Ejecuta de nuevo cuando tengas 60-90 días de histórico en BigQuery."
        )