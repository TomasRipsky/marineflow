"""
feature_engineering.py
=======================
Prepara los features para los modelos de Phase 4:
  - Isolation Forest      (detección de anomalías no supervisada)
  - XGBoost Risk Classifier  (predicción de alto riesgo en horizonte de 7 días)

Separación temporal — por qué es crítica
-----------------------------------------
El error más común en ML sobre series temporales es el data leakage:
usar información del futuro para predecir el futuro. En la versión
anterior, el modelo predecía risk_score > 50 usando risk_score como
feature — una tautología perfecta (ROC-AUC = 1.0, best_iteration = 0).

La arquitectura correcta para el XGBoost es:

    Ventana de features  [T-7 .. T]     →   lo que sabemos HOY del buque
    Ventana de target    [T+1 .. T+7]   →   lo que queremos predecir

Esto garantiza que el modelo aprende relaciones causales reales:
¿los patrones de movimiento de esta semana anticipan riesgo la siguiente?

Limitación conocida con datos actuales
----------------------------------------
Con ~11 días de histórico, el dataset temporal útil es pequeño
(pocos buques tienen datos en ambas ventanas). El modelo es correcto
pero necesita 60-90 días de datos para ser estadísticamente robusto.
Esto se documenta en el log en cada ejecución.

Por qué normalizamos risk_score en feature_engineering y no en dbt
-------------------------------------------------------------------
El risk_score Gold es una métrica de negocio auditable. Modificarla en
dbt mezclaría responsabilidades. La normalización es un artefacto del
pipeline ML que puede cambiar entre modelos o re-entrenamientos.
MinMaxScaler → [0,1]: preserva distribución, interpretable, robusto
ante la distribución skewed del risk_score (mayoría baja, pocos altos).
"""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd
from google.cloud import bigquery
from sklearn.preprocessing import MinMaxScaler

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

PROJECT_ID = "marineflow-489815"
GOLD_DATASET = "marineflow_gold"

# Features de comportamiento puro — sin ninguna columna derivada del risk_score.
# Estas son las señales que el buque emite ANTES de que el sistema las agregue
# en un score. Son las únicas válidas para predecir riesgo futuro sin leakage.
ISOLATION_FOREST_FEATURES = [
    "avg_speed_knots",
    "max_speed_knots",
    "avg_speed_change_rate",
    "max_speed_change_rate",
    "avg_heading_change",
    "sharp_turns",
    "ais_gaps_30min",
    "ais_gaps_2hr",
    "max_ais_gap_minutes",
    "estimated_distance_km",
    "positions_in_port",
    "positions_at_sea",
    "risk_score_normalized",  # válido en IF: no hay target, no hay leakage posible
]

# Features XGBoost: SOLO comportamiento de la ventana de observación.
# risk_score_normalized excluido — aunque sea de la ventana anterior,
# ya agrega señales de los 30d previos y contaminaría el aprendizaje.
# El modelo debe aprender desde señales crudas de movimiento.
XGBOOST_FEATURES = [
    "avg_speed_knots",
    "max_speed_knots",
    "avg_underway_speed_knots",
    "avg_speed_change_rate",
    "max_speed_change_rate",
    "avg_heading_change",
    "max_heading_change",
    "sharp_turns",
    "sudden_speed_changes",
    "ais_gaps_30min",
    "ais_gaps_2hr",
    "max_ais_gap_minutes",
    "estimated_distance_km",
    "positions_in_port",
    "positions_at_sea",
    "positions_stationary",
    "distinct_ports_visited",
    "active_minutes",
]

XGBOOST_TARGET = "high_risk_next_7d"  # risk_score > 50 en los 7 días siguientes


# ---------------------------------------------------------------------------
# Carga de datos desde BigQuery
# ---------------------------------------------------------------------------

def _load_activity(
    client: bigquery.Client,
    lookback_days: int = 30,
) -> pd.DataFrame:
    """
    Carga vessel_activity_summary completo para la ventana de lookback.
    Trae todas las columnas necesarias para construir ventanas temporales.
    """
    query = f"""
        SELECT
            mmsi,
            date_day,
            avg_speed_knots,
            max_speed_knots,
            avg_underway_speed_knots,
            avg_speed_change_rate,
            max_speed_change_rate,
            avg_heading_change,
            max_heading_change,
            sharp_turns,
            sudden_speed_changes,
            ais_gaps_30min,
            ais_gaps_2hr,
            max_ais_gap_minutes,
            estimated_distance_km,
            positions_in_port,
            positions_at_sea,
            positions_stationary,
            distinct_ports_visited,
            active_minutes,
            position_count
        FROM `{PROJECT_ID}.{GOLD_DATASET}.vessel_activity_summary`
        WHERE date_day >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
          AND avg_speed_knots IS NOT NULL
    """
    logger.info("Cargando vessel_activity_summary (lookback=%d días)…", lookback_days)
    df = client.query(query).to_dataframe()
    df["date_day"] = pd.to_datetime(df["date_day"])
    return df


def _load_risk_scores(client: bigquery.Client, lookback_days: int = 30) -> pd.DataFrame:
    """
    Carga TODOS los risk_scores del periodo, no solo el último.
    Necesitamos el histórico completo para construir el target temporal.
    """
    query = f"""
        SELECT
            mmsi,
            risk_score,
            score_date
        FROM `{PROJECT_ID}.{GOLD_DATASET}.vessel_risk_score`
        WHERE score_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
    """
    logger.info("Cargando vessel_risk_score histórico (lookback=%d días)…", lookback_days)
    df = client.query(query).to_dataframe()
    df["score_date"] = pd.to_datetime(df["score_date"])
    return df


# ---------------------------------------------------------------------------
# Normalización
# ---------------------------------------------------------------------------

def _normalize_risk_score(
    df: pd.DataFrame,
    scaler: Optional[MinMaxScaler] = None,
) -> tuple[pd.DataFrame, MinMaxScaler]:
    """
    Escala risk_score a [0,1] con MinMaxScaler.
    Retorna df con 'risk_score_normalized' añadida y el scaler ajustado,
    que debe persistirse junto al modelo para consistencia en serving.
    """
    values = df[["risk_score"]].fillna(0)

    if scaler is None:
        scaler = MinMaxScaler(feature_range=(0, 1))
        df["risk_score_normalized"] = scaler.fit_transform(values)
        logger.info(
            "MinMaxScaler ajustado: min=%.2f, max=%.2f",
            scaler.data_min_[0],
            scaler.data_max_[0],
        )
    else:
        df["risk_score_normalized"] = scaler.transform(values)
        logger.info("MinMaxScaler aplicado (pre-ajustado).")

    return df, scaler


# ---------------------------------------------------------------------------
# Construcción de features para Isolation Forest (sin separación temporal)
# ---------------------------------------------------------------------------

def build_feature_matrix(
    lookback_days: int = 30,
    risk_scaler: Optional[MinMaxScaler] = None,
) -> tuple[pd.DataFrame, MinMaxScaler]:
    """
    Matriz de features para Isolation Forest.

    No requiere separación temporal — el IF es no supervisado y no tiene
    target, por lo que no existe riesgo de data leakage.

    Retorna
    -------
    df_if : pd.DataFrame
        Features listas para IsolationForest.fit()
    scaler : MinMaxScaler
        Scaler ajustado sobre risk_score. Persístelo con el modelo.
    """
    client = bigquery.Client(project=PROJECT_ID)

    activity = _load_activity(client, lookback_days)

    # Para el IF usamos el último risk_score por buque
    risk_all = _load_risk_scores(client, lookback_days)
    risk_latest = (
        risk_all.sort_values("score_date")
        .groupby("mmsi")["risk_score"]
        .last()
        .reset_index()
    )

    df = activity.merge(risk_latest, on="mmsi", how="left")
    df["risk_score"] = df["risk_score"].fillna(0)

    df, scaler = _normalize_risk_score(df, risk_scaler)

    feature_cols = [c for c in ISOLATION_FOREST_FEATURES if c != "risk_score_normalized"]
    df[feature_cols] = df[feature_cols].fillna(0)

    df_if = df[["mmsi", "date_day"] + ISOLATION_FOREST_FEATURES].copy()

    logger.info(
        "IF feature matrix — filas: %d | buques únicos: %d",
        len(df_if), df_if["mmsi"].nunique(),
    )

    return df_if, scaler


# ---------------------------------------------------------------------------
# Construcción de features para XGBoost (CON separación temporal)
# ---------------------------------------------------------------------------

def build_temporal_feature_matrix(
    feature_window_days: int = 7,
    target_horizon_days: int = 7,
    lookback_days: int = 30,
) -> pd.DataFrame:
    """
    Construye el dataset con separación temporal estricta para XGBoost.

    Arquitectura temporal
    ---------------------
    Para cada buque y cada fecha T válida:

        [T - feature_window_days .. T]   →  features (lo que sabemos hoy)
        [T+1 .. T + target_horizon_days] →  target (lo que queremos predecir)

    Solo se incluyen buques con datos en AMBAS ventanas. Buques sin
    actividad en la ventana de target se descartan — no sabemos si su
    riesgo aumentó o simplemente dejaron de aparecer en los datos.

    Parámetros
    ----------
    feature_window_days : int
        Días de actividad a agregar como features (default: 7).
    target_horizon_days : int
        Días hacia adelante para calcular el target (default: 7).
    lookback_days : int
        Total de días históricos a cargar desde BigQuery.

    Retorna
    -------
    df : pd.DataFrame
        Una fila por (mmsi, anchor_date) con features agregadas y target.
        Columnas: mmsi, anchor_date + XGBOOST_FEATURES + XGBOOST_TARGET
    """
    client = bigquery.Client(project=PROJECT_ID)

    activity = _load_activity(client, lookback_days)
    risk_all = _load_risk_scores(client, lookback_days)

    # Fechas ancla válidas: necesitan al menos target_horizon_days de margen
    # hacia adelante en los datos disponibles
    max_date = activity["date_day"].max()
    min_anchor = activity["date_day"].min() + pd.Timedelta(days=feature_window_days)
    max_anchor = max_date - pd.Timedelta(days=target_horizon_days)

    if min_anchor > max_anchor:
        raise ValueError(
            f"Datos insuficientes para separación temporal. "
            f"Necesitas al menos {feature_window_days + target_horizon_days + 1} días. "
            f"Disponibles: {(max_date - activity['date_day'].min()).days + 1} días. "
            f"Con 60-90 días de histórico el dataset será estadísticamente robusto."
        )

    anchor_dates = pd.date_range(min_anchor, max_anchor, freq="D")
    logger.info(
        "Fechas ancla válidas: %d | rango: %s → %s",
        len(anchor_dates),
        min_anchor.date(),
        max_anchor.date(),
    )

    records = []

    for anchor in anchor_dates:
        # Ventana de features: [anchor - feature_window_days .. anchor]
        feat_start = anchor - pd.Timedelta(days=feature_window_days)
        feat_mask = (
            (activity["date_day"] >= feat_start) &
            (activity["date_day"] <= anchor)
        )
        feat_window = activity[feat_mask]

        # Ventana de target: [anchor+1 .. anchor + target_horizon_days]
        tgt_start = anchor + pd.Timedelta(days=1)
        tgt_end = anchor + pd.Timedelta(days=target_horizon_days)
        tgt_mask = (
            (risk_all["score_date"] >= tgt_start) &
            (risk_all["score_date"] <= tgt_end)
        )
        tgt_window = risk_all[tgt_mask]

        if feat_window.empty or tgt_window.empty:
            continue

        # Agregar features por buque en la ventana de observación
        feat_agg = (
            feat_window
            .groupby("mmsi")
            .agg(
                avg_speed_knots=("avg_speed_knots", "mean"),
                max_speed_knots=("max_speed_knots", "max"),
                avg_underway_speed_knots=("avg_underway_speed_knots", "mean"),
                avg_speed_change_rate=("avg_speed_change_rate", "mean"),
                max_speed_change_rate=("max_speed_change_rate", "max"),
                avg_heading_change=("avg_heading_change", "mean"),
                max_heading_change=("max_heading_change", "max"),
                sharp_turns=("sharp_turns", "sum"),
                sudden_speed_changes=("sudden_speed_changes", "sum"),
                ais_gaps_30min=("ais_gaps_30min", "sum"),
                ais_gaps_2hr=("ais_gaps_2hr", "sum"),
                max_ais_gap_minutes=("max_ais_gap_minutes", "max"),
                estimated_distance_km=("estimated_distance_km", "sum"),
                positions_in_port=("positions_in_port", "sum"),
                positions_at_sea=("positions_at_sea", "sum"),
                positions_stationary=("positions_stationary", "sum"),
                distinct_ports_visited=("distinct_ports_visited", "sum"),
                active_minutes=("active_minutes", "sum"),
            )
            .reset_index()
        )

        # Target: ¿tuvo risk_score > 50 en algún día de la ventana futura?
        tgt_agg = (
            tgt_window
            .groupby("mmsi")["risk_score"]
            .max()
            .reset_index()
            .rename(columns={"risk_score": "max_future_risk_score"})
        )
        tgt_agg[XGBOOST_TARGET] = (tgt_agg["max_future_risk_score"] > 50).astype(int)

        # Solo buques con datos en AMBAS ventanas
        merged = feat_agg.merge(tgt_agg[["mmsi", XGBOOST_TARGET]], on="mmsi", how="inner")
        merged["anchor_date"] = anchor.date()

        records.append(merged)

    if not records:
        raise ValueError(
            "No se pudieron construir muestras con separación temporal. "
            "Verifica que hay datos suficientes en vessel_activity_summary "
            "y vessel_risk_score para el periodo solicitado."
        )

    df = pd.concat(records, ignore_index=True)
    df = df.fillna(0)

    # Deduplicar: si un buque aparece en múltiples anchor_dates, mantenemos todas
    # las filas — son observaciones independientes en momentos distintos.
    n_samples = len(df)
    n_vessels = df["mmsi"].nunique()
    n_positive = df[XGBOOST_TARGET].sum()
    positive_rate = n_positive / n_samples * 100

    logger.info(
        "Dataset temporal construido — muestras: %d | buques únicos: %d | "
        "positivos: %d (%.1f%%) | fechas ancla: %d",
        n_samples, n_vessels, n_positive, positive_rate, len(anchor_dates),
    )

    if n_samples < 500:
        logger.warning(
            "Dataset pequeño (%d muestras). El modelo es arquitectónicamente correcto "
            "pero necesita más datos históricos para ser estadísticamente robusto. "
            "Recomendado: 60-90 días de histórico.",
            n_samples,
        )

    return df