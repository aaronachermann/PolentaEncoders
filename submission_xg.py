# Databricks notebook source
# MAGIC %md
# MAGIC # Energy Consumption Prediction: Submission
# MAGIC
# MAGIC ## Objective
# MAGIC Predict the **total (aggregated) energy consumption** across all clients for each
# MAGIC **15-minute interval** in **2026**.
# MAGIC
# MAGIC ## How It Works
# MAGIC 1. Add as many cells as you need **above** the Submit cell: install packages, import
# MAGIC    libraries, load additional data, do pre-computation, and define your `EnergyConsumptionModel` class.
# MAGIC 2. Run the **Submit** cell (the last cell). This triggers a scoring job that:
# MAGIC    - Re-runs **this entire notebook** with access to the full dataset (2025 + 2026)
# MAGIC    - Your `%pip install` commands, imports, and model class all run exactly as if you ran them yourself
# MAGIC    - Calls `model.predict()` to generate predictions for 2026
# MAGIC    - Computes your MAE and records it on the leaderboard
# MAGIC 3. Your **MAE score** and remaining submissions are printed once the job finishes.
# MAGIC
# MAGIC ## Model Contract
# MAGIC `predict(self, df, predict_start, predict_end)` receives a **PySpark DataFrame** with
# MAGIC **all** data (2025 + 2026):
# MAGIC - Columns: `client_id` (int), `datetime_local` (timestamp), `community_code` (string), `active_kw` (double)
# MAGIC - `predict_start` / `predict_end` define the prediction window
# MAGIC - `spark` is available as a global (you can use `spark.table()`, `spark.createDataFrame()`, etc.)
# MAGIC
# MAGIC It must return a **PySpark DataFrame** with exactly two columns:
# MAGIC - `datetime_15min` (timestamp): the 15-minute interval (floor of the timestamp)
# MAGIC - `prediction` (double): the **total** predicted `active_kw` across all clients for that interval
# MAGIC - One row per 15-minute interval in the prediction window
# MAGIC
# MAGIC ## Rules
# MAGIC - **Limited submissions** per team. Use the **exploration notebook** for local validation before submitting.
# MAGIC - Only **successful** submissions count towards the limit.
# MAGIC - **Do not modify the Submit cell** (the last cell). Everything else is yours to change.
# MAGIC
# MAGIC ## Performance Tips
# MAGIC - Use **PySpark** for all heavy data processing. Avoid `.toPandas()` on the full dataset.
# MAGIC - If using ML libraries (LightGBM, sklearn, etc.), do feature engineering in PySpark first, then
# MAGIC   `.toPandas()` only the **compact feature matrix** (e.g. aggregated 15-min intervals).
# MAGIC - Scoring has a **timeout (45 min)**. Keep the PySpark→pandas conversion for the very last step.
# MAGIC
# MAGIC ## Evaluation
# MAGIC **MAE (Mean Absolute Error)** on the aggregated 15-minute totals. Lower is better.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# >>> Set your team name <<<
TEAM_NAME = "polenta_encoders"  # lowercase, no spaces, use underscores

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Packages (optional)
# MAGIC
# MAGIC Add `%pip install` cells here if your model needs additional packages.
# MAGIC These will also be installed during scoring.

# COMMAND ----------

# MAGIC %pip install xgboost

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import numpy as np

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col as C
from pyspark.sql.types import IntegerType, LongType, StructField, StructType
import xgboost as xgb

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Your Model
# MAGIC
# MAGIC The baseline below predicts each 15-minute interval using the value from
# MAGIC **7 days ago**, with the historical mean as a fallback. It gives you an initial score to beat.
# MAGIC
# MAGIC Feel free to add as many cells as you need above this one for pre-computation.
# MAGIC Everything above the Submit cell will be executed during scoring.

# COMMAND ----------

def cluster_clients(df, k: int) -> list:
    """
    End-to-end clustering of clients by hourly load-profile shape.

    Steps reproduced from the EDA above:
      1. Aggregate df -> per-client hourly means (h00-h23), pct_zero, we/wd ratio
      2. Min-max normalise each client's 24-h profile to [0,1]
      3. Stack scalar features (we_wd_ratio clipped [0,3], pct_zero)
      4. Fit KMeans(n_clusters=k)

    Parameters
    ----------
    k : int - number of clusters

    Returns
    -------
    list[list[int]] - k lists, where the i-th list contains the client_ids
                      assigned to cluster i.
    """
    from pyspark.sql.functions import col as C
    from sklearn.cluster import KMeans

    hourly_pivoted = (
        df.withColumn('hour', F.hour('datetime_local'))
          .groupBy('client_id')
          .agg(
              *[F.mean(F.when(C('hour') == h, C('active_kw'))).alias(f'h{h:02d}')
                for h in range(24)],
              F.mean('active_kw').alias('mean_kw'),
              F.mean((C('active_kw') == 0).cast('int')).alias('pct_zero'),
          )
    )

    wde = (
        df.withColumn('is_weekend', F.dayofweek('datetime_local').isin([1, 7]).cast('int'))
          .groupBy('client_id')
          .agg(
              F.mean(F.when(C('is_weekend') == 0, C('active_kw'))).alias('wd_avg'),
              F.mean(F.when(C('is_weekend') == 1, C('active_kw'))).alias('we_avg'),
          )
          .withColumn('we_wd_ratio',
                      F.when(C('wd_avg') > 0, C('we_avg') / C('wd_avg')).otherwise(1.0))
    )

    # .toPandas() is safe: one row per client (small frame)
    feat = (
        hourly_pivoted
        .join(wde.select('client_id', 'we_wd_ratio'), on='client_id', how='left')
        .toPandas()
    )

    hour_cols = [f'h{h:02d}' for h in range(24)]
    H = feat[hour_cols].fillna(0).values.astype(float)
    row_max = H.max(axis=1, keepdims=True)
    row_max[row_max == 0] = 1
    H_norm = H / row_max

    extra = np.column_stack([
        feat['we_wd_ratio'].fillna(1.0).clip(0, 3).values,
        feat['pct_zero'].fillna(0).values,
    ])
    X = np.hstack([H_norm, extra])  # shape: (n_clients, 26)

    km = KMeans(n_clusters=k, random_state=42, n_init=20)
    feat['cluster'] = km.fit_predict(X)

    return [
        feat.loc[feat['cluster'] == c, 'client_id'].tolist()
        for c in range(k)
    ]

def build_cluster_mapping(clusters: list) -> DataFrame:
    """
    Convert the list-of-lists output from cluster_clients into a Spark DataFrame.

    The input is small (one row per client), so driver-side createDataFrame is fine.

    Parameters
    ----------
    clusters : list[list[int]]
        k lists of client_ids; index position == cluster_id.

    Returns
    -------
    Spark DataFrame with columns: client_id (LongType), cluster_id (IntegerType)
    """
    rows = [
        (int(client_id), int(cluster_id))
        for cluster_id, client_ids in enumerate(clusters)
        for client_id in client_ids
    ]

    schema = StructType([
        StructField("client_id",  LongType(),    nullable=False),
        StructField("cluster_id", IntegerType(), nullable=False),
    ])

    return spark.createDataFrame(rows, schema)

def attach_cluster_id(client_df: DataFrame, cluster_map_df: DataFrame) -> DataFrame:
    """
    Broadcast-join the cluster mapping onto the full client consumption table.

    Uses inner join: only rows with a valid cluster assignment are kept.

    Returns
    -------
    DataFrame with columns:
        client_id, cluster_id, datetime_utc, datetime_local, community_code, active_kw
    """
    return (
        client_df
        .join(
            F.broadcast(cluster_map_df),  # small side → broadcast join
            on="client_id",
            how="inner",
        )
        .select(
            "client_id",
            "cluster_id",
            "datetime_utc",
            "datetime_local",
            "community_code",
            "active_kw",
        )
    )

def aggregate_cluster_load(df_with_cluster: DataFrame) -> DataFrame:
    """
    Aggregate client consumption to cluster x UTC-timestamp granularity.

    Returns
    -------
    DataFrame with columns:
        cluster_id, datetime_utc, datetime_local, sum_active_kw
    Sorted by cluster_id ASC, datetime_utc ASC.
    """
    return (
        df_with_cluster
        .groupBy("cluster_id", "datetime_utc")
        .agg(
            F.first("datetime_local").alias("datetime_local"),
            F.sum("active_kw").alias("sum_active_kw"),
        )
        .orderBy("cluster_id", "datetime_utc")
    )

def add_temporal_features(df: DataFrame) -> DataFrame:
    """
    Add lag and rolling features to the cluster-level aggregated DataFrame.

    All windows are partitioned by (cluster_id, community_code) and ordered by
    datetime_utc, so each cluster-community pair is treated as its own time series.

    Assumes 15-min data resolution (96 slots per day).

    Parameters
    ----------
    df : DataFrame
        Output of aggregate_cluster_load — must contain:
        cluster_id, community_code, datetime_utc, sum_active_kw.

    Returns
    -------
    DataFrame with all original columns plus:
        lag_7d_same_slot, lag_14d_same_slot, lag_21d_same_slot, lag_28d_same_slot,
        same_slot_mean_4w, safe_lag_count,
        rolling_mean_48h_safe, rolling_mean_7d_safe,
        short_ma_minus_long_ma
    """
    from pyspark.sql.window import Window

    # Base window: one independent series per (cluster, community), ordered by time
    window_base = (
    Window
    .partitionBy("cluster_id")   # ← solo cluster_id
    .orderBy("datetime_utc")
    )

    # Safe rolling windows: right boundary at -193 excludes the nearest 48 h of data,
    # preventing any leakage of future values into the feature at forecast time.
    window_rolling_48h_safe = window_base.rowsBetween(-383, -193)   # 48 h window, 48 h ago
    window_rolling_7d_safe  = window_base.rowsBetween(-863, -193)   # 7-day window, 48 h ago

    # ── Lag features (same time slot, N days back) ────────────────────────────
    # 15-min resolution: 1 day = 96 slots
    df = (
        df
        .withColumn("lag_7d_same_slot",  F.lag("sum_active_kw", 672).over(window_base))
        .withColumn("lag_14d_same_slot", F.lag("sum_active_kw", 1344).over(window_base))
        .withColumn("lag_21d_same_slot", F.lag("sum_active_kw", 2016).over(window_base))
        .withColumn("lag_28d_same_slot", F.lag("sum_active_kw", 2688).over(window_base))
    )

    # ── Null-safe mean across the 4 same-slot lags ───────────────────────────
    lag_cols = ["lag_7d_same_slot", "lag_14d_same_slot", "lag_21d_same_slot", "lag_28d_same_slot"]
    safe_lags = [F.col(c) for c in lag_cols]

    non_null_count = sum(
        F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in lag_cols
    )

    df = (
        df
        .withColumn("safe_same_slot_array", F.array(*safe_lags))
        .withColumn("safe_lag_count", non_null_count)
        .withColumn(
            "same_slot_mean_4w",
            F.aggregate(
                "safe_same_slot_array",
                F.lit(0.0),
                lambda acc, x: acc + F.coalesce(x, F.lit(0.0)),
            ) / F.greatest(F.col("safe_lag_count"), F.lit(1)),
        )
        # Drop the intermediate array column; it is not needed downstream
        .drop("safe_same_slot_array")
    )

    # ── Rolling statistics (leak-safe windows) ────────────────────────────────
    df = (
        df
        .withColumn("rolling_mean_48h_safe", F.mean("sum_active_kw").over(window_rolling_48h_safe))
        .withColumn("rolling_mean_7d_safe",  F.mean("sum_active_kw").over(window_rolling_7d_safe))
        .withColumn(
            "short_ma_minus_long_ma",
            F.col("rolling_mean_48h_safe") - F.col("rolling_mean_7d_safe"),
        )
    )

    return df.drop("safe_lag_count")

def join_external_features(df: DataFrame) -> DataFrame:
    """
    Enrich the cluster-level feature DataFrame with four external signals:
      1. Holiday flags       — community-level, broadcast join
      2. Interpolated forecasts — hourly demand/PV/wind linearly resampled to 15-min
      3. Weekday info        — date-level, broadcast join
      4. National visitors   — date-level daily population signal, broadcast join

    Parameters
    ----------
    df : DataFrame
        Output of add_temporal_features. Must contain:
        cluster_id, community_code, datetime_utc, datetime_local.

    Returns
    -------
    DataFrame with all original columns plus the external feature columns,
    sorted by cluster_id, community_code, datetime_utc.
    """
    from pyspark.sql.window import Window

    # ── 1. Holiday flags ──────────────────────────────────────────────────────
    holidays_df = spark.table("datathon.polenta_encoders.combined_holidays_table")

    holidays_dedup = (
    holidays_df
    .select("datetime_utc", "is_normal_holiday")  # ← solo queste due
    .dropDuplicates(["datetime_utc"])
)
    df = df.join(F.broadcast(holidays_dedup), on=["datetime_utc"], how="left")
    # Stable integer encoding of community_code (global dense rank, no partition needed)
    # NOTE: the "No Partition Defined" warning below is expected and harmless —
    # the lookup table has few distinct community codes so the single-partition sort is cheap.
    window_community = Window.orderBy("community_code")
    #df = df.withColumn("community_code_num", F.dense_rank().over(window_community) - 1)

    # ── 2. Interpolated forecasts (hourly → 15-min linear) ───────────────────
    # Prepare hourly forecast table with current-hour and next-hour values
    # NOTE: Window.orderBy without partition triggers the same expected warning.
    window_forecast = Window.orderBy("hour_start")

    forecasts_prep = (
        forecasts_combined_df
        .select(
            F.col("datetime_utc").alias("hour_start"),
            F.col("demand_forecast").alias("demand_curr")
        )
        .withColumn("demand_next", F.lead("demand_curr").over(window_forecast))
        # Last row has no next value — carry forward the current value
        .withColumn("demand_next", F.coalesce(F.col("demand_next"), F.col("demand_curr")))
    )

    # Linear interpolation weight: how far into the hour is this 15-min slot?
    # value = (1 - alpha) * curr + alpha * next,  alpha = minute / 60
    alpha = F.minute(F.col("datetime_utc")) / F.lit(60.0)

    df = (
        df
        .withColumn("hour_start", F.date_trunc("hour", F.col("datetime_utc")))
        .join(F.broadcast(forecasts_prep), on="hour_start", how="left")
        .withColumn("demand_forecast",
            F.col("demand_curr") * (F.lit(1.0) - alpha) + F.col("demand_next") * alpha)
        # Drop intermediate helper columns
        .drop("hour_start",
              "demand_curr", "demand_next")
    )

    # ── 3. Weekday table ──────────────────────────────────────────────────────
    weekday_df = spark.table("datathon.polenta_encoders.weekday")

    df = (
        df
        .withColumn("_date_local", F.to_date(F.col("datetime_local")))
        .join(F.broadcast(weekday_df), F.col("_date_local") == F.col("date"), how="left")
        .drop("_date_local", "date")
    )

    # ── 4. National daily population — visitors only ──────────────────────────
    population_df = (
        spark.table("datathon.polenta_encoders.national_daily_population")
        .select("date", "visitors")
    )

    df = (
        df
        .withColumn("_date_local", F.to_date(F.col("datetime_local")))
        .join(F.broadcast(population_df), F.col("_date_local") == F.col("date"), how="left")
        .drop("_date_local", "date")
    )

    return df.orderBy("cluster_id", "datetime_utc").drop("is_school_holiday")



# COMMAND ----------


# Columns to always exclude from features
EXCLUDE_COLS = {'sum_active_kw', 'datetime_local', 'datetime_utc', 'cluster_id'}

def train_xgboost_model(df):
    """
    Trains an XGBoost model to predict sum_active_kw.

    Automatically selects all columns as features except for the target
    (sum_active_kw) and non-predictive columns (timestamps, cluster_id).

    Args:
        df: PySpark DataFrame. Must contain 'sum_active_kw' as the target.
            All other columns not in EXCLUDE_COLS are used as features.

    Returns:
        Tuple of (trained XGBoost model, list of feature column names).
    """
    print("Training XGBoost model...")

    feature_cols = [c for c in df.columns if c not in EXCLUDE_COLS]
    print(f"  Features ({len(feature_cols)}): {feature_cols}")

    pandas_df = (
        df.select(feature_cols + ['sum_active_kw'])
          .dropna()
          .toPandas()
    )

    X = pandas_df[feature_cols].values
    y = pandas_df['sum_active_kw'].values

    dtrain = xgb.DMatrix(X, label=y, feature_names=feature_cols)
    params = {
        'objective': 'reg:squarederror',
        'eval_metric': 'mae',
        'seed': 42
    }
    model = xgb.train(params, dtrain, num_boost_round=100)
    return model, feature_cols

def predict_xgboost(model, df, feature_cols):
    """
    Uses a trained XGBoost model to predict sum_active_kw.

    Args:
        model: Trained XGBoost model.
        df: PySpark DataFrame with the required feature columns.
        feature_cols: List of feature column names (returned by train_xgboost_model).

    Returns:
        PySpark DataFrame with columns:
            - datetime_local (timestamp)
            - datetime_utc (timestamp)
            - prediction (double)
    """
    pandas_df = (
        df.select(['datetime_local', 'datetime_utc'] + feature_cols)
          .toPandas()
    )

    # Safety net: if no rows remain, return an empty DataFrame with correct schema
    if pandas_df.empty:
        from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType
        schema = StructType([
            StructField("datetime_local", TimestampType(), True),
            StructField("datetime_utc", TimestampType(), True),
            StructField("prediction", DoubleType(), True),
        ])
        return spark.createDataFrame([], schema)

    X = pandas_df[feature_cols].values
    preds = model.predict(xgb.DMatrix(X, feature_names=feature_cols))
    pandas_df['prediction'] = preds
    result_df = spark.createDataFrame(
        pandas_df[['datetime_local', 'datetime_utc', 'prediction']]
    )
    return result_df

# COMMAND ----------

class EnergyConsumptionModel:
    """
    Energy consumption forecasting model.

    predict(df, predict_start, predict_end):
        Given data as a PySpark DataFrame, return predictions for every
        15-min interval between predict_start and predict_end.
    """

    
    def predict(self, df, predict_start, predict_end):
        """
        Args:
            df: PySpark DataFrame with columns client_id, datetime_local,
                         community_code, active_kw.
            predict_start: Start of the prediction period (inclusive), e.g. "2025-12-01".
            predict_end: End of the prediction period (exclusive), e.g. "2026-03-01".

        Must return a PySpark DataFrame with columns:
          - datetime_15min (timestamp): the 15-minute interval
          - prediction (double): total predicted active_kw for that interval
        """
        # Cluster and group by similarity of behaviour
        forecasts_combined_df        = spark.table("datathon.polenta_encoders.forecasts_combined")
        community_consumption_agg_df = spark.table("datathon.polenta_encoders.community_consumption_agg")
        # Step 1: Cluster clients
        clusters = cluster_clients(df, k=5)
        # Step 2: Build cluster mapping DataFrame
        cluster_map_df = build_cluster_mapping(clusters)
        # Step 3: Attach cluster_id to full client data
        df_with_cluster = attach_cluster_id(df, cluster_map_df)
        # Step 4: Aggregate client consumption to cluster x timestamp
        cluster_agg_df = aggregate_cluster_load(df_with_cluster)

        # Step 5: Add temporal features, including custom lags and rolling features
        from pyspark.sql.window import Window

        window_base = Window.partitionBy("cluster_id").orderBy("datetime_utc")
        window_rolling_7d_safe = window_base.rowsBetween(-863, -193)

        # Add lags
        cluster_agg_df = (
            cluster_agg_df
            .withColumn("lag_2d_same_slot",  F.lag("sum_active_kw", 192).over(window_base))
            .withColumn("lag_7d_same_slot",  F.lag("sum_active_kw", 672).over(window_base))
            .withColumn("lag_14d_same_slot", F.lag("sum_active_kw", 1344).over(window_base))
            .withColumn("lag_21d_same_slot", F.lag("sum_active_kw", 2016).over(window_base))
            .withColumn("lag_28d_same_slot", F.lag("sum_active_kw", 2688).over(window_base))
        )

        # same_slot_mean_4w: mean of lag_7d, lag_14d, lag_21d, lag_28d
        lag_cols = ["lag_7d_same_slot", "lag_14d_same_slot", "lag_21d_same_slot", "lag_28d_same_slot"]
        safe_lags = [F.col(c) for c in lag_cols]
        non_null_count = sum(F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in lag_cols)
        cluster_agg_df = (
            cluster_agg_df
            .withColumn("safe_same_slot_array", F.array(*safe_lags))
            .withColumn("safe_lag_count", non_null_count)
            .withColumn(
                "same_slot_mean_4w",
                F.aggregate(
                    "safe_same_slot_array",
                    F.lit(0.0),
                    lambda acc, x: acc + F.coalesce(x, F.lit(0.0)),
                ) / F.greatest(F.col("safe_lag_count"), F.lit(1)),
            )
            .drop("safe_same_slot_array")
        )

        # rolling_mean_7d_safe and short_ma_minus_long_ma
        cluster_agg_df = (
            cluster_agg_df
            .withColumn("rolling_mean_7d_safe", F.mean("sum_active_kw").over(window_rolling_7d_safe))
        )
        # For short_ma_minus_long_ma, reuse add_temporal_features logic
        cluster_agg_df = add_temporal_features(cluster_agg_df)

        # last_measured_value: previous value of sum_active_kw
        cluster_agg_df = cluster_agg_df.withColumn("last_measured_value", F.lag("sum_active_kw", 1).over(window_base))

        # time_slots_distance: difference in slots since last non-null value
        cluster_agg_df = cluster_agg_df.withColumn(
            "time_slots_distance",
            F.when(F.col("last_measured_value").isNotNull(), F.lit(1)).otherwise(F.lit(None))
        )

        # Step 6: Join external features
        # is_normal_holiday
        holidays_df = spark.table("datathon.polenta_encoders.combined_holidays_table")
        holidays_dedup = holidays_df.select("datetime_utc", "is_normal_holiday").dropDuplicates(["datetime_utc"])
        cluster_agg_df = cluster_agg_df.join(F.broadcast(holidays_dedup), on="datetime_utc", how="left")

        # demand_forecast
        forecasts_combined_df = spark.table("datathon.polenta_encoders.forecasts_combined")
        window_forecast = Window.orderBy("hour_start")
        forecasts_prep = (
            forecasts_combined_df
            .select(
                F.col("datetime_utc").alias("hour_start"),
                F.col("demand_forecast").alias("demand_curr")
            )
            .withColumn("demand_next", F.lead("demand_curr").over(window_forecast))
            .withColumn("demand_next", F.coalesce(F.col("demand_next"), F.col("demand_curr")))
        )
        alpha = F.minute(F.col("datetime_utc")) / F.lit(60.0)
        cluster_agg_df = (
            cluster_agg_df
            .withColumn("hour_start", F.date_trunc("hour", F.col("datetime_utc")))
            .join(F.broadcast(forecasts_prep), on="hour_start", how="left")
            .withColumn("demand_forecast",
                F.col("demand_curr") * (F.lit(1.0) - alpha) + F.col("demand_next") * alpha)
            .drop("hour_start", "demand_curr", "demand_next")
        )

        # day (from weekday table)
        weekday_df = spark.table("datathon.polenta_encoders.weekday")
        cluster_agg_df = (
            cluster_agg_df
            .withColumn("_date_local", F.to_date(F.col("datetime_local")))
            .join(F.broadcast(weekday_df), F.col("_date_local") == F.col("date"), how="left")
            .drop("_date_local", "date")
        )

        # visitors
        population_df = (
            spark.table("datathon.polenta_encoders.national_daily_population")
            .select("date", "visitors")
        )
        cluster_agg_df = (
            cluster_agg_df
            .withColumn("_date_local", F.to_date(F.col("datetime_local")))
            .join(F.broadcast(population_df), F.col("_date_local") == F.col("date"), how="left")
            .drop("_date_local", "date")
        )

        # AvgPrice (assume available in forecasts_combined_df)
        if "AvgPrice" in forecasts_combined_df.columns:
            avgprice_df = forecasts_combined_df.select(
                F.col("datetime_utc").alias("avgprice_datetime_utc"),
                F.col("AvgPrice")
            ).dropDuplicates(["avgprice_datetime_utc"])
            cluster_agg_df = cluster_agg_df.join(
                F.broadcast(avgprice_df),
                cluster_agg_df.datetime_utc == avgprice_df.avgprice_datetime_utc,
                how="left"
            ).drop("avgprice_datetime_utc")

        # weekly_trend_smooth: average of differences between lag3d/10d and lag4d/11d
        cluster_agg_df = (
            cluster_agg_df
            .withColumn("lag_3d",  F.lag("sum_active_kw", 288).over(window_base))
            .withColumn("lag_4d",  F.lag("sum_active_kw", 384).over(window_base))
            .withColumn("lag_10d", F.lag("sum_active_kw", 960).over(window_base))
            .withColumn("lag_11d", F.lag("sum_active_kw", 1056).over(window_base))
            .withColumn(
                "weekly_trend_smooth",
                (
                    (F.col("lag_3d") - F.col("lag_4d")) +
                    (F.col("lag_10d") - F.col("lag_11d"))
                ) / 2.0
            )
        )

        final_df = cluster_agg_df
        final_df = final_df.drop("community_code", "lag_3d", "lag_4d", "lag_10d", "lag_11d")
        display(final_df)

        # Train and predict consumption for each cluster, collect predictions as columns
        unique_clusters = [0, 1, 2, 3, 4]
        preds_dfs = []
        for cluster in unique_clusters:
            cluster_df = final_df.filter(final_df.cluster_id == cluster)
            cluster_df = cluster_df.drop("community_code_num")
            df_train = cluster_df.filter(cluster_df.datetime_local < predict_start)
            df_predict = cluster_df.filter(cluster_df.datetime_local >= predict_start)
            model, feature_cols = train_xgboost_model(df_train)
            preds_df = predict_xgboost(model, df_predict, feature_cols)
            preds_df = preds_df.withColumnRenamed('prediction', f'prediction_cluster_{cluster}')
            preds_dfs.append(preds_df)

            display(preds_df)

        
        

        # Join all prediction DataFrames on datetime_local and datetime_utc
        from functools import reduce
        from pyspark.sql import DataFrame

        # Join on datetime_local only, dropping extra datetime_utc columns after join
        result = reduce(
            lambda df1, df2: df1.join(
                df2.drop('datetime_utc'), ['datetime_local'], how='outer'
            ),
            preds_dfs
        ).orderBy('datetime_local')

        display(result)

        from pyspark.sql.functions import coalesce, lit, sum as Fsum

        result = result.withColumnRenamed('datetime_local', 'datetime_15min')
        prediction_cols = [c for c in result.columns if c.startswith('prediction_cluster_')]
        result = result.withColumn(
            'prediction',
            sum([coalesce(result[c], lit(0)) for c in prediction_cols])
        )
        result = result.select('datetime_15min', 'prediction').orderBy('datetime_15min')

        display(result)
        
        return result
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Submit for Scoring
# MAGIC
# MAGIC **⚠️ DO NOT CHANGE THIS CELL ⚠️**
# MAGIC
# MAGIC When you run this cell interactively, it triggers the scoring job.
# MAGIC When the scoring job re-runs this notebook, this cell generates predictions
# MAGIC and writes them for evaluation.

# COMMAND ----------

# ============================================================
# ⚠️  DO NOT CHANGE THIS CELL — submission will break  ⚠️
# ============================================================

# Provided by the organizers. Do not change.
SCORING_JOB_ID = 402405061860333  # Set automatically during setup

# --- Internal mode detection (set by the scoring job) ---
dbutils.widgets.text("mode", "interactive")
_MODE = dbutils.widgets.get("mode").strip()

if _MODE == "score":
    # ---- Score mode: generate predictions and exit ----
    from pyspark.sql import functions as _F

    _predict_start = dbutils.widgets.get("predict_start").strip()
    _predict_end = dbutils.widgets.get("predict_end").strip()

    _full_df = spark.table("datathon.shared.client_consumption")
    _model = EnergyConsumptionModel()
    _predictions = _model.predict(_full_df, _predict_start, _predict_end)

    _predictions_table = "datathon.evaluation.submissions"
    (
        _predictions
        .withColumn("team_name", _F.lit(TEAM_NAME))
        .withColumn("submitted_at", _F.current_timestamp())
        .select("team_name", "datetime_15min", "prediction", "submitted_at")
        .write.mode("overwrite").saveAsTable(_predictions_table)
    )
    print(f"Wrote {_predictions.count():,} predictions to {_predictions_table}")
    dbutils.notebook.exit("ok")

# ---- Interactive mode: trigger the scoring job ----
import json
import datetime as dt
from databricks.sdk import WorkspaceClient

assert SCORING_JOB_ID is not None, "SCORING_JOB_ID has not been set. Ask the organisers."
assert TEAM_NAME != "my_team", "Please set your TEAM_NAME in the configuration cell before submitting."

_w = WorkspaceClient()
_submitter_email = _w.current_user.me().user_name
_notebook_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)

print(f"Submitting as: {_submitter_email}")
print(f"Notebook: {_notebook_path}")

_job_run = _w.jobs.run_now(
    job_id=SCORING_JOB_ID,
    notebook_params={
        "team_name": TEAM_NAME,
        "submitter_email": _submitter_email,
        "notebook_path": _notebook_path,
    },
)
print("Job triggered. Waiting for scoring to finish (this may take a few minutes) ...")

try:
    _job_run = _job_run.result(timeout=dt.timedelta(minutes=50))
    _tasks = _w.jobs.get_run(_job_run.run_id).tasks
    _task_run_id = _tasks[0].run_id
    _output = _w.jobs.get_run_output(_task_run_id)
    _result = json.loads(_output.notebook_output.result)
except Exception as e:
    print(f"\nScoring job failed: {e}")
    _result = None

if _result and _result["status"] == "success":
    print(f"\n{'='*50}")
    print(f"  Team: {_result['team_name']}")
    print(f"  MAE:  {_result['mae']:.6f}")
    print(f"  Submissions remaining: {_result['submissions_remaining']}")
    print(f"{'='*50}")
elif _result:
    print(f"\nSubmission FAILED: {_result['message']}")
    if "submissions_remaining" in _result:
        print(f"Submissions remaining: {_result['submissions_remaining']}")