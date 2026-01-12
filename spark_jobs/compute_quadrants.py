import sys
import os
import shutil
import glob
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    lag,
    avg,
    stddev_samp,
    when,
    expr,
    to_date,
    greatest,
    lit,
    coalesce,
    last,
    count,
    abs
)

""" compute_quadrants.py

Objectif :
- Determiner le quadrant economique (Q1-Q4) pour CHAQUE jour (Continu).
- Gestion des donnees 'Sparse' (Macro sort 1x/mois) via Forward Fill.
- Gestion des Lags en 'Jours de Trading' (lignes).

python spark_jobs/compute_quadrants.py data/Indicators.parquet data/quadrants.parquet data/quadrants.csv

"""

def write_single_file(df, output_path: str, format: str = "parquet"):
    """
    Ecrit un DataFrame en un seul fichier (CSV ou Parquet) en fusionnant les partitions.
    """
    tmp_dir = output_path + f"_tmp_{format}"

    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

    writer = df.coalesce(1).write.mode("overwrite")

    if format == "csv":
        writer.option("header", "true").csv(tmp_dir)
        pattern = "part-*.csv"
    else:
        writer.parquet(tmp_dir)
        pattern = "part-*.parquet"

    part_files = glob.glob(os.path.join(tmp_dir, pattern))
    if not part_files:
        raise RuntimeError(f"Erreur Spark : Aucun fichier {pattern} dans {tmp_dir}")

    single_part = part_files[0]

    if os.path.exists(output_path):
        os.remove(output_path)

    shutil.move(single_part, output_path)
    shutil.rmtree(tmp_dir)


def main(indicators_parquet_path: str, output_parquet_path: str, output_csv_path: str):
    spark = (
        SparkSession.builder
        .appName("ComputeEconomicQuadrants_Daily_V2")
        .getOrCreate()
    )

    # 1. LECTURE (Golden Source = déjà dédupliquée par le DAG)
    df_ind = (
        spark.read.parquet(indicators_parquet_path)
        .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        .orderBy("date")
    )

    # Liste complete de tous les indicateurs
    # REMOVALS: UNEMPLOYMENT (redundant with INITIAL_CLAIMS), Real_Gross_Domestic_Product (too lagging, not useful for quadrants)
    # ADDITIONS: WTI_CRUDE_OIL, COPPER, US_DOLLAR_INDEX (should be in Indicators.parquet, not Assets)
    all_indicators = [
        "INFLATION",
        "CONSUMER_SENTIMENT",
        "INITIAL_CLAIMS",
        "HOUSING_PERMITS",
        "IND_PRODUCTION",
        "High_Yield_Bond_SPREAD",
        "10-2Year_Treasury_Yield_Bond",
        "TAUX_FED",
        "VIX",
        "US_DOLLAR_INDEX",  # Dollar strength: Strong = Deflation, Weak = Inflation
        "WTI_CRUDE_OIL",     # Oil spike = Inflation signal
        "COPPER"             # Industrial demand = Growth signal
    ]

    # Filtrer pour ne garder que ceux qui existent vraiment dans le fichier
    existing_cols = [c for c in all_indicators if c in df_ind.columns]
    
    # 2. FORWARD FILL GLOBAL (Boucher les trous)
    # Important : On propage la derniere valeur connue pour TOUT (Macro + Market le week-end)
    window_ff = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)

    for c in existing_cols:
        df_ind = df_ind.withColumn(c, last(col(c), ignorenulls=True).over(window_ff))

    # 3. LAGS (en jours de trading / lignes)
    lags_config = {
        "INFLATION": 18,
        "CONSUMER_SENTIMENT": 10,
        "INITIAL_CLAIMS": 5,
        "HOUSING_PERMITS": 18,
        "IND_PRODUCTION": 15,
        # VIX, Bond Spread, WTI, COPPER, USD, etc. = 0 (Real Time - market data)
    }

    window_lag = Window.orderBy("date")

    for col_name, lag_rows in lags_config.items():
        if col_name in existing_cols:
            df_ind = df_ind.withColumn(col_name, lag(col(col_name), lag_rows).over(window_lag))

    # 4. CALCULS DES SCORES (Expanding & Momentum)
    
    # Historique depuis le debut (pour le nivau absolu)
    window_expanding = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, -1)
    
    # Momentum 6 mois (approx 126 jours ouvres) pour normaliser la variation
    window_momentum_norm = Window.orderBy("date").rowsBetween(-126, -1)
    
    working_df = df_ind

    cols_to_score = [c for c in existing_cols if c in df_ind.columns] # Securite

    for ind in cols_to_score:
        # SCORE DE POSITION
        working_df = (
            working_df
            .withColumn(f"{ind}_hist_mean", avg(col(ind)).over(window_expanding))
            .withColumn(f"{ind}_hist_std", stddev_samp(col(ind)).over(window_expanding))
        )

        z_score_pos = (
            (col(ind) - col(f"{ind}_hist_mean")) /
            coalesce(col(f"{ind}_hist_std"), lit(1.0))
        )

        working_df = working_df.withColumn(
            f"{ind}_pos_score",
            when(z_score_pos > 1.5, 2)
            .when(z_score_pos > 0.5, 1)
            .when(z_score_pos < -1.5, -2)
            .when(z_score_pos < -0.5, -1)
            .otherwise(0)
        )

        # --- B. SCORE DE VARIATION (Momentum) ---
        # On garde la logique "Momentum 1 Mois" (approx 20 jours de trading)
        # Delta = Valeur_J - Valeur_J-20
        prev_val = lag(col(ind), 20).over(Window.orderBy("date"))
        delta = col(ind) - prev_val

        # Normalisation par rapport a la volatilite du Delta sur 6 mois (126 jours)
        delta_mean = avg(delta).over(window_momentum_norm)
        delta_std = stddev_samp(delta).over(window_momentum_norm)

        z_score_var = (
            (delta - delta_mean) /
            coalesce(delta_std, lit(1.0))
        )

        working_df = working_df.withColumn(
            f"{ind}_var_score",
            when(z_score_var > 2, 2)
            .when(z_score_var > 1, 1)
            .when(z_score_var < -2, -2)
            .when(z_score_var < -1, -1)
            .otherwise(0)
        )

        #  OFFICIEL COMBINED
        working_df = working_df.withColumn(
            f"{ind}_combined",
            col(f"{ind}_pos_score") + col(f"{ind}_var_score")
        )

    # Nettoyage Warm-up (2 ans approx = 500 jours ouvres)
    # working_df = working_df.filter(col(f"{existing_cols[0]}_hist_std").isNotNull())

    # 5. ATTRIBUTION DES QUADRANTS
    working_df = (
        working_df
        .withColumn("score_Q1", lit(0))
        .withColumn("score_Q2", lit(0))
        .withColumn("score_Q3", lit(0))
        .withColumn("score_Q4", lit(0))
    )

    # Regles de Scoring (Incluant les nouveaux indicateurs)
    mappings = {
        "INFLATION_combined": (["score_Q2", "score_Q3"], ["score_Q1", "score_Q4"]),  # High Inflation = Q2 or Q3, Low = Q1 or Q4
        "CONSUMER_SENTIMENT_combined": (["score_Q1", "score_Q2"], ["score_Q3", "score_Q4"]),
        "High_Yield_Bond_SPREAD_combined": (["score_Q3", "score_Q4"], ["score_Q1", "score_Q2"]),
        "10-2Year_Treasury_Yield_Bond_combined": (["score_Q1"], ["score_Q4"]),
        "TAUX_FED_combined": (["score_Q3", "score_Q4"], ["score_Q1", "score_Q2"]),
        "INITIAL_CLAIMS_combined": (["score_Q3", "score_Q4"], ["score_Q1", "score_Q2"]),  # Rising = Bad (Labor Market Weakness)
        "VIX_combined": (["score_Q4"], ["score_Q1", "score_Q2"]),  # Rising = Fear
        "HOUSING_PERMITS_combined": (["score_Q1", "score_Q2"], ["score_Q3", "score_Q4"]),  # Rising = Growth
        "IND_PRODUCTION_combined": (["score_Q1", "score_Q2"], ["score_Q3", "score_Q4"]),  # Rising = Growth
        "US_DOLLAR_INDEX_combined": (["score_Q1", "score_Q4"], ["score_Q2", "score_Q3"]),  # Dollar Index
        "WTI_CRUDE_OIL_combined": (["score_Q2", "score_Q3"], ["score_Q1", "score_Q4"]),  # Oil spike
        "COPPER_combined": (["score_Q1", "score_Q2"], ["score_Q3", "score_Q4"]),  # Industrial demand
    }
    
    # ✅ AMÉLIORATION: Poids des Indicateurs (Rééquilibrage DOUX)
    # Poids trop élevés (2.0) = modèle paranoïaque (33% Q4)
    # Poids doux (1.2/1.1) = tie-breakers subtils sans distorsion
    INDICATOR_WEIGHTS = {
        # LEADING (1.2): +20% comme tie-breaker
        "10-2Year_Treasury_Yield_Bond_combined": 1.2,
        "VIX_combined": 1.2,
        "WTI_CRUDE_OIL_combined": 1.2,
        
        # COINCIDENT (1.1): +10% légère prime
        "US_DOLLAR_INDEX_combined": 1.1,
        "COPPER_combined": 1.1,
        "HOUSING_PERMITS_combined": 1.1,
        "High_Yield_Bond_SPREAD_combined": 1.1,
        
        # LAGGING (1.0): Base fondamentale
        "INFLATION_combined": 1.0,
        "INITIAL_CLAIMS_combined": 1.0,
        "CONSUMER_SENTIMENT_combined": 1.0,
        "TAUX_FED_combined": 1.0,
        "IND_PRODUCTION_combined": 1.0
    }

    # ✅ Application des scores avec pondération ET intensité du signal
    for combined_col, (pos_quads, neg_quads) in mappings.items():
        # Verifier si la colonne existe (au cas ou)
        if combined_col not in working_df.columns:
            continue
        
        # Récupérer le poids de l'indicateur (défaut = 1.0 si non spécifié)
        weight = INDICATOR_WEIGHTS.get(combined_col, 1.0)

        for q in pos_quads:
            # Multiplier le poids par le score combined pour intensité du signal
            working_df = working_df.withColumn(
                q, 
                when(col(combined_col) > 0, col(q) + lit(weight) * col(combined_col)).otherwise(col(q))
            )
        
        for q in neg_quads:
            # Multiplier le poids par la valeur absolue du score combined
            working_df = working_df.withColumn(
                q, 
                when(col(combined_col) < 0, col(q) + lit(weight) * abs(col(combined_col))).otherwise(col(q))
            )

    working_df = (
        working_df
        .withColumn(
            "max_score",
            greatest(col("score_Q1"), col("score_Q2"), col("score_Q3"), col("score_Q4"))
        )
        .withColumn(
            "assigned_quadrant",
            when(col("score_Q1") == col("max_score"), 1)
            .when(col("score_Q2") == col("max_score"), 2)
            .when(col("score_Q3") == col("max_score"), 3)
            .otherwise(4)
        )
    )

    # 6. ECRIRE
    print("Writing Parquet...")
    write_single_file(working_df, output_parquet_path, "parquet")
    print(f"✔ Written Parquet → {output_parquet_path}")

    print("Writing CSV...")
    write_single_file(working_df, output_csv_path, "csv")
    print(f"✔ Written CSV     → {output_csv_path}")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python compute_quadrants.py <input.parquet> <output.parquet> <output.csv>")
        sys.exit(1)

    main(sys.argv[1], sys.argv[2], sys.argv[3])
