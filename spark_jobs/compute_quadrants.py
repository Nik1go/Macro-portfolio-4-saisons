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
)
""" compute_quadrants.py

Objectif :
Ce script Spark permet de déterminer dans quel quadrant économique (Q1 à Q4) se trouve chaque mois,
en analysant des indicateurs macroéconomiques comme l’inflation, le chômage, ou les spreads de crédit.

Définition des quadrants :
- Q1 : Croissance & faible inflation (environnement favorable)
- Q2 : Croissance & forte inflation
- Q3 : Récession & forte inflation (stagflation)
- Q4 : Récession & faible inflation

Étapes réalisées :
1. Lecture des indicateurs macroéconomiques à partir d’un fichier `.parquet`
2. Calcul :
   - des deltas (variations mensuelles),
   - des z-scores (anomalies par rapport à l'historique),
   - des scores de position absolue (niveau par rapport à la médiane)
   - des scores de variation (delta par rapport à l'écart type)
3. Attribution de scores à chaque quadrant en fonction de la direction de chaque indicateur :
   (ex: inflation élevée → Q2 ou Q3 / chômage faible → Q1 ou Q2, etc.)
4. Quadrant final attribué = celui avec le score total le plus élevé.
5. Export final dans un unique fichier Parquet et un fichier CSV unique, utilisables dans les étapes suivantes du pipeline.

Usage :
Le script s'utilise avec 3 arguments :
```bash
python spark_jobs/compute_quadrants.py data/Indicators.parquet data/quadrants.parquet data/quadrants.csv
 """


def write_single_file(df, output_path: str, format: str = "parquet"):
    """
    Écrit un DataFrame en un seul fichier (CSV ou Parquet) en fusionnant les partitions.
    Nettoie les fichiers temporaires automatiquement.
    """
    tmp_dir = output_path + f"_tmp_{format}"

    # 1. Nettoyage préventif
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

    # 2. Écriture partitionnée
    writer = df.coalesce(1).write.mode("overwrite")

    if format == "csv":
        writer.option("header", "true").csv(tmp_dir)
        pattern = "part-*.csv"
    else:
        writer.parquet(tmp_dir)
        pattern = "part-*.parquet"

    # 3. Récupération du fichier unique
    part_files = glob.glob(os.path.join(tmp_dir, pattern))
    if not part_files:
        raise RuntimeError(f"Erreur Spark : Aucun fichier {pattern} généré dans {tmp_dir}")

    single_part = part_files[0]

    # 4. Déplacement final (Atomique)
    if os.path.exists(output_path):
        os.remove(output_path)

    shutil.move(single_part, output_path)
    shutil.rmtree(tmp_dir)


def main(indicators_parquet_path: str, output_parquet_path: str, output_csv_path: str):
    spark = (
        SparkSession.builder
        .appName("ComputeEconomicQuadrants_Expanding")
        .getOrCreate()
    )

    # ==========================================
    # 1. CHARGEMENT
    # ==========================================
    df_ind = (
        spark.read.parquet(indicators_parquet_path)
        .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        .orderBy("date")
    )

    indicator_cols = [
        "INFLATION",
        "UNEMPLOYMENT",
        "CONSUMER_SENTIMENT",
        "High_Yield_Bond_SPREAD",
        "10-2Year_Treasury_Yield_Bond",
        "TAUX_FED",
    ]

    # ==========================================
    # 2. CALCUL DES SCORES (SANS LEAKAGE)
    # ==========================================

    # A. Fenêtre Extensive (Expanding) : Du début des temps jusqu'à "hier"
    # Sert à définir la "normalité" historique connue à l'instant T.
    window_expanding = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, -1)

    # B. Fenêtre Glissante (Rolling) : Court terme (ex: 6 mois) pour le Momentum
    window_short_term = Window.orderBy("date").rowsBetween(-120, -1)

    working_df = df_ind

    for ind in indicator_cols:
        # --- PARTIE 1 : SCORE DE POSITION (Basé sur l'historique connu) ---

        # Moyenne et Ecart-Type de tout ce qui s'est passé AVANT aujourd'hui
        working_df = (
            working_df
            .withColumn(f"{ind}_hist_mean", avg(col(ind)).over(window_expanding))
            .withColumn(f"{ind}_hist_std", stddev_samp(col(ind)).over(window_expanding))
        )

        # Z-Score Point-in-Time : (Valeur Actuelle - Moyenne Historique) / Std Historique
        # On utilise coalesce(..., lit(1)) pour éviter la division par None/Zero au tout début
        z_score_pos_col = (
                (col(ind) - col(f"{ind}_hist_mean")) /
                coalesce(col(f"{ind}_hist_std"), lit(1.0))
        )

        # Mapping Position (-2 à +2)
        working_df = working_df.withColumn(
            f"{ind}_pos_score",
            when(z_score_pos_col > 1.5, 2)
            .when(z_score_pos_col > 0.5, 1)
            .when(z_score_pos_col < -1.5, -2)
            .when(z_score_pos_col < -0.5, -1)
            .otherwise(0)
        )

        # --- PARTIE 2 : SCORE DE VARIATION (Momentum Court Terme) ---

        # Variation en % ou absolue selon la donnée (ici variation simple)
        prev_val = lag(col(ind), 1).over(Window.orderBy("date"))
        delta = (col(ind) - prev_val)  # On peut diviser par prev_val si c'est des % relatifs

        # On normalise cette variation par rapport aux 120 derniers jours
        delta_mean = avg(delta).over(window_short_term)
        delta_std = stddev_samp(delta).over(window_short_term)

        z_score_var_col = (
                (delta - delta_mean) /
                coalesce(delta_std, lit(1.0))
        )

        # Mapping Variation (-2 à +2)
        working_df = working_df.withColumn(
            f"{ind}_var_score",
            when(z_score_var_col > 2, 2)
            .when(z_score_var_col > 1, 1)
            .when(z_score_var_col < -2, -2)
            .when(z_score_var_col < -1, -1)
            .otherwise(0)
        )

        # --- COMBINAISON ---
        working_df = working_df.withColumn(
            f"{ind}_combined",
            col(f"{ind}_pos_score") + col(f"{ind}_var_score")
        )

    # Nettoyage "Warm-up" : On supprime les 2 premières années où les stats historiques
    # ne sont pas fiables (std = null ou très volatile).
    # Ajustez la date selon le début de vos données.
    working_df = working_df.filter(col(f"{indicator_cols[0]}_hist_std").isNotNull())

    # ==========================================
    # 3. ATTRIBUTION DES QUADRANTS
    # ==========================================

    # Initialisation des compteurs
    working_df = (
        working_df
        .withColumn("score_Q1", lit(0))
        .withColumn("score_Q2", lit(0))
        .withColumn("score_Q3", lit(0))
        .withColumn("score_Q4", lit(0))
    )

    # Définition des règles (Vos règles métier)
    mappings = {
        "INFLATION_combined": (["score_Q2", "score_Q3"], ["score_Q4"]),
        "UNEMPLOYMENT_combined": (["score_Q1", "score_Q2"], ["score_Q3", "score_Q4"]),
        "CONSUMER_SENTIMENT_combined": (["score_Q1", "score_Q2"], ["score_Q3", "score_Q4"]),
        "High_Yield_Bond_SPREAD_combined": (["score_Q3", "score_Q4"], ["score_Q1", "score_Q2"]),
        "10-2Year_Treasury_Yield_Bond_combined": (["score_Q1"], ["score_Q4"]),
        "TAUX_FED_combined": (["score_Q3", "score_Q4"], ["score_Q1", "score_Q2"]),
    }

    # Application des points
    for combined_col, (pos_quads, neg_quads) in mappings.items():
        # Si indicateur positif (ex: Forte Inflation) -> On ajoute des points aux quadrants concernés
        for q in pos_quads:
            working_df = working_df.withColumn(q, when(col(combined_col) > 0, col(q) + 1).otherwise(col(q)))

        # Si indicateur négatif -> On ajoute des points aux autres quadrants
        for q in neg_quads:
            working_df = working_df.withColumn(q, when(col(combined_col) < 0, col(q) + 1).otherwise(col(q)))

    # Détermination du Vainqueur
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

    # ==========================================
    # 4. SÉLECTION ET ÉCRITURE
    # ==========================================
    final_cols = [
        "date",
        *indicator_cols,
        *[f"{ind}_pos_score" for ind in indicator_cols],
        *[f"{ind}_var_score" for ind in indicator_cols],
        # *[f"{ind}_hist_mean" for ind in indicator_cols], # Debug: décommentez pour voir la moyenne évolutive
        "score_Q1", "score_Q2", "score_Q3", "score_Q4", "assigned_quadrant"
    ]

    final_df = working_df.select(*final_cols).orderBy("date")

    print("Writing Parquet...")
    write_single_file(final_df, output_parquet_path, "parquet")
    print(f"✔ Written Parquet → {output_parquet_path}")

    print("Writing CSV...")
    write_single_file(final_df, output_csv_path, "csv")
    print(f"✔ Written CSV     → {output_csv_path}")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python compute_quadrants.py <input.parquet> <output.parquet> <output.csv>")
        sys.exit(1)

    main(sys.argv[1], sys.argv[2], sys.argv[3])