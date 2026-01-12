
import os
import sys
import pandas as pd
import numpy as np

""" compute_assets_performance.py

Objectif :
Ce script Spark (exécuté via `spark-submit`) analyse la performance des actifs financiers
en fonction des quadrants économiques (croissance/inflation/déflation/récession) détectés auparavant.

Il calcule, pour chaque combinaison (actif, quadrant) :
- le rendement mensuel moyen,
- le max drawdown (perte maximale en période de repli),
- le Sharpe ratio annualisé (mesure du couple rendement/risque ajusté de la volatilité).

Étapes principales :
1. Chargement du fichier des quadrants économiques (`quadrant_file`, CSV ou Parquet).
2. Chargement du fichier des valeurs quotidiennes des actifs (`assets_file`, Parquet).
3. Transformation des données au format "long" pour faciliter les calculs par actif/quadrant.
4. Calculs statistiques :
   - Rendement mensuel par actif/quadrant
   - Drawdown maximum
   - Ratio de Sharpe (annualisé)
5. Sauvegarde du tableau synthétique dans un fichier `.parquet` + `.csv`

Exemple de sortie :
| asset_id | assigned_quadrant | monthly_return | max_drawdown | sharpe_annualized |
|----------|-------------------|----------------|--------------|-------------------|
| SP500    | 2 (Stagflation)   | +3.2%          | -8.7%        | 1.12              |

Usage :
Ce script attend 3 arguments :
```bash
python spark_jobs/compute_assets_performance.py data/quadrants.parquet data/Assets_daily.parquet data/assets_performance_by_quadrant.parquet
"""
def main():
    if len(sys.argv) != 4:
        print("Usage: spark-submit compute_assets_performance.py "
              "<quadrant_file> <assets_file> <output_parquet>")
        sys.exit(1)
    quadrant_file  = sys.argv[1]
    assets_file    = sys.argv[2]
    output_parquet = sys.argv[3]
    parent_out = os.path.dirname(output_parquet)
    if parent_out and not os.path.isdir(parent_out):
        os.makedirs(parent_out, exist_ok=True)

    ext_q = os.path.splitext(quadrant_file)[1].lower()
    if ext_q == '.parquet':
        df_quadrant = pd.read_parquet(quadrant_file)
    elif ext_q in ('.csv', '.txt'):
        df_quadrant = pd.read_csv(quadrant_file, parse_dates=['date'])
    else:
        raise ValueError(f"Extension non supportée pour quadrant_file → {ext_q!r}")

    if 'assigned_quadrant' not in df_quadrant.columns:
        raise KeyError("La colonne 'assigned_quadrant' est absente.")

    df_quadrant['date'] = pd.to_datetime(df_quadrant['date'])
    # ✅ On garde les quadrants DAILY - pas d'agrégation mensuelle!
    df_q = df_quadrant[['date', 'assigned_quadrant']].copy()

    df_assets_wide = pd.read_parquet(assets_file)
    if 'date' not in df_assets_wide.columns:
        raise KeyError("La colonne 'date' est absente de Assets_daily.parquet.")
    asset_columns = [c for c in df_assets_wide.columns if c != 'date']
    
    if len(asset_columns) == 0:
        raise ValueError("Aucune colonne d'actif détectée (hormis 'date').")
    
    print(f"[compute_assets_performance] Actifs analysés : {asset_columns}")

    # Transformation en format long (une ligne par actif-date)
    df_long = df_assets_wide.melt(
        id_vars=['date'],
        value_vars=asset_columns,
        var_name='asset_id',
        value_name='close'
    ).dropna(subset=['close'])

    df_long['date'] = pd.to_datetime(df_long['date'])
    
    # Calculer les rendements AVANT le merge
    # Garantit que pct_change() utilise des dates continues (pas de trous de dates)
    df_long = df_long.sort_values(['asset_id', 'date'])
    df_long['ret'] = df_long.groupby('asset_id')['close'].pct_change()
    
    # Chaque rendement journalier est attribué au quadrant du jour
    df_merged = pd.merge(
        left=df_long,
        right=df_q,
        on='date',
        how='inner'
    )

    #  Calculs de performance par (actif, quadrant) sur base DAILY
    rows = []
    grouped = df_merged.groupby(['asset_id', 'assigned_quadrant'])

    for (asset, quadrant), sub in grouped:
        sub = sub.sort_values('date')
        daily_ret = sub['ret'].dropna()
        
        if len(daily_ret) < 2:  # Besoin d'au moins 2 points pour stats
            continue

        # Stats sur rendements journaliers
        mean_daily_ret = daily_ret.mean()
        std_daily_ret = daily_ret.std()
        
        # ✅ Annualisation (base 252 jours de trading)
        annual_return = mean_daily_ret * 252
        annual_vol = std_daily_ret * np.sqrt(252)
        sharpe_annual = (mean_daily_ret / std_daily_ret) * np.sqrt(252) if std_daily_ret > 0 else np.nan

        # ✅ Max Drawdown sur la série de richesse simulée
        cumprod = (1 + daily_ret).cumprod()
        rolling_max = cumprod.cummax()
        drawdown = (cumprod - rolling_max) / rolling_max
        max_dd = drawdown.min()

        rows.append({
            'asset': asset,
            'quadrant': int(quadrant),
            'annual_return': annual_return,
            'sharpe': sharpe_annual,
            'max_drawdown': -max_dd,
            'nb_days': len(daily_ret)  # Nombre de jours dans ce quadrant pour cet actif
        })

    df_summary = pd.DataFrame(rows)

    df_summary.to_parquet(output_parquet, index=False)
    print(f"[compute_assets_performance] Parquet écrit → {output_parquet}")
    out_csv = os.path.splitext(output_parquet)[0] + ".csv"
    df_summary.to_csv(out_csv, index=False)
    print(f"[compute_assets_performance] CSV écrit     → {out_csv}")


if __name__ == "__main__":
    main()
