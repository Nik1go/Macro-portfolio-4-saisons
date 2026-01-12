from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import yfinance as yf
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from fredapi import Fred

""" Pipeline Airflow : macro_trading_dag.py

Objectif :
Ce script définit un pipeline de données complet avec Apache Airflow pour analyser les cycles économiques.
Il récupère, nettoie, et structure des données macroéconomiques (via FRED API) et financières (via Yahoo Finance),
puis déclenche des scripts Spark pour identifier les phases du cycle économique et analyser la performance des actifs financiers.

Étapes du pipeline :
1. `fetch_data` : Récupération des données macro et financières (Yahoo Finance, FRED API)
2. `prepare_indicators_data` & `prepare_assets_data` : Agrégation des données par type (indicateurs et actifs)
3. `format_indicators_data` & `format_assets_data` : Nettoyage, interpolation, resampling et export en Parquet
4. `compute_economic_quadrants` : Exécution d'un script Spark pour classifier chaque période dans un des 4 quadrants économiques
5. `compute_assets_performance` : Évaluation des performances des actifs dans chaque quadrant économique
6. '
Outils utilisés :
- Airflow : orchestration du pipeline (avec DAG, tâches Python & Bash)
- FRED API / Yahoo Finance : extraction des données économiques et financières
- Pandas : transformation, fusion et sauvegarde des données
- Spark (via spark-submit) : traitement à grande échelle pour la modélisation quadrants + performance

Le DAG est exécuté automatiquement chaque jour à 8h (cron : `0 8 * * *`), mais peut être lancé manuellement pour test.

Structure d'enregistrement :
Les fichiers sont sauvegardés dans `~/airflow/data` :
- Données brutes dans `/backup`
- Données formatées en `.parquet`
- Résultats finaux des analyses dans `quadrants.parquet`, `assets_performance_by_quadrant.parquet`

Ce DAG constitue le cœur du projet : il gère toute la chaîne de collecte, traitement et modélisation pour construire un outil d’analyse macro-financière automatisé.

venv activate 
airflow dags trigger macro_trading_dag

"""

FRED_API_KEY = 'c4caaa1267e572ae636ff75a2a600f3d'

FRED_SERIES_MAPPING = {
    'INFLATION': 'CPIAUCSL',
    # UNEMPLOYMENT removed: redundant with INITIAL_CLAIMS
    'High_Yield_Bond_SPREAD': 'BAMLH0A0HYM2',
    '10-2Year_Treasury_Yield_Bond': 'T10Y2Y',
    'CONSUMER_SENTIMENT': 'UMCSENT',
    'TAUX_FED': 'FEDFUNDS',
    'Real_Gross_Domestic_Product': 'GDPC1', #A191RP1Q027SBEA POUR LA VAR Q
    'INITIAL_CLAIMS': 'ICSA',
    'VIX': 'VIXCLS',
    'HOUSING_PERMITS': 'PERMIT',
    'IND_PRODUCTION': 'INDPRO',
    'WTI_CRUDE_OIL': 'DCOILWTICO'  # Daily WTI Spot Price - Leading indicator for cost-push inflation
}

# ✅ Yahoo Finance INDICATORS (not assets) - Market-based leading indicators
YF_INDICATORS_MAPPING = {
    'US_DOLLAR_INDEX': {'ticker': 'DX-Y.NYB', 'series_id': 'US_DOLLAR_INDEX'},  # Dollar strength: Strong = Deflation signal
    'COPPER': {'ticker': 'HG=F', 'series_id': 'COPPER'}  # Copper Futures: Industrial demand = Growth signal
}

# ✅ Yahoo Finance ASSETS (tradable securities for portfolio)
YF_SERIES_MAPPING = {
    'S&P500(LARGE CAP)': {'ticker': 'SPY', 'series_id': 'SP500'},          # Inception 1993
    "GOLD_OZ_USD": {'ticker': 'GLD', 'series_id': 'GOLD_OZ_USD'},           # Inception 2004
    "RUSSELL2000(Small CAP)": {'ticker': 'IWM', 'series_id': 'SmallCAP'},   # Inception 2000
    "REITs(Immobilier US)": {'ticker': 'VNQ', 'series_id': 'US_REIT_VNQ'},  # Inception 2004
    'US_TREASURY_10Y': {'ticker': 'IEF', 'series_id': 'TREASURY_10Y'},      # Inception 2002
    "OBLIGATION ENTREPRISE" : { 'ticker': 'LQD', "series_id": "OBLIGATION"},# Inception 2002
    'NASDAQ_100': {'ticker': 'QQQ', 'series_id': 'NASDAQ_100'},            # Inception 1999
    'COMMODITIES': {'ticker': 'DBC', 'series_id': 'COMMODITIES'}           # Broad Commodities (Historical proxy for SXRS.DE)
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

def fetch_and_save_data(**kwargs):
    fred = Fred(api_key=FRED_API_KEY)
    base_dir = os.path.expanduser('~/airflow/data')

    # --- Données FRED (Indicators) ---
    for name, series_id in FRED_SERIES_MAPPING.items():
        backup_path = os.path.join(base_dir, 'backup', f'{name}.csv')
        os.makedirs(os.path.dirname(backup_path), exist_ok=True)

        existing_data = pd.DataFrame()
        if os.path.exists(backup_path):
            existing_data = pd.read_csv(backup_path, parse_dates=['date'])
            last_date = existing_data['date'].max()
            start_date = last_date + pd.Timedelta(days=1)
        else:
            start_date = datetime(2005, 1, 1)

        new_data = fred.get_series(series_id, observation_start=start_date)

        if not new_data.empty:
            new_df = new_data.reset_index()
            new_df.columns = ['date', 'value']
            new_df['date'] = pd.to_datetime(new_df['date']).dt.date

            if not existing_data.empty:
                existing_data['date'] = pd.to_datetime(existing_data['date']).dt.date
                combined = pd.concat([existing_data, new_df])
                combined = combined.drop_duplicates('date').sort_values('date')
            else:
                combined = new_df

            combined.to_csv(backup_path, index=False)
            print(f'Données mises à jour pour {name} ({series_id})')
        else:
            print(f'Aucune nouvelle donnée pour {name} ({series_id})')

    # --- Données Yahoo Finance INDICATORS (not assets) ---
    for name, meta in YF_INDICATORS_MAPPING.items():
        backup_path = os.path.join(base_dir, 'backup', f"{name}.csv")
        os.makedirs(os.path.dirname(backup_path), exist_ok=True)

        existing_data = pd.DataFrame()
        if os.path.exists(backup_path):
            existing_data = pd.read_csv(backup_path, parse_dates=['date'])
            last_date = existing_data['date'].max()
            start_date = last_date + pd.Timedelta(days=1)
        else:
            start_date = datetime(2005, 1, 1)

        end_date = datetime.today() - timedelta(days=1)
        start_date = min(start_date, end_date)

        if start_date.date() > end_date.date():
            print(f"Pas de nouvelles données à récupérer pour {name} ({meta['series_id']})")
            continue

        data = yf.download(meta['ticker'], start=start_date, end=end_date, progress=False, auto_adjust=True)

        if not data.empty:
            df = data[['Close']].reset_index()
            df.columns = ['date', 'value']
            df['date'] = pd.to_datetime(df['date']).dt.date

            if not existing_data.empty:
                existing_data['date'] = pd.to_datetime(existing_data['date']).dt.date
                combined = pd.concat([existing_data, df])
                combined = combined.drop_duplicates('date').sort_values('date')
            else:
                combined = df

            combined.to_csv(backup_path, index=False)
            print(f"Données indicateur mises à jour pour {name} ({meta['series_id']})")
        else:
            print(f"Aucune nouvelle donnée indicateur pour {name} ({meta['series_id']})")

    # --- Données Yahoo Finance ASSETS (tradable securities) ---
    for name, meta in YF_SERIES_MAPPING.items():
        backup_path = os.path.join(base_dir, 'backup', f"{name}.csv")
        os.makedirs(os.path.dirname(backup_path), exist_ok=True)

        existing_data = pd.DataFrame()
        if os.path.exists(backup_path):
            existing_data = pd.read_csv(backup_path, parse_dates=['date'])
            last_date = existing_data['date'].max()
            start_date = last_date + pd.Timedelta(days=1)
        else:
            start_date = datetime(2005, 1, 1)

        end_date = datetime.today() - timedelta(days=1)
        start_date = min(start_date, end_date)

        if start_date.date() > end_date.date():
            print(f"Pas de nouvelles données à récupérer pour {name} ({meta['series_id']})")
            continue

        data = yf.download(meta['ticker'], start=start_date, end=end_date, progress=False, auto_adjust=True)

        if not data.empty:
            df = data[['Close']].reset_index()
            df.columns = ['date', 'value']
            df['date'] = pd.to_datetime(df['date']).dt.date

            if not existing_data.empty:
                existing_data['date'] = pd.to_datetime(existing_data['date']).dt.date
                combined = pd.concat([existing_data, df])
                combined = combined.drop_duplicates('date').sort_values('date')
            else:
                combined = df

            combined.to_csv(backup_path, index=False)
            print(f"Données actif mises à jour pour {name} ({meta['series_id']})")
        else:
            print(f"Aucune nouvelle donnée actif pour {name} ({meta['series_id']})")

def prepare_indicators_data(base_dir):
    """Combine les indicateurs économiques (FRED + YF Indicators) en un seul DataFrame"""
    backup_dir = os.path.join(base_dir, 'backup')
    
    # ✅ FRED Indicators
    fred_indicators = [
        'INFLATION',
        # UNEMPLOYMENT removed: redundant with INITIAL_CLAIMS
        'CONSUMER_SENTIMENT',
        'High_Yield_Bond_SPREAD',
        '10-2Year_Treasury_Yield_Bond',
        'TAUX_FED',
        'Real_Gross_Domestic_Product',  # Kept for historical tracking (but not used in quadrant scoring)
        'INITIAL_CLAIMS',
        'VIX',
        'HOUSING_PERMITS',
        'IND_PRODUCTION',
        'WTI_CRUDE_OIL'  # ✅ ADDED: Oil price indicator
    ]
    
    # ✅ Yahoo Finance Indicators (market-based)
    yf_indicators = list(YF_INDICATORS_MAPPING.keys())  # US_DOLLAR_INDEX, COPPER

    combined_df = pd.DataFrame()

    # Process FRED indicators
    for indicator in fred_indicators:
        file_path = os.path.join(backup_dir, f"{indicator}.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, parse_dates=['date'])
            df = df.rename(columns={'value': indicator})

            if combined_df.empty:
                combined_df = df
            else:
                combined_df = pd.merge(combined_df, df, on='date', how='outer')
        else:
            print(f"⚠️  Fichier manquant: {file_path}")
    
    # Process Yahoo Finance indicators
    for indicator in yf_indicators:
        file_path = os.path.join(backup_dir, f"{indicator}.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, parse_dates=['date'])
            df = df.rename(columns={'value': indicator})

            if combined_df.empty:
                combined_df = df
            else:
                combined_df = pd.merge(combined_df, df, on='date', how='outer')
        else:
            print(f"⚠️  Fichier manquant: {file_path}")

    output_path = os.path.join(base_dir, 'combined_indicators.csv')
    combined_df.to_csv(output_path, index=False)
    print(f"✅ Fichier combiné des indicateurs créé: {output_path}")
    print(f"   Colonnes: {combined_df.columns.tolist()}")
    return output_path

def prepare_assets_data(base_dir):
    """Combine les actifs en un seul DataFrame"""
    backup_dir = os.path.join(base_dir, 'backup')
    assets = list(YF_SERIES_MAPPING.keys())

    combined_df = pd.DataFrame()

    for asset in assets:
        file_path = os.path.join(backup_dir, f"{asset}.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, parse_dates=['date'])
            asset_name = YF_SERIES_MAPPING[asset]['series_id']
            df = df.rename(columns={'value': asset_name})

            if combined_df.empty:
                combined_df = df
            else:
                combined_df = pd.merge(combined_df, df, on='date', how='outer')

    output_path = os.path.join(base_dir, 'combined_assets.csv')
    combined_df.to_csv(output_path, index=False)
    print(f"Fichier combiné des actifs créé: {output_path}")
    return output_path


def format_and_clean_data(base_dir, input_path, data_type):

    print(f"→ format_and_clean_data: on lit le fichier CSV : {input_path}")
    df = pd.read_csv(input_path, parse_dates=['date'])

    # 1. Nettoyage basique
    # On supprime les colonnes qui sont entièrement vides, mais on garde les lignes
    df = df.dropna(how='all', subset=df.columns.difference(['date']))

    # 2. Mise en place de l'index temporel
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    
    # 2b. GOLDEN SOURCE: Dédoublonnage strict (garde la dernière valeur si doublon)
    df = df.drop_duplicates(subset=['date'], keep='last')
    
    df = df.set_index('date')

    # 3. LE FIX EST ICI : Création d'un calendrier continu
    # Au lieu de compresser par mois, on s'assure d'avoir une ligne pour CHAQUE jour.
    # start = la toute première date dispo, end = la toute dernière date dispo
    full_idx = pd.date_range(start=df.index.min(), end=df.index.max(), freq='D')

    # On applique ce calendrier. Les jours sans données (weekends, trous) deviennent des NaN.
    # C'est parfait pour que le "Forward Fill" de Spark fonctionne ensuite.
    df = df.reindex(full_idx)

    # 4. Finalisation pour l'export
    df.index.name = 'date'
    df = df.reset_index()
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')

    # 5. Sauvegarde
    output_path = os.path.join(base_dir, f"{data_type}.parquet")
    # On garde aussi un CSV pour que tu puisses vérifier à l'oeil si besoin
    output_csv = os.path.join(base_dir, f"{data_type}.csv")

    df.to_parquet(output_path, index=False)
    df.to_csv(output_csv, index=False)

    print(f"Données {data_type} (Mode DAILY Continu) sauvegardées: {output_path}")
    print("Aperçu des 5 dernières lignes :")
    print(df.tail(5))

    return output_path


def format_and_clean_data_daily(base_dir, input_path, data_type):

    print(f"→ format_and_clean_data_daily: on lit le fichier CSV : {input_path}")

    df = pd.read_csv(input_path, parse_dates=['date'])
    print("   Colonnes lues dans df :", df.columns.tolist())
    df = df.dropna(how='all', subset=df.columns.difference(['date']))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    output_path = os.path.join(base_dir, f"{data_type}_daily.parquet")
    df.to_parquet(output_path, index=False)
    print(f"Données {data_type} journalières nettoyées sauvegardées : {output_path}")
    print(df.head(5))
    print(df.tail(5))

    return output_path

# === Configuration du DAG ===
base_dir = os.path.expanduser('~/airflow/data')

with DAG(
    dag_id='macro_trading_dag',
    default_args=default_args,
    description='Stratégie contre-cyclique avec données FRED et Yahoo Finance',
    schedule_interval='0 8 * * *',
    catchup=False,
    tags=['macro','assets','performance']
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_and_save_data
    )

    prepare_indicators_task = PythonOperator(
        task_id='prepare_indicators_data',
        python_callable=prepare_indicators_data,
        op_kwargs={'base_dir': base_dir}
    )

    prepare_assets_task = PythonOperator(
        task_id='prepare_assets_data',
        python_callable=prepare_assets_data,
        op_kwargs={'base_dir': base_dir}
    )

    format_indicators_task = PythonOperator(
        task_id='format_indicators_data',
        python_callable=format_and_clean_data,
        op_kwargs={
            'base_dir': base_dir,
            'input_path': "{{ ti.xcom_pull(task_ids='prepare_indicators_data') }}",
            'data_type': 'Indicators'
        }
    )

    format_assets_task = PythonOperator(
        task_id='format_assets_data',
        python_callable=format_and_clean_data_daily,
        op_kwargs={
            'base_dir': base_dir,
            'input_path': "{{ ti.xcom_pull(task_ids='prepare_assets_data') }}",
            'data_type': 'Assets'
        }
    )

    ASSETS_PERF_OUTPUT = os.path.join(base_dir, "assets_performance_by_quadrant.parquet")
    INDICATORS_PARQUET = os.path.join(base_dir, "Indicators.parquet")
    QUADRANT_OUTPUT = os.path.join(base_dir, "quadrants.parquet")
    QUADRANT_CSV = os.path.join(base_dir, "quadrants.csv")
    BACKTEST_OUTPUT = os.path.join(base_dir, "backtest_results")


    compute_quadrant_task = SparkSubmitOperator(
        task_id='compute_economic_quadrants',
        application="/home/leoja/airflow/spark_jobs/compute_quadrants.py",
        name="compute_economic_quadrants",
            application_args=[INDICATORS_PARQUET, QUADRANT_OUTPUT, QUADRANT_CSV],
        conn_id="spark_local",
        conf={
            "spark.pyspark.python": "/home/leoja/airflow_venv/bin/python",
            "spark.pyspark.driver.python": "/home/leoja/airflow_venv/bin/python"
        },
        verbose=False
    )

    compute_assets_performance_task = SparkSubmitOperator(
        task_id='compute_assets_performance',
        application="/home/leoja/airflow/spark_jobs/compute_assets_performance.py",
        name="compute_assets_performance",
        application_args=[
            QUADRANT_OUTPUT,
            "{{ ti.xcom_pull(task_ids='format_assets_data') }}",
            ASSETS_PERF_OUTPUT
        ],
        conn_id="spark_local",
        conf={
            "spark.pyspark.python": "/home/leoja/airflow_venv/bin/python",
            "spark.pyspark.driver.python": "/home/leoja/airflow_venv/bin/python"
        },
        verbose=False
    )

    backtest_task = SparkSubmitOperator(
        task_id='backtest_strategy',
        application="/home/leoja/airflow/spark_jobs/backtest_strategy.py",
        name="backtest_strategy",
        application_args=[
            QUADRANT_CSV,
            "{{ ti.xcom_pull(task_ids='format_assets_data') }}",
            "1000",
            BACKTEST_OUTPUT
        ],
        conn_id="spark_local",
        conf={
            "spark.pyspark.python": "/home/leoja/airflow_venv/bin/python",
            "spark.pyspark.driver.python": "/home/leoja/airflow_venv/bin/python"
        },
        verbose=False
    )
    index_to_elasticsearch = BashOperator(
        task_id='index_to_elasticsearch',
        bash_command="""
            cd ~/airflow/index_jobs && \
            source ~/airflow/airflow_venv/bin/activate && \
            python indexe.py
        """,
    )

    fetch_task >> [prepare_indicators_task, prepare_assets_task]
    prepare_indicators_task >> format_indicators_task >> compute_quadrant_task
    prepare_assets_task >> format_assets_task
    [compute_quadrant_task, format_assets_task] >> compute_assets_performance_task
    compute_assets_performance_task >> backtest_task >> index_to_elasticsearch