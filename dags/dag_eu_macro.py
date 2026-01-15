"""
dag_eu_macro.py - European Macro Data Ingestion Layer

Pipeline Airflow pour récupérer les données macro européennes.
Produit deux fichiers distincts:
- data/EU/indicators.parquet : Indicateurs (Daily YFinance + Monthly DBnomics)
- data/EU/assets_daily.parquet : Prix des ETFs tradables

Fréquence: Daily à 8h
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import yfinance as yf
import os
from dbnomics import fetch_series



# =============================================================================
# CONFIGURATION
# =============================================================================

# YFinance Tickers for INDICATORS (Real-Time Reactive)
YF_INDICATOR_TICKERS = {
    'EU_BUND_FUTURE': 'RX=F',       # Bund Future - Risk-Free/Fear proxy
    'BRENT_USD': 'BZ=F',            # Brent Crude USD (for EUR conversion)
    'COPPER_USD': 'HG=F',           # Copper USD (for EUR conversion)
    'EUR_USD_RATE': 'EURUSD=X',     # EUR/USD FX Rate
    'IHYG': 'IHYG.AS',              # iShares Euro High Yield
    'IBGL': 'IBGL.AS',              # iShares Euro Gov Bond
}

# DBnomics Series for INDICATORS (Slow Context - Monthly)
DBNOMICS_SERIES = {
    'EU_10Y_YIELD': 'Eurostat/irt_lt_mcby_m/M.MCBY.EA',
    'EU_3M_RATE': 'Eurostat/irt_st_m/M.IRT_ST.EA',
    'EU_OECD_CLI': 'OECD/MEI_CLI/EA19.LOLITOTR_GYSA.M',
    'EU_INFLATION_HICP': 'Eurostat/prc_hicp_midx/M.I15.CP00.EA',
    'EU_SENTIMENT_SERVICES': 'Eurostat/ei_bssi_m_r2/M.SERV.BS-CSMCI.SA.EA',
}

# YFinance Tickers for TRADABLE ASSETS
YF_ASSET_TICKERS = {
    'EURO_STOXX_50': 'FEZ',      # SPDR EURO STOXX 50 (Large Cap) - TER 0.29%
    'EU_SMALL_CAP': 'IEUS',     # iShares MSCI Europe Small-Cap - TER 0.40%
    'INTL_BONDS': 'BNDX',       # Vanguard Total Intl Bond Hedged - TER 0.07%
    'GOLD': 'GLD',              # SPDR Gold Shares - TER 0.40%
}

BASE_DIR = os.path.expanduser('~/airflow/data/EU')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


# =============================================================================
# TASK 1: Fetch DBnomics Data (Monthly → Daily resampled)
# =============================================================================
def fetch_dbnomics_indicators(**kwargs):
    """
    Fetch monthly macro data from DBnomics (Eurostat, OECD).
    Resample to daily and forward-fill.
    """
    os.makedirs(BASE_DIR, exist_ok=True)
    
    if fetch_series is None:
        raise ImportError("dbnomics library not installed")
    
    dbnomics_df = pd.DataFrame()
    
    for name, series_id in DBNOMICS_SERIES.items():
        try:
            print(f"Fetching DBnomics: {name} ({series_id})")
            df = fetch_series(series_id)
            
            if df.empty:
                print(f"  ⚠️ No data for {name}")
                continue
            
            # Extract date and value
            df = df[['period', 'value']].copy()
            df.columns = ['date', name]
            df['date'] = pd.to_datetime(df['date'])
            df[name] = pd.to_numeric(df[name], errors='coerce')
            df = df.set_index('date').sort_index()
            
            # Resample to daily + ffill
            df = df.resample('D').ffill()
            
            if dbnomics_df.empty:
                dbnomics_df = df
            else:
                dbnomics_df = dbnomics_df.join(df, how='outer')
                
            print(f"  ✓ {name}: {len(df)} daily rows")
            
        except Exception as e:
            print(f"  ❌ Error fetching {name}: {e}")
    
    # Calculate EU_YIELD_CURVE = 10Y - 3M
    if 'EU_10Y_YIELD' in dbnomics_df.columns and 'EU_3M_RATE' in dbnomics_df.columns:
        dbnomics_df['EU_YIELD_CURVE'] = dbnomics_df['EU_10Y_YIELD'] - dbnomics_df['EU_3M_RATE']
        print("  ✓ Calculated EU_YIELD_CURVE (10Y - 3M)")
    
    # Final ffill to handle any remaining gaps
    dbnomics_df = dbnomics_df.ffill()
    
    # Save intermediate
    output_path = os.path.join(BASE_DIR, 'dbnomics_temp.parquet')
    dbnomics_df.reset_index().to_parquet(output_path, index=False)
    print(f"✓ DBnomics indicators saved: {output_path}")
    
    return output_path


# =============================================================================
# TASK 2: Fetch YFinance Indicators (Daily)
# =============================================================================
def fetch_yfinance_indicators(**kwargs):
    """
    Fetch daily market indicators from YFinance.
    Calculate derived indicators (EUR conversions, spreads).
    """
    os.makedirs(BASE_DIR, exist_ok=True)
    
    # Fetch all tickers at once
    tickers = list(YF_INDICATOR_TICKERS.values())
    print(f"Fetching YFinance indicators: {tickers}")
    
    data = yf.download(tickers, period='max', progress=False, auto_adjust=True)['Close']
    
    if isinstance(data, pd.Series):
        data = data.to_frame()
    
    # Rename columns to our naming convention
    rename_map = {v: k for k, v in YF_INDICATOR_TICKERS.items()}
    data = data.rename(columns=rename_map)
    
    # Strip timezone
    data.index = pd.to_datetime(data.index).tz_localize(None)
    
    yf_df = pd.DataFrame(index=data.index)
    
    # Keep direct indicators
    yf_df['EU_BUND_FUTURE'] = data.get('EU_BUND_FUTURE')
    yf_df['EUR_USD_RATE'] = data.get('EUR_USD_RATE')
    
    # Calculate EUR-denominated commodities
    eurusd = data.get('EUR_USD_RATE')
    if eurusd is not None:
        brent = data.get('BRENT_USD')
        copper = data.get('COPPER_USD')
        
        if brent is not None:
            yf_df['EU_BRENT_OIL_EUR'] = brent / eurusd
            print("  ✓ Calculated EU_BRENT_OIL_EUR")
        
        if copper is not None:
            yf_df['COPPER_EUR'] = copper / eurusd
            print("  ✓ Calculated COPPER_EUR")
    
    # Calculate HY Spread Proxy (IHYG / IBGL)
    ihyg = data.get('IHYG')
    ibgl = data.get('IBGL')
    if ihyg is not None and ibgl is not None:
        yf_df['EU_HY_SPREAD_PROXY'] = ihyg / ibgl
        print("  ✓ Calculated EU_HY_SPREAD_PROXY (IHYG/IBGL)")
    
    # Clean up
    yf_df = yf_df.dropna(how='all')
    yf_df = yf_df.ffill()
    
    # Save intermediate
    output_path = os.path.join(BASE_DIR, 'yfinance_temp.parquet')
    yf_df.reset_index().rename(columns={'index': 'date', 'Date': 'date'}).to_parquet(output_path, index=False)
    print(f"✓ YFinance indicators saved: {output_path}")
    
    return output_path


# =============================================================================
# TASK 3: Merge & Save All Indicators
# =============================================================================
def merge_indicators(**kwargs):
    """
    Merge DBnomics and YFinance indicators.
    YFinance (daily trading) serves as master time axis.
    """
    ti = kwargs['ti']
    
    dbnomics_path = ti.xcom_pull(task_ids='fetch_dbnomics_indicators')
    yfinance_path = ti.xcom_pull(task_ids='fetch_yfinance_indicators')
    
    # Load data
    db_df = pd.read_parquet(dbnomics_path)
    db_df['date'] = pd.to_datetime(db_df['date'])
    db_df = db_df.set_index('date')
    
    yf_df = pd.read_parquet(yfinance_path)
    yf_df['date'] = pd.to_datetime(yf_df['date'])
    yf_df = yf_df.set_index('date')
    
    # YFinance as master (trading days), LEFT JOIN DBnomics
    combined = yf_df.join(db_df, how='left')
    
    # Final cleanup
    combined = combined.ffill()
    combined = combined.dropna()
    combined = combined.reset_index()
    combined = combined.sort_values('date')
    
    # Save final indicators
    output_path = os.path.join(BASE_DIR, 'indicators.parquet')
    combined.to_parquet(output_path, index=False)
    
    # Also save CSV for inspection
    combined.to_csv(os.path.join(BASE_DIR, 'indicators.csv'), index=False)
    
    print(f"✓ EU Indicators saved: {output_path}")
    print(f"  Shape: {combined.shape}")
    print(f"  Columns: {combined.columns.tolist()}")
    print(f"  Date range: {combined['date'].min()} → {combined['date'].max()}")
    
    # Cleanup temp files
    os.remove(dbnomics_path)
    os.remove(yfinance_path)
    
    return output_path


# =============================================================================
# TASK 4: Fetch & Save Tradable Assets
# =============================================================================
def fetch_assets(**kwargs):
    """
    Fetch daily ETF prices for tradable assets.
    Strictly separate from indicators.
    """
    os.makedirs(BASE_DIR, exist_ok=True)
    
    tickers = list(YF_ASSET_TICKERS.values())
    print(f"Fetching EU Assets: {tickers}")
    
    data = yf.download(tickers, period='max', progress=False, auto_adjust=True)['Close']
    
    if isinstance(data, pd.Series):
        data = data.to_frame()
    
    # Rename columns
    rename_map = {v: k for k, v in YF_ASSET_TICKERS.items()}
    data = data.rename(columns=rename_map)
    
    # Strip timezone
    data.index = pd.to_datetime(data.index).tz_localize(None)
    
    # Clean
    data = data.dropna(how='all')
    data = data.ffill()
    
    # Save
    output_path = os.path.join(BASE_DIR, 'assets_daily.parquet')
    data.reset_index().rename(columns={'index': 'date', 'Date': 'date'}).to_parquet(output_path, index=False)
    
    # Also CSV
    data.reset_index().to_csv(os.path.join(BASE_DIR, 'assets_daily.csv'), index=False)
    
    print(f"✓ EU Assets saved: {output_path}")
    print(f"  Shape: {data.shape}")
    print(f"  Columns: {data.columns.tolist()}")
    
    return output_path


# =============================================================================
# DAG DEFINITION
# =============================================================================
with DAG(
    dag_id='dag_eu_macro',
    default_args=default_args,
    description='European Macro Data Ingestion (DBnomics + YFinance)',
    schedule_interval='0 8 * * *',
    catchup=False,
    tags=['macro', 'europe', 'indicators', 'assets']
) as dag:

    fetch_dbnomics_task = PythonOperator(
        task_id='fetch_dbnomics_indicators',
        python_callable=fetch_dbnomics_indicators
    )

    fetch_yfinance_task = PythonOperator(
        task_id='fetch_yfinance_indicators',
        python_callable=fetch_yfinance_indicators
    )

    merge_indicators_task = PythonOperator(
        task_id='merge_indicators',
        python_callable=merge_indicators
    )

    fetch_assets_task = PythonOperator(
        task_id='fetch_assets',
        python_callable=fetch_assets
    )

    # DAG Dependencies
    # Indicators branch: DBnomics + YFinance → Merge
    [fetch_dbnomics_task, fetch_yfinance_task] >> merge_indicators_task
    
    # Assets branch: Independent
    fetch_assets_task


# =============================================================================
# SUGGESTED WEIGHTS FOR compute_quadrants_eu.py
# =============================================================================
"""
INDICATOR_WEIGHTS = {
    # MAJOR SIGNALS (1.5x) - Real-time stress/rate indicators
    'EU_HY_SPREAD_PROXY_combined': 1.5,   # Credit Stress (HY vs Gov spread)
    'EU_BUND_FUTURE_combined': 1.5,       # Rate/Fear Signal (Bund price = inverse yield)
    
    # LEADING (1.2x) - Inflation, Industrial, Currency context
    'EU_BRENT_OIL_EUR_combined': 1.2,     # Energy Inflation (EUR denominated)
    'COPPER_EUR_combined': 1.2,           # Industrial Demand (EUR denominated)
    'EUR_USD_RATE_combined': 1.2,         # Currency Strength (imported inflation)
    'EU_YIELD_CURVE_combined': 1.2,       # Recession Signal (10Y - 3M)
    
    # LAGGING (1.0x) - Background trend context
    'EU_OECD_CLI_combined': 1.0,          # Leading Economic Indicator (but lagged publication)
    'EU_INFLATION_HICP_combined': 1.0,    # Official Inflation (very lagged)
    'EU_SENTIMENT_SERVICES_combined': 1.0 # Services Confidence
}

# Quadrant Mappings for EU:
# Q1 (Growth): CLI↑, Sentiment↑, Copper↑, HY_Spread↓
# Q2 (Inflation): Oil↑, HICP↑, EUR/USD↓, Yield_Curve↑
# Q3 (Stagflation): Oil↑, HICP↑, CLI↓, HY_Spread↑, Bund↑
# Q4 (Deflation/Crash): Bund↑↑, HY_Spread↑↑, Yield_Curve↓, CLI↓
"""
