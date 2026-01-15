import os
import sys
import shutil
import pandas as pd
import numpy as np
from scipy import stats as sp_stats

"""
backtest_strategy.py - Daily Native Version (Vectorized)
Core Strategy:
1. Macro Smoothing: 20-day Rolling Mode for stable quadrant allocation.
2. Trend Following: MA150 overlay for SP500/GOLD with 5-day streak confirmation.
3. Transaction Costs: 0.10% on any allocation change.
4. Annualized Stats: 252 trading days.

dans le venv active  
python spark_jobs/backtest_strategy.py data/US/output_dag/quadrants.csv data/US/output_dag/Assets_daily.parquet 1000 data/US/backtest_results
"""

TRANSACTION_COST = 0.0010  # 0.10%
TRADING_DAYS = 252
MA_WINDOW = 200 # MA200 for trend following
SMOOTH_WINDOW = 18# Rolling mode window for quadrant smoothing
#optimiser pour arriver a une planche de performance entre 16 et 20j
N_STREAK = 5  # Days below/above MA to trigger action

# UCITS ETFs TER (Total Expense Ratio) - Real costs for European investors
TER = {
    'SP500': 0.0007,         # CSPX: 0.07% 
    'GOLD_OZ_USD': 0.0012,   # IGLN: 0.12%
    'SmallCAP': 0.0030,      # R2US: 0.30%
    'US_REIT_VNQ': 0.0040,   # IUSP: 0.40%
    'TREASURY_10Y': 0.0007,  # IBB1: 0.07%
    'OBLIGATION': 0.0020,    # LQDE: 0.20%
    'NASDAQ_100': 0.0030,    # EQQQ: 0.30%
    'COMMODITIES': 0.0019    # SXRS.DE: 0.19% (Broad Commodities: Energy + Agriculture + Metals)
}

# Allocations Optimisées par Quadrant (Data-Driven 2005-2025)
# Basé sur l'analyse des heatmaps de performance et Sharpe ratios historiques
ALLOCATIONS = {
    # Q1 (Croissance Saine): ON FONCE.
    # Ici, ta logique est bonne. On prend du risque (Beta).
    1: {
        'NASDAQ_100': 0.4,      # Moteur de performance
        'SmallCAP': 0.3,        # Beta élevé
        'SP500': 0.3,           # Stabilité relative
        'US_REIT_VNQ': 0.0, 
        'GOLD_OZ_USD': 0.0, 
        'TREASURY_10Y': 0.0, 
        'OBLIGATION': 0.0, 
        'COMMODITIES': 0.0
    },
    
    # Q2 (Inflation): QUALITÉ & RÉEL.
    # On vire les SmallCaps (trop fragiles) et on réduit la Tech (taux).
    # On mise sur les grosses boîtes (SP500) et les matières premières.
    2: {
        'SP500': 0.4,           # Pricing Power (McDo monte ses prix, pas la start-up)
        'GOLD_OZ_USD': 0.3,     # Protection Monétaire
        'COMMODITIES': 0.2,     # Hedge Inflation direct
        'NASDAQ_100': 0.1,      # On garde un pied dans la tech, mais léger
        'SmallCAP': 0.0,        # Trop risqué avec les taux
        'TREASURY_10Y': 0.0, 
        'US_REIT_VNQ': 0.0,
        'OBLIGATION': 0.0
    },
    
    # Q3 (Stagflation): DÉFENSE TOTALE.
    # On enlève l'Immo (REITs). On ne garde que ce qui survit au chaos.
    3: {
        'GOLD_OZ_USD': 0.6,     # L'actif roi en Q3
        'COMMODITIES': 0.2,     # La cause de l'inflation
        'TREASURY_10Y': 0.2,    # Sécurité (Volatilité faible)
        'NASDAQ_100': 0.0,
        'SP500': 0.0,
        'SmallCAP': 0.0, 
        'US_REIT_VNQ': 0.0,     # OUT
        'OBLIGATION': 0.0
    },
    
    # Q4 (Crash Déflationniste): BUNKER.
    # On enlève les Obligations Corporate. On veut du sans risque.
    4: {
        'TREASURY_10Y': 0.6,    # Profite de la baisse des taux
        'GOLD_OZ_USD': 0.4,     # Peur / Valeur Refuge
        'SP500': 0.0, 
        'NASDAQ_100': 0.0, 
        'SmallCAP': 0.0, 
        'US_REIT_VNQ': 0.0, 
        'OBLIGATION': 0.0,      # OUT (Risque de deffault de crédit)
        'COMMODITIES': 0.0
    },
}

WEIGHTS = ALLOCATIONS

ASSETS = ['SP500', 'GOLD_OZ_USD', 'SmallCAP', 'US_REIT_VNQ', 'OBLIGATION', 'TREASURY_10Y', 'NASDAQ_100', 'COMMODITIES']


def rolling_mode(series, window):
    """Calculate rolling mode (most frequent value in window)."""

    def mode_func(x):
        if len(x) == 0 or x.isna().all():
            return np.nan
        mode_result = sp_stats.mode(x.dropna(), keepdims=True)
        return mode_result.mode[0] if len(mode_result.mode) > 0 else np.nan

    return series.rolling(window, min_periods=1).apply(mode_func, raw=False)


def max_drawdown(wealth_series):
    """Calculate maximum drawdown."""
    running_max = wealth_series.cummax()
    drawdown = (running_max - wealth_series) / running_max
    return drawdown.max()


def calculate_stats(returns, wealth, label):
    """Calculate annualized stats."""
    mean_ret = returns.mean()
    std_ret = returns.std(ddof=1)
    sharpe_annual = (mean_ret / std_ret) * np.sqrt(TRADING_DAYS) if std_ret > 0 else np.nan
    vol_annual = std_ret * np.sqrt(TRADING_DAYS)
    md = max_drawdown(wealth)
    avg_year_ret = mean_ret * TRADING_DAYS
    return {
        f"{label}_vol_annual": vol_annual,
        f"{label}_sharpe_annual": sharpe_annual,
        f"{label}_max_drawdown": md,
        f"{label}_avg_year_return": avg_year_ret
    }


def main():
    if len(sys.argv) != 5:
        print("Usage: backtest_strategy.py <quadrants.csv> <Assets_daily.parquet> <initial_capital> <output_dir>")
        sys.exit(1)

    quadrants_csv, assets_parquet, initial_capital, output_dir = sys.argv[1:]
    initial_capital = float(initial_capital)

    if os.path.isdir(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # ========== 1. LOAD & DEDUPLICATE ==========
    df_q = pd.read_csv(quadrants_csv, parse_dates=['date'])
    df_q = df_q.drop_duplicates(subset=['date']).set_index('date').sort_index()

    df_a = pd.read_parquet(assets_parquet)
    df_a['date'] = pd.to_datetime(df_a['date'])
    df_a = df_a.drop_duplicates(subset=['date']).set_index('date').sort_index()

    # ========== 2. INNER JOIN ==========
    df = df_a[ASSETS].join(df_q[['assigned_quadrant']], how='inner')
    df = df.dropna(subset=['assigned_quadrant'])
    df['assigned_quadrant'] = df['assigned_quadrant'].astype(int)

    # ========== 3. DAILY RETURNS ==========
    for asset in ASSETS:
        df[f'{asset}_ret'] = df[asset].pct_change().fillna(0.0)

    # ========== 4. MACRO SMOOTHING: 20-Day Rolling Mode ==========
    df['smooth_quadrant'] = rolling_mode(df['assigned_quadrant'], SMOOTH_WINDOW).ffill().astype(int)

    # ========== 5. BASE ALLOCATION FROM SMOOTH QUADRANT ==========
    for asset in ASSETS:
        df[f'{asset}_base_weight'] = df['smooth_quadrant'].map(lambda q: WEIGHTS.get(q, {}).get(asset, 0.0))

    # ========== 6. TREND FOLLOWING OVERLAY (MA150 + 5-Day Streak) ==========
    # Applied to SP500, GOLD_OZ_USD, and NASDAQ_100 for downside protection
    for asset in ['SP500', 'GOLD_OZ_USD', 'NASDAQ_100']:
        df[f'{asset}_MA'] = df[asset].rolling(MA_WINDOW, min_periods=1).mean()

        # Below MA streak
        below = (df[asset] < df[f'{asset}_MA']).astype(int)
        df[f'{asset}_below_streak'] = below.rolling(N_STREAK, min_periods=N_STREAK).sum() >= N_STREAK

        # Above MA streak
        above = (df[asset] > df[f'{asset}_MA']).astype(int)
        df[f'{asset}_above_streak'] = above.rolling(N_STREAK, min_periods=N_STREAK).sum() >= N_STREAK

    # ========== 7. RISK-OFF STATE MACHINE (Vectorized) ==========
    # For SP500, GOLD, and NASDAQ: track risk_off state using expanding logic
    for asset in ['SP500', 'GOLD_OZ_USD', 'NASDAQ_100']:
        risk_off = pd.Series(False, index=df.index)

        # We need to iterate here due to the state machine nature
        # But we'll do it efficiently with numpy
        below_streak = df[f'{asset}_below_streak'].values
        above_streak = df[f'{asset}_above_streak'].values
        base_weight = df[f'{asset}_base_weight'].values
        quadrant_changed = df['smooth_quadrant'].diff().fillna(0).values != 0

        risk_off_arr = np.zeros(len(df), dtype=bool)

        for i in range(1, len(df)):
            # Quadrant changed -> reset risk_off, follow new quadrant rules
            if quadrant_changed[i]:
                risk_off_arr[i] = False
            # Currently risk_off, check if can go back risk_on
            elif risk_off_arr[i - 1]:
                if above_streak[i] and base_weight[i] > 0:
                    risk_off_arr[i] = False  # Back to risk_on
                else:
                    risk_off_arr[i] = True  # Stay risk_off
            # Currently risk_on, check if need to go risk_off
            else:
                if below_streak[i] and base_weight[i] > 0:
                    risk_off_arr[i] = True
                else:
                    risk_off_arr[i] = False

        df[f'{asset}_risk_off'] = risk_off_arr

    # ========== 8. FINAL WEIGHTS (After Trend Overlay) ==========
    for asset in ASSETS:
        df[f'{asset}_weight'] = df[f'{asset}_base_weight'].copy()

    # Apply risk-off: move weight to Treasury
    treasury_boost = pd.Series(0.0, index=df.index)
    for asset in ['SP500', 'GOLD_OZ_USD', 'NASDAQ_100']:
        risk_off_mask = df[f'{asset}_risk_off']
        weight_to_move = df.loc[risk_off_mask, f'{asset}_weight'].copy()
        df.loc[risk_off_mask, f'{asset}_weight'] = 0.0
        treasury_boost.loc[risk_off_mask] += weight_to_move

    df['TREASURY_10Y_weight'] = df['TREASURY_10Y_weight'] + treasury_boost

    # ========== 9. TRANSACTION COSTS ==========
    weight_cols = [f'{a}_weight' for a in ASSETS]
    turnover = df[weight_cols].diff().abs().sum(axis=1).fillna(0.0)
    df['transaction_cost'] = turnover * TRANSACTION_COST

    # ========== 10. TER COSTS (Daily) ==========
    ter_daily = pd.Series(0.0, index=df.index)
    for asset in ASSETS:
        ter_daily += df[f'{asset}_weight'] * (TER.get(asset, 0.0) / TRADING_DAYS)
    df['ter_cost'] = ter_daily

    # ========== 11. PORTFOLIO RETURN ==========
    df['portfolio_return'] = 0.0
    for asset in ASSETS:
        df['portfolio_return'] += df[f'{asset}_weight'] * df[f'{asset}_ret']

    # Subtract costs
    df['portfolio_return'] = df['portfolio_return'] - df['ter_cost'] - df['transaction_cost']

    # ========== 12. WEALTH SERIES ==========
    df['wealth'] = initial_capital * (1 + df['portfolio_return']).cumprod()
    df['SP500_wealth'] = initial_capital * (1 + df['SP500_ret']).cumprod()
    df['GOLD_wealth'] = initial_capital * (1 + df['GOLD_OZ_USD_ret']).cumprod()

    # ========== 13. STATS ==========
    stats = {}
    stats.update(calculate_stats(df['portfolio_return'], df['wealth'], 'strategy'))
    stats.update(calculate_stats(df['SP500_ret'], df['SP500_wealth'], 'SP500'))
    stats.update(calculate_stats(df['GOLD_OZ_USD_ret'], df['GOLD_wealth'], 'GOLD'))

    stats['cum_transaction_cost'] = df['transaction_cost'].sum()
    stats['cum_ter_cost'] = df['ter_cost'].sum()
    stats['initial_capital'] = initial_capital
    stats['final_wealth'] = df['wealth'].iloc[-1]
    stats['total_return'] = (df['wealth'].iloc[-1] / initial_capital) - 1

    # Count risk-off switches (including NASDAQ-100)
    for asset in ['SP500', 'GOLD_OZ_USD', 'NASDAQ_100']:
        stats[f'nb_switch_{asset.lower()}'] = df[f'{asset}_risk_off'].diff().fillna(0).abs().sum() / 2

    # ========== 14. EXPORT ==========
    pd.DataFrame([stats]).to_csv(f"{output_dir}/backtest_stats.csv", index=False)

    # Timeseries
    out_cols = ['smooth_quadrant', 'portfolio_return', 'wealth', 'SP500_wealth', 'GOLD_wealth',
                'transaction_cost', 'ter_cost'] + weight_cols
    df_out = df[out_cols].copy()
    df_out.index.name = 'date'
    df_out.to_csv(f"{output_dir}/backtest_timeseries.csv")

    # Costs breakdown
    df_costs = df[['transaction_cost', 'ter_cost']].copy()
    df_costs['cum_transaction_cost'] = df_costs['transaction_cost'].cumsum()
    df_costs['cum_ter_cost'] = df_costs['ter_cost'].cumsum()
    df_costs.to_csv(f"{output_dir}/backtest_costs.csv")

    print(f"Backtest terminé. Final Wealth: {stats['final_wealth']:.2f}, Sharpe: {stats['strategy_sharpe_annual']:.2f}")
    print(f"Stats: {stats}")


if __name__ == "__main__":
    main()