"""
optimize_window.py - Sensitivity Analysis on ROLLING_WINDOW Parameter

Loops through different rolling window sizes and computes performance metrics 
to determine optimal macro smoothing lag.

Usage (in venv):
python spark_jobs/optimize_window.py data/quadrants.csv data/Assets_daily.parquet 1000

"""

import sys
import pandas as pd
import numpy as np
from scipy import stats as sp_stats

# Constants from backtest_strategy.py
TRANSACTION_COST = 0.0010  # 0.10%
TRADING_DAYS = 252
MA_WINDOW = 150  
N_STREAK = 5 

# Windows to test
WINDOWS = [21,20,22, 19, 23, 18, 24,17,15,14,13,16]

TER = {
    'SP500': 0.0007,
    'GOLD_OZ_USD': 0.0012,
    'SmallCAP': 0.0030,
    'US_REIT_VNQ': 0.0040,
    'TREASURY_10Y': 0.0007,
    'OBLIGATION': 0.0020,
    'NASDAQ_100': 0.0030,
    'COMMODITIES': 0.0019
}

ALLOCATIONS = {
    1: {'NASDAQ_100': 0.4, 'SmallCAP': 0.3, 'SP500': 0.3, 'US_REIT_VNQ': 0.0, 
        'GOLD_OZ_USD': 0.0, 'TREASURY_10Y': 0.0, 'OBLIGATION': 0.0, 'COMMODITIES': 0.0},
    2: {'SP500': 0.4, 'GOLD_OZ_USD': 0.3, 'COMMODITIES': 0.2, 'NASDAQ_100': 0.1,
        'SmallCAP': 0.0, 'TREASURY_10Y': 0.0, 'US_REIT_VNQ': 0.0, 'OBLIGATION': 0.0},
    3: {'GOLD_OZ_USD': 0.6, 'COMMODITIES': 0.2, 'TREASURY_10Y': 0.2, 'NASDAQ_100': 0.0,
        'SP500': 0.0, 'SmallCAP': 0.0, 'US_REIT_VNQ': 0.0, 'OBLIGATION': 0.0},
    4: {'TREASURY_10Y': 0.6, 'GOLD_OZ_USD': 0.4, 'SP500': 0.0, 'NASDAQ_100': 0.0,
        'SmallCAP': 0.0, 'US_REIT_VNQ': 0.0, 'OBLIGATION': 0.0, 'COMMODITIES': 0.0},
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


def run_backtest_for_window(df_base, window, initial_capital):
    """
    Run the full backtest with a specific rolling window.
    Returns a dict with: window, total_return, sharpe, max_dd
    """
    df = df_base.copy()
    
    # Macro Smoothing with specified window
    df['smooth_quadrant'] = rolling_mode(df['assigned_quadrant'], window).ffill().astype(int)
    
    # Base Allocation
    for asset in ASSETS:
        df[f'{asset}_base_weight'] = df['smooth_quadrant'].map(lambda q: WEIGHTS.get(q, {}).get(asset, 0.0))
    
    # Trend Following Overlay (MA150 + 5-Day Streak)
    for asset in ['SP500', 'GOLD_OZ_USD', 'NASDAQ_100']:
        df[f'{asset}_MA'] = df[asset].rolling(MA_WINDOW, min_periods=1).mean()
        below = (df[asset] < df[f'{asset}_MA']).astype(int)
        df[f'{asset}_below_streak'] = below.rolling(N_STREAK, min_periods=N_STREAK).sum() >= N_STREAK
        above = (df[asset] > df[f'{asset}_MA']).astype(int)
        df[f'{asset}_above_streak'] = above.rolling(N_STREAK, min_periods=N_STREAK).sum() >= N_STREAK
    
    # Risk-Off State Machine
    for asset in ['SP500', 'GOLD_OZ_USD', 'NASDAQ_100']:
        below_streak = df[f'{asset}_below_streak'].values
        above_streak = df[f'{asset}_above_streak'].values
        base_weight = df[f'{asset}_base_weight'].values
        quadrant_changed = df['smooth_quadrant'].diff().fillna(0).values != 0
        
        risk_off_arr = np.zeros(len(df), dtype=bool)
        for i in range(1, len(df)):
            if quadrant_changed[i]:
                risk_off_arr[i] = False
            elif risk_off_arr[i - 1]:
                if above_streak[i] and base_weight[i] > 0:
                    risk_off_arr[i] = False
                else:
                    risk_off_arr[i] = True
            else:
                if below_streak[i] and base_weight[i] > 0:
                    risk_off_arr[i] = True
                else:
                    risk_off_arr[i] = False
        
        df[f'{asset}_risk_off'] = risk_off_arr
    
    # Final Weights
    for asset in ASSETS:
        df[f'{asset}_weight'] = df[f'{asset}_base_weight'].copy()
    
    treasury_boost = pd.Series(0.0, index=df.index)
    for asset in ['SP500', 'GOLD_OZ_USD', 'NASDAQ_100']:
        risk_off_mask = df[f'{asset}_risk_off']
        weight_to_move = df.loc[risk_off_mask, f'{asset}_weight'].copy()
        df.loc[risk_off_mask, f'{asset}_weight'] = 0.0
        treasury_boost.loc[risk_off_mask] += weight_to_move
    
    df['TREASURY_10Y_weight'] = df['TREASURY_10Y_weight'] + treasury_boost
    
    # Transaction Costs
    weight_cols = [f'{a}_weight' for a in ASSETS]
    turnover = df[weight_cols].diff().abs().sum(axis=1).fillna(0.0)
    df['transaction_cost'] = turnover * TRANSACTION_COST
    
    # TER Costs
    ter_daily = pd.Series(0.0, index=df.index)
    for asset in ASSETS:
        ter_daily += df[f'{asset}_weight'] * (TER.get(asset, 0.0) / TRADING_DAYS)
    df['ter_cost'] = ter_daily
    
    # Portfolio Return
    df['portfolio_return'] = 0.0
    for asset in ASSETS:
        df['portfolio_return'] += df[f'{asset}_weight'] * df[f'{asset}_ret']
    
    df['portfolio_return'] = df['portfolio_return'] - df['ter_cost'] - df['transaction_cost']
    
    # Wealth Series
    df['wealth'] = initial_capital * (1 + df['portfolio_return']).cumprod()
    
    # Compute Stats
    returns = df['portfolio_return']
    wealth = df['wealth']
    
    mean_ret = returns.mean()
    std_ret = returns.std(ddof=1)
    sharpe_annual = (mean_ret / std_ret) * np.sqrt(TRADING_DAYS) if std_ret > 0 else np.nan
    total_return = (wealth.iloc[-1] / initial_capital) - 1
    mdd = max_drawdown(wealth)
    
    # Count quadrant changes (trades)
    nb_quadrant_changes = df['smooth_quadrant'].diff().fillna(0).abs().sum()
    
    return {
        'Window': window,
        'Return': f"{total_return * 100:.1f}%",
        'Sharpe': round(sharpe_annual, 2),
        'MaxDD': f"-{mdd * 100:.1f}%",
        'Total_Return_Raw': total_return,
        'Quadrant_Changes': int(nb_quadrant_changes)
    }


def main():
    if len(sys.argv) != 4:
        print("Usage: optimize_window.py <quadrants.csv> <Assets_daily.parquet> <initial_capital>")
        sys.exit(1)
    
    quadrants_csv, assets_parquet, initial_capital = sys.argv[1:]
    initial_capital = float(initial_capital)
    
    # Load Data
    df_q = pd.read_csv(quadrants_csv, parse_dates=['date'])
    df_q = df_q.drop_duplicates(subset=['date']).set_index('date').sort_index()
    
    df_a = pd.read_parquet(assets_parquet)
    df_a['date'] = pd.to_datetime(df_a['date'])
    df_a = df_a.drop_duplicates(subset=['date']).set_index('date').sort_index()
    
    # Inner Join
    df = df_a[ASSETS].join(df_q[['assigned_quadrant']], how='inner')
    df = df.dropna(subset=['assigned_quadrant'])
    df['assigned_quadrant'] = df['assigned_quadrant'].astype(int)
    
    # Daily Returns
    for asset in ASSETS:
        df[f'{asset}_ret'] = df[asset].pct_change().fillna(0.0)
    
    print(f"\n{'='*60}")
    print("SENSITIVITY ANALYSIS: Rolling Window for Macro Smoothing")
    print(f"{'='*60}\n")
    print(f"Testing windows: {WINDOWS}")
    print(f"Initial Capital: {initial_capital:,.0f}")
    print(f"Date Range: {df.index.min().date()} to {df.index.max().date()}")
    print(f"Total Trading Days: {len(df)}\n")
    
    # Run backtest for each window
    results = []
    for window in WINDOWS:
        print(f"  Running backtest with window = {window}...", end=" ")
        result = run_backtest_for_window(df, window, initial_capital)
        results.append(result)
        print(f"Done. Sharpe: {result['Sharpe']}")
    
    # Create DataFrame and sort by Sharpe
    df_results = pd.DataFrame(results)
    df_results = df_results.sort_values('Sharpe', ascending=False)
    
    # Display Results
    print(f"\n{'='*60}")
    print("RESULTS (Sorted by Sharpe Ratio)")
    print(f"{'='*60}\n")
    
    display_cols = ['Window', 'Return', 'Sharpe', 'MaxDD', 'Quadrant_Changes']
    print(df_results[display_cols].to_string(index=False))
    
    # Best window recommendation
    best = df_results.iloc[0]
    print(f"\n{'='*60}")
    print(f"RECOMMENDATION: Window = {int(best['Window'])} days")
    print(f"  - Total Return: {best['Return']}")
    print(f"  - Sharpe Ratio: {best['Sharpe']}")
    print(f"  - Max Drawdown: {best['MaxDD']}")
    print(f"{'='*60}\n")
    
    # Save to CSV
    output_file = "data/sensitivity_window.csv"
    df_results[display_cols].to_csv(output_file, index=False)
    print(f"Results saved to: {output_file}")


if __name__ == "__main__":
    main()
