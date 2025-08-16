import os
import sys
import shutil
import pandas as pd
import numpy as np

"""
backtest_strategy.py

Ce script réalise un backtest mensuel de la stratégie de rotation d’actifs basée sur les quadrants
économiques. Il prend en compte les frais de gestion (TER) mensuels et les frais de transaction 
basés sur le turnover mensuel du portefeuille.
Usage :
    backtest_strategy.py <quadrants.csv> <Assets_daily.parquet> <capital_initial> <dossier_sortie>
"""


TRANSACTION_COST = 0.0035  # 0.35% par montant réalloué
TER = {
    'SP500': 0.0007,
    'GOLD_OZ_USD': 0.0039,
    'SmallCAP': 0.0033,
    'US_REIT_VNQ': 0.0059,
    'TREASURY_10Y': 0.0020,
    'OBLIGATION': 0.0014
}

def compute_monthly_returns_from_parquet(parquet_path, assets):
    df = pd.read_parquet(parquet_path)
    df['date'] = pd.to_datetime(df['date'])
    monthly = df.set_index('date').resample('ME').last().ffill()
    rets = monthly[assets].pct_change().rename(columns=lambda c: f"{c}_ret")
    rets.index = rets.index.to_period('M').to_timestamp()
    return rets

def max_drawdown(wealth_series):
    rm = wealth_series.cummax()
    return ((rm - wealth_series) / rm).max()

def stats_from_series(ret_series, wealth_series):
    mean_r = ret_series.mean()
    std_r = ret_series.std(ddof=1)
    sharpe_m = mean_r / std_r if std_r > 0 else np.nan
    sharpe_a = sharpe_m * np.sqrt(12)
    vol_a = std_r * np.sqrt(12)
    md = max_drawdown(wealth_series)
    avg_yr = mean_r * 12
    return vol_a, sharpe_a, md, avg_yr

def main():
    if len(sys.argv) != 5:
        print("Usage: backtest_strategy.py <quadrants.csv> <Assets_daily.parquet> <initial_capital> <output_dir>")
        sys.exit(1)

    quadrants_csv, assets_parquet, initial_capital, output_dir = sys.argv[1:]
    initial_capital = float(initial_capital)

    if os.path.isdir(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    df_q = pd.read_csv(quadrants_csv, parse_dates=['date'])
    df_q['year_month'] = df_q['date'].dt.to_period('M').dt.to_timestamp()
    df_q = df_q.set_index('year_month')['assigned_quadrant']

    assets = ['SP500', 'GOLD_OZ_USD', 'SmallCAP', 'US_REIT_VNQ', 'TREASURY_10Y']
    df_rets = compute_monthly_returns_from_parquet(assets_parquet, assets)

    # Trend following setup
    df_daily = pd.read_parquet(assets_parquet)
    df_daily['date'] = pd.to_datetime(df_daily['date'])
    df_daily = df_daily.set_index('date').sort_index()

    df_daily['SP500_MA120'] = df_daily['SP500'].rolling(50).mean()
    df_daily['GOLD_MA120'] = df_daily['GOLD_OZ_USD'].rolling(50).mean()

    df_daily['SP500_alert'] = df_daily['SP500'] < df_daily['SP500_MA120']
    df_daily['SP500_alert'] = df_daily['SP500_alert'].rolling(5).sum() >= 5

    df_daily['GOLD_alert'] = df_daily['GOLD_OZ_USD'] < df_daily['GOLD_MA120']
    df_daily['GOLD_alert'] = df_daily['GOLD_alert'].rolling(5).sum() >= 5

    trend_alerts = df_daily[['SP500_alert', 'GOLD_alert']].resample('M').last()
    trend_alerts.index = trend_alerts.index.to_period('M').to_timestamp()

    weights = {
        1: {'SmallCAP': 0.5, 'SP500': 0.4, 'US_REIT_VNQ': 0.1},
        2: {'TREASURY_10Y': 0.5, 'GOLD_OZ_USD': 0.4, 'OBLIGATION': 0.1},
        3: {'GOLD_OZ_USD': 0.5, 'SP500': 0.4, 'OBLIGATION': 0.1},
        4: {'GOLD_OZ_USD': 0.5, 'SmallCAP': 0.4, 'TREASURY_10Y': 0.1},
    }

    records = []
    prev_alloc = None
    cum_transaction_cost = 0.0
    cum_ter_cost = 0.0
    nb_switch_gold = 0
    nb_switch_sp500 = 0


    for ym, quad in df_q.items():
        row = {'year_month': ym, 'quadrant': quad}
        row['SP500_ret'] = df_rets.at[ym, 'SP500_ret'] if ym in df_rets.index else np.nan
        row['GOLD_OZ_USD_ret'] = df_rets.at[ym, 'GOLD_OZ_USD_ret'] if ym in df_rets.index else np.nan

        if ym in df_rets.index:
            rents = df_rets.loc[ym]
            alloc = weights.get(int(quad), {}).copy()
            port_ret = 0.0
            ter_cost = 0.0


            # Trend Following Adjustments
            if ym in trend_alerts.index:
                if trend_alerts.loc[ym, 'SP500_alert'] and 'SP500' in alloc:
                    weight = alloc.pop('SP500')
                    alloc['TREASURY_10Y'] = alloc.get('TREASURY_10Y', 0.0) + weight
                    nb_switch_sp500 += 1  # log


                if trend_alerts.loc[ym, 'GOLD_alert'] and 'GOLD_OZ_USD' in alloc:
                    weight = alloc.pop('GOLD_OZ_USD')
                    alloc['TREASURY_10Y'] = alloc.get('TREASURY_10Y', 0.0) + weight
                    nb_switch_gold += 1  # log

            for asset, weight in alloc.items():
                ret = rents.get(f"{asset}_ret", 0.0)
                monthly_ter = TER.get(asset, 0.0) / 12
                ter_cost += weight * monthly_ter
                port_ret += weight * (ret - monthly_ter)

            cum_ter_cost += ter_cost

            if prev_alloc is not None:
                all_assets = set(alloc.keys()).union(prev_alloc.keys())
                turnover = sum(abs(alloc.get(asset, 0.0) - prev_alloc.get(asset, 0.0)) for asset in all_assets)
                tc = turnover * TRANSACTION_COST
                port_ret -= tc
                cum_transaction_cost += tc

            prev_alloc = alloc
        else:
            port_ret = 0.0

        row['portfolio_return'] = port_ret
        records.append(row)

    df_bt = pd.DataFrame(records)
    df_bt['date'] = df_bt['year_month'].dt.to_period('M').dt.end_time.dt.strftime('%Y-%m-%d')
    df_bt['date'] = pd.to_datetime(df_bt['date'], format='%Y-%m-%d')
    df_bt = df_bt.set_index('date').sort_index()

    df_bt['wealth'] = initial_capital * (1 + df_bt['portfolio_return']).cumprod()
    df_bt['SP500_wealth'] = initial_capital * (1 + df_bt['SP500_ret']).cumprod()
    df_bt['GOLD_wealth'] = initial_capital * (1 + df_bt['GOLD_OZ_USD_ret']).cumprod()

    stats = {}
    for label, (rcol, wcol) in {
        'strategy': ('portfolio_return', 'wealth'),
        'SP500': ('SP500_ret', 'SP500_wealth'),
        'GOLD': ('GOLD_OZ_USD_ret', 'GOLD_wealth')
    }.items():
        vol_a, sharpe_a, md, avg_yr = stats_from_series(df_bt[rcol], df_bt[wcol])
        stats[f"{label}_vol_annual"] = vol_a
        stats[f"{label}_sharpe_annual"] = sharpe_a
        stats[f"{label}_max_drawdown"] = md
        stats[f"{label}_avg_year_return"] = avg_yr

    stats["cum_transaction_cost"] = float(cum_transaction_cost)
    stats["cum_ter_cost"] = float(cum_ter_cost)
    stats["nb_switch_sp500"] = nb_switch_sp500
    stats["nb_switch_gold"] = nb_switch_gold

    print("Export des stats :", stats)

    pd.DataFrame([stats]).to_csv(f"{output_dir}/backtest_stats.csv", index=False)

    df_bt = df_bt.reset_index()
    df_bt = df_bt[['date', 'year_month', 'quadrant', 'SP500_ret', 'GOLD_OZ_USD_ret',
                   'portfolio_return', 'wealth', 'SP500_wealth', 'GOLD_wealth']]
    df_bt = df_bt.set_index('date')
    df_bt.to_csv(f"{output_dir}/backtest_timeseries.csv", date_format='%Y-%m-%d')

    print("Backtest terminé. Stats :", stats)

if __name__ == "__main__":
    main()