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
airflow_venv source bin/activate 
python spark_jobs/backtest_strategy.py data/quadrants.csv data/Assets_daily.parquet 1000 data/backtest_results
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

TRANSACTION_COST = 0.0035  # 0.35% par montant réalloué
TRADING_DAYS = 252
MA_WINDOW = int(os.getenv('MA_WINDOW', '120'))
N_BELOW_RISK_OFF = int(os.getenv('N_BELOW_RISK_OFF', '5'))
N_ABOVE_RISK_ON = int(os.getenv('N_ABOVE_RISK_ON', '5'))

def compute_monthly_returns_from_parquet(parquet_path, assets):
    df = pd.read_parquet(parquet_path)
    df['date'] = pd.to_datetime(df['date'])
    monthly = df.set_index('date').resample('M').last().ffill()
    rets = monthly[assets].pct_change().rename(columns=lambda c: f"{c}_ret")
    rets.index = rets.index.to_period('M').to_timestamp()
    return rets

def compute_daily_returns(parquet_path, assets):
    df = pd.read_parquet(parquet_path)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date').sort_index()
    daily = df[assets].pct_change().fillna(0.0)
    return daily

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

def run_backtest_daily(df_q, assets_parquet, initial_capital, output_dir):
    assets = ['SP500', 'GOLD_OZ_USD', 'SmallCAP', 'US_REIT_VNQ', 'OBLIGATION', 'TREASURY_10Y']
    daily_returns = compute_daily_returns(assets_parquet, assets)

    df_daily = pd.read_parquet(assets_parquet)
    df_daily['date'] = pd.to_datetime(df_daily['date'])
    df_daily = df_daily.set_index('date').sort_index()

    # Trend following uniquement pour SP500 et GOLD_OZ_USD avec MA150
    for a in ['SP500', 'GOLD_OZ_USD']:
        df_daily[f'{a}_MA'] = df_daily[a].rolling(MA_WINDOW).mean()
        below = (df_daily[a] < df_daily[f'{a}_MA']).astype(int)
        above = (df_daily[a] > df_daily[f'{a}_MA']).astype(int)
        df_daily[f'{a}_below_streak'] = below.rolling(N_BELOW_RISK_OFF).sum() >= N_BELOW_RISK_OFF
        df_daily[f'{a}_above_streak'] = above.rolling(N_ABOVE_RISK_ON).sum() >= N_ABOVE_RISK_ON

    weights = {
        1: {'SmallCAP': 0.5, 'SP500': 0.4, 'US_REIT_VNQ': 0.1},
        2: {'TREASURY_10Y': 0.5, 'GOLD_OZ_USD': 0.4, 'OBLIGATION': 0.1},
        3: {'GOLD_OZ_USD': 0.5, 'SP500': 0.4, 'OBLIGATION': 0.1},
        4: {'GOLD_OZ_USD': 0.5, 'SmallCAP': 0.4, 'TREASURY_10Y': 0.1},
    }

    dynamic_alloc = {a: 0.0 for a in assets}
    risk_off_state = {a: False for a in ['SP500', 'GOLD_OZ_USD']}
    current_month = None
    base_alloc = None

    month_portfolio_daily = {}
    month_sp500_daily = {}
    month_gold_daily = {}
    month_ter_sum = {}
    month_tc_sum = {}
    month_tf_counts = {}
    month_tf_costs = {}
    month_quadrant_tc = {}

    cum_transaction_cost = 0.0
    cum_ter_cost = 0.0
    nb_switch_sp500 = 0
    nb_switch_gold = 0

    for dt, row in daily_returns.iterrows():
        ym = pd.Timestamp(dt.year, dt.month, 1)
        if current_month is None:
            current_month = ym
        if base_alloc is None or ym != current_month:
            current_month = ym
            quad = int(df_q.get(ym, df_q.iloc[0])) if len(df_q) > 0 else 1
            base_alloc = weights.get(quad, {})
            desired = {}
            # Trend following seulement pour SP500 et GOLD_OZ_USD
            for a in ['SP500','GOLD_OZ_USD']:
                desired[a] = 0.0 if risk_off_state[a] else base_alloc.get(a, 0.0)
            # Autres actifs sans trend following
            for a in ['SmallCAP','US_REIT_VNQ','OBLIGATION']:
                desired[a] = base_alloc.get(a, 0.0)
            desired['TREASURY_10Y'] = 1.0 - sum(desired.values())
            all_keys = set(dynamic_alloc.keys()).union(desired.keys())
            turnover = sum(abs(desired.get(k,0.0) - dynamic_alloc.get(k,0.0)) for k in all_keys)
            quadrant_tc = turnover * TRANSACTION_COST
            for k in all_keys:
                dynamic_alloc[k] = desired.get(k,0.0)
            cum_transaction_cost += quadrant_tc
            month_tc_sum[current_month] = month_tc_sum.get(current_month, 0.0) + quadrant_tc
            month_quadrant_tc[current_month] = month_quadrant_tc.get(current_month, 0.0) + quadrant_tc

        tf_counts_today = {'SP500':0,'GOLD_OZ_USD':0}
        tf_costs_today = {k:0.0 for k in tf_counts_today}
        # Trend following uniquement pour SP500 et GOLD_OZ_USD
        for a, below_col, above_col in [
            ('SP500', 'SP500_below_streak', 'SP500_above_streak'),
            ('GOLD_OZ_USD', 'GOLD_OZ_USD_below_streak', 'GOLD_OZ_USD_above_streak'),
        ]:
            below_ok = bool(df_daily.loc[dt, below_col]) if below_col in df_daily.columns else False
            above_ok = bool(df_daily.loc[dt, above_col]) if above_col in df_daily.columns else False
            if (not risk_off_state[a]) and below_ok and dynamic_alloc.get(a,0.0) > 0:
                w = dynamic_alloc[a]
                dynamic_alloc[a] = 0.0
                dynamic_alloc['TREASURY_10Y'] = dynamic_alloc.get('TREASURY_10Y',0.0) + w
                tc = 2 * w * TRANSACTION_COST
                cum_transaction_cost += tc
                month_tc_sum[current_month] = month_tc_sum.get(current_month, 0.0) + tc
                tf_counts_today[a] += 1
                tf_costs_today[a] += tc
                risk_off_state[a] = True
                if a=='SP500': nb_switch_sp500 += 1
                elif a=='GOLD_OZ_USD': nb_switch_gold += 1
            elif risk_off_state[a] and above_ok:
                target = base_alloc.get(a, 0.0)
                delta = max(0.0, target - dynamic_alloc.get(a,0.0))
                if delta > 0 and dynamic_alloc.get('TREASURY_10Y',0.0) >= delta:
                    dynamic_alloc[a] += delta
                    dynamic_alloc['TREASURY_10Y'] -= delta
                    tc = 2 * delta * TRANSACTION_COST
                    cum_transaction_cost += tc
                    month_tc_sum[current_month] = month_tc_sum.get(current_month, 0.0) + tc
                    tf_counts_today[a] += 1
                    tf_costs_today[a] += tc
                    risk_off_state[a] = False

        tf_counts = month_tf_counts.get(current_month, {'SP500':0,'GOLD_OZ_USD':0})
        tf_costs = month_tf_costs.get(current_month, {'SP500':0.0,'GOLD_OZ_USD':0.0})
        for k in tf_counts:
            tf_counts[k] += tf_counts_today[k]
            tf_costs[k] += tf_costs_today[k]
        month_tf_counts[current_month] = tf_counts
        month_tf_costs[current_month] = tf_costs

        ter_day = 0.0
        for a,w in dynamic_alloc.items():
            ter_day += w * (TER.get(a, 0.0) / TRADING_DAYS)
        port_ret_day = sum(dynamic_alloc[a]*row.get(a,0.0) for a in assets) - ter_day
        cum_ter_cost += ter_day

        mp = month_portfolio_daily.get(current_month, [])
        mp.append(port_ret_day)
        month_portfolio_daily[current_month] = mp
        ms = month_sp500_daily.get(current_month, [])
        ms.append(row.get('SP500',0.0))
        month_sp500_daily[current_month] = ms
        mg = month_gold_daily.get(current_month, [])
        mg.append(row.get('GOLD_OZ_USD',0.0))
        month_gold_daily[current_month] = mg
        month_ter_sum[current_month] = month_ter_sum.get(current_month, 0.0) + ter_day

    rows = []
    wealth = initial_capital
    sp500_wealth = initial_capital
    gold_wealth = initial_capital
    for ym in sorted(month_portfolio_daily.keys()):
        pr = np.prod([1+x for x in month_portfolio_daily[ym]]) - 1
        sp = np.prod([1+x for x in month_sp500_daily[ym]]) - 1
        go = np.prod([1+x for x in month_gold_daily[ym]]) - 1
        wealth *= (1+pr)
        sp500_wealth *= (1+sp)
        gold_wealth *= (1+go)
        quad = int(df_q.get(ym, df_q.iloc[0])) if len(df_q) > 0 else 1
        rows.append({
            'year_month': ym,
            'quadrant': quad,
            'SP500_ret': sp,
            'GOLD_OZ_USD_ret': go,
            'portfolio_return': pr,
            'wealth': wealth,
            'SP500_wealth': sp500_wealth,
            'GOLD_wealth': gold_wealth,
        })
    df_bt = pd.DataFrame(rows)
    df_bt['date'] = df_bt['year_month'].dt.to_period('M').dt.end_time.dt.strftime('%Y-%m-%d')
    df_bt['date'] = pd.to_datetime(df_bt['date'], format='%Y-%m-%d')
    df_bt = df_bt.set_index('date').sort_index()

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

    stats["cum_transaction_cost"] = float(sum(month_tc_sum.values()))
    stats["cum_ter_cost"] = float(sum(month_ter_sum.values()))
    stats["nb_switch_sp500"] = nb_switch_sp500
    stats["nb_switch_gold"] = nb_switch_gold

    pd.DataFrame([stats]).to_csv(f"{output_dir}/backtest_stats.csv", index=False)

    out_ts = df_bt.reset_index()[['date','year_month','quadrant','SP500_ret','GOLD_OZ_USD_ret','portfolio_return','wealth','SP500_wealth','GOLD_wealth']]
    out_ts = out_ts.set_index('date')
    out_ts.to_csv(f"{output_dir}/backtest_timeseries.csv", date_format='%Y-%m-%d')

    cost_rows = []
    for ym in sorted(month_portfolio_daily.keys()):
        tfs = month_tf_counts.get(ym, {'SP500':0,'GOLD_OZ_USD':0})
        tfc = month_tf_costs.get(ym, {'SP500':0.0,'GOLD_OZ_USD':0.0})
        cost_rows.append({
            'year_month': ym,
            'monthly_ter_cost': month_ter_sum.get(ym,0.0),
            'monthly_transaction_cost': month_tc_sum.get(ym,0.0),
            'monthly_quadrant_transaction_cost': month_quadrant_tc.get(ym,0.0),
            'switches_SP500': tfs['SP500'],
            'switches_GOLD_OZ_USD': tfs['GOLD_OZ_USD'],
            'switch_cost_SP500': tfc['SP500'],
            'switch_cost_GOLD_OZ_USD': tfc['GOLD_OZ_USD'],
        })
    df_costs = pd.DataFrame(cost_rows)
    df_costs['date'] = df_costs['year_month'].dt.to_period('M').dt.end_time.dt.strftime('%Y-%m-%d')
    df_costs['date'] = pd.to_datetime(df_costs['date'], format='%Y-%m-%d')
    df_costs = df_costs.sort_values('date')
    df_costs['cum_ter_cost'] = df_costs['monthly_ter_cost'].cumsum()
    df_costs['cum_transaction_cost'] = df_costs['monthly_transaction_cost'].cumsum()
    for k in ['SP500','GOLD_OZ_USD']:
        df_costs[f'cum_switch_cost_{k}'] = df_costs[f'switch_cost_{k}'].cumsum()
    df_costs = df_costs.set_index('date')
    df_costs.to_csv(f"{output_dir}/backtest_costs.csv", date_format='%Y-%m-%d')

    print("Backtest terminé. Stats :", stats)

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

    assets = ['SP500', 'GOLD_OZ_USD', 'SmallCAP', 'US_REIT_VNQ', 'OBLIGATION', 'TREASURY_10Y']
    df_rets = compute_monthly_returns_from_parquet(assets_parquet, assets)

    # Trend following setup
    df_daily = pd.read_parquet(assets_parquet)
    df_daily['date'] = pd.to_datetime(df_daily['date'])
    df_daily = df_daily.set_index('date').sort_index()

    # Trend following uniquement pour SP500 et GOLD_OZ_USD avec MA150
    df_daily['SP500_MA150'] = df_daily['SP500'].rolling(120).mean()
    df_daily['GOLD_MA150'] = df_daily['GOLD_OZ_USD'].rolling(120).mean()

    df_daily['SP500_alert'] = df_daily['SP500'] < df_daily['SP500_MA150']
    df_daily['SP500_alert'] = df_daily['SP500_alert'].rolling(5).sum() >= 5

    df_daily['GOLD_alert'] = df_daily['GOLD_OZ_USD'] < df_daily['GOLD_MA150']
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

    cost_records = []
    cum_tf_switch_costs = {
        'SP500': 0.0,
        'GOLD_OZ_USD': 0.0,
    }


    for ym, quad in df_q.items():
        row = {'year_month': ym, 'quadrant': quad}
        row['SP500_ret'] = df_rets.at[ym, 'SP500_ret'] if ym in df_rets.index else np.nan
        row['GOLD_OZ_USD_ret'] = df_rets.at[ym, 'GOLD_OZ_USD_ret'] if ym in df_rets.index else np.nan

        monthly_tf_switch_counts = {'SP500': 0, 'GOLD_OZ_USD': 0}
        monthly_tf_switch_costs = {'SP500': 0.0, 'GOLD_OZ_USD': 0.0}

        if ym in df_rets.index:
            rents = df_rets.loc[ym]
            alloc = weights.get(int(quad), {}).copy()
            port_ret = 0.0
            ter_cost = 0.0


            # Trend Following Adjustments (uniquement SP500 et GOLD_OZ_USD)
            if ym in trend_alerts.index:
                # attribute TF switch costs per asset (not deducted twice from returns)
                def tf_switch(asset_key, alert_col):
                    nonlocal nb_switch_sp500, nb_switch_gold
                    if trend_alerts.loc[ym, alert_col] and asset_key in alloc:
                        w = alloc.pop(asset_key)
                        alloc['TREASURY_10Y'] = alloc.get('TREASURY_10Y', 0.0) + w
                        monthly_tf_switch_counts[asset_key] += 1
                        cost = 2 * w * TRANSACTION_COST
                        monthly_tf_switch_costs[asset_key] += cost
                        cum_tf_switch_costs[asset_key] += cost
                        if asset_key == 'SP500':
                            nb_switch_sp500 += 1
                        elif asset_key == 'GOLD_OZ_USD':
                            nb_switch_gold += 1

                tf_switch('SP500', 'SP500_alert')
                tf_switch('GOLD_OZ_USD', 'GOLD_alert')

            for asset, weight in alloc.items():
                ret = rents.get(f"{asset}_ret", np.nan)
                if pd.isna(ret):
                    ret = 0.0
                monthly_ter = TER.get(asset, 0.0) / 12
                ter_cost += weight * monthly_ter
                port_ret += weight * (ret - monthly_ter)

            cum_ter_cost += ter_cost

            monthly_transaction_cost = 0.0
            if prev_alloc is not None:
                all_assets = set(alloc.keys()).union(prev_alloc.keys())
                turnover = sum(abs(alloc.get(asset, 0.0) - prev_alloc.get(asset, 0.0)) for asset in all_assets)
                tc = turnover * TRANSACTION_COST
                port_ret -= tc
                cum_transaction_cost += tc
                monthly_transaction_cost = tc

            prev_alloc = alloc
        else:
            port_ret = 0.0

        row['portfolio_return'] = port_ret
        records.append(row)

        # collect monthly costs/switches
        monthly_ter_cost = float(ter_cost) if ('ter_cost' in locals() and ym in df_rets.index) else 0.0
        cost_row = {
            'year_month': ym,
            'monthly_ter_cost': monthly_ter_cost,
            'monthly_transaction_cost': float(monthly_transaction_cost) if 'monthly_transaction_cost' in locals() else 0.0,
            'cum_ter_cost': float(cum_ter_cost),
            'cum_transaction_cost': float(cum_transaction_cost),
            'switches_SP500': monthly_tf_switch_counts['SP500'],
            'switches_GOLD_OZ_USD': monthly_tf_switch_counts['GOLD_OZ_USD'],
            'switch_cost_SP500': monthly_tf_switch_costs['SP500'],
            'switch_cost_GOLD_OZ_USD': monthly_tf_switch_costs['GOLD_OZ_USD'],
            'cum_switch_cost_SP500': cum_tf_switch_costs['SP500'],
            'cum_switch_cost_GOLD_OZ_USD': cum_tf_switch_costs['GOLD_OZ_USD'],
        }
        cost_records.append(cost_row)

    df_bt = pd.DataFrame(records)
    df_bt['date'] = df_bt['year_month'].dt.to_period('M').dt.end_time.dt.strftime('%Y-%m-%d')
    df_bt['date'] = pd.to_datetime(df_bt['date'], format='%Y-%m-%d')
    df_bt = df_bt.set_index('date').sort_index()

    df_bt['portfolio_return'] = df_bt['portfolio_return'].fillna(0.0)
    df_bt['SP500_ret'] = df_bt['SP500_ret'].fillna(0.0)
    df_bt['GOLD_OZ_USD_ret'] = df_bt['GOLD_OZ_USD_ret'].fillna(0.0)

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

    # export costs & switches CSV
    df_costs = pd.DataFrame(cost_records)
    df_costs['date'] = df_costs['year_month'].dt.to_period('M').dt.end_time.dt.strftime('%Y-%m-%d')
    df_costs['date'] = pd.to_datetime(df_costs['date'], format='%Y-%m-%d')
    df_costs = df_costs.set_index('date').sort_index()
    df_costs = df_costs.reset_index()
    df_costs = df_costs[['date','year_month','monthly_ter_cost','monthly_transaction_cost','cum_ter_cost','cum_transaction_cost',
                         'switches_SP500','switches_GOLD_OZ_USD',
                         'switch_cost_SP500','switch_cost_GOLD_OZ_USD',
                         'cum_switch_cost_SP500','cum_switch_cost_GOLD_OZ_USD']]
    df_costs = df_costs.set_index('date')
    df_costs.to_csv(f"{output_dir}/backtest_costs.csv", date_format='%Y-%m-%d')

    print("Backtest terminé. Stats :", stats)

if __name__ == "__main__":
    main()