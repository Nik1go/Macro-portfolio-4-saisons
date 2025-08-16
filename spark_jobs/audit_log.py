import pandas as pd
import matplotlib.pyplot as plt

# On charge les résultats du backtest pour analyse
df = pd.read_csv('data/backtest_results/backtest_timeseries.csv', parse_dates=['date'])
df.set_index('date', inplace=True)

# Vérification visuelle : évolution de la performance relative
plt.figure(figsize=(12, 6))
plt.plot(df['wealth'], label='Stratégie')
plt.plot(df['SP500_wealth'], label='SP500')
plt.plot(df['GOLD_wealth'], label='GOLD')
plt.title("Évolution des portefeuilles (wealth)")
plt.ylabel("Valeur")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

# Vérification des rendements mensuels (comportement extrême ?)
df[['portfolio_return', 'SP500_ret', 'GOLD_OZ_USD_ret']].describe()
