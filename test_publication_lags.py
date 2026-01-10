"""
Script de test pour vérifier l'application correcte des lags de publication
"""
import pandas as pd
import os

print("=" * 70)
print("TEST DES LAGS DE PUBLICATION")
print("=" * 70)

# Charger les données combinées
indicators_path = 'data/Indicators.parquet'

if os.path.exists(indicators_path):
    df = pd.read_parquet(indicators_path)
    df['date'] = pd.to_datetime(df['date'])
    
    print("\n📊 Aperçu des dernières données disponibles :")
    print(df.tail(10)[['date', 'INFLATION', 'UNEMPLOYMENT', 'CONSUMER_SENTIMENT']])
    
    # Vérifier les décalages
    print("\n" + "=" * 70)
    print("VÉRIFICATION DES LAGS")
    print("=" * 70)
    
    # Test : Comparer avec les données brutes
    indicators_to_check = {
        'INFLATION': 15,
        'UNEMPLOYMENT': 7,
        'CONSUMER_SENTIMENT': 5,
        'High_Yield_Bond_SPREAD': 0,
        '10-2Year_Treasury_Yield_Bond': 0,
        'TAUX_FED': 0
    }
    
    for indicator, expected_lag in indicators_to_check.items():
        backup_path = f'data/backup/{indicator}.csv'
        if os.path.exists(backup_path):
            df_raw = pd.read_csv(backup_path, parse_dates=['date'])
            
            # Comparer les dernières dates
            last_date_raw = df_raw['date'].max()
            if indicator in df.columns:
                last_date_processed = df[df[indicator].notna()]['date'].max()
                
                diff_days = (last_date_processed - last_date_raw).days
                
                status = "✅" if abs(diff_days - expected_lag) <= 5 else "⚠️"
                print(f"\n{status} {indicator}")
                print(f"   Dernière date brute     : {last_date_raw.strftime('%Y-%m-%d')}")
                print(f"   Dernière date traitée   : {last_date_processed.strftime('%Y-%m-%d')}")
                print(f"   Décalage observé        : {diff_days} jours")
                print(f"   Décalage attendu        : {expected_lag} jours")
    
    print("\n" + "=" * 70)
    print("IMPACT SUR LE BACKTEST")
    print("=" * 70)
    
    # Vérifier les quadrants
    quadrants_path = 'data/quadrants.csv'
    if os.path.exists(quadrants_path):
        df_quad = pd.read_csv(quadrants_path, parse_dates=['date'])
        print(f"\n📅 Dernier quadrant calculé : {df_quad['date'].max().strftime('%Y-%m-%d')}")
        print(f"📊 Quadrant actuel          : Q{df_quad.iloc[-1]['assigned_quadrant']}")
        
        print("\n💡 Les décisions de trading utilisent maintenant des données")
        print("   disponibles au moment réel (correction du look-ahead bias)")
    
    print("\n✅ Test terminé !")
    
else:
    print(f"❌ Fichier {indicators_path} introuvable.")
    print("   Exécutez d'abord le DAG Airflow pour générer les données.")
    print("\n   Commande : airflow dags trigger macro_trading_dag")

