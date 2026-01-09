# Stratégie de Trading Macro-Économique

##  Objectif du Projet

Ce projet implémente une **stratégie de trading systématique** basée sur l'analyse des cycles économiques (quadrants macro-économiques) combinée à un système de **trend following** pour optimiser l'allocation d'actifs.

L'objectif est de construire un portefeuille dynamique qui s'adapte automatiquement aux différentes phases économiques (croissance, récession, inflation, déflation) tout en utilisant des filtres de tendance pour réduire les risques de drawdown.

##  Méthodologie

### 1. Classification des Quadrants Économiques

Le système classe chaque période en **4 quadrants** basés sur deux dimensions :
- **Croissance économique** (PIB, sentiment des consommateurs, chômage)
- **Inflation** (CPI, taux de la Fed, spreads obligataires)

**Les 4 Quadrants :**
- **Q1 (Goldilocks)** : Croissance forte + Inflation faible
- **Q2 (Reflation)** : Croissance forte + Inflation élevée 
- **Q3 (Stagflation)** : Croissance faible + Inflation élevée 
- **Q4 (Deflation)** : Croissance faible + Inflation faible 

**Méthodologie de calcul des Quadrants :**
1. **Score de Position** : Z-score basé sur l'historique complet (fenêtre expanding)
   - Compare la valeur actuelle à la moyenne/écart-type historique
   - Mapping : -2 (très bas) à +2 (très haut)

2. **Score de Variation** : Momentum court terme (120 jours glissants)
   - Détecte les changements de direction récents
   - Mapping : -2 (forte baisse) à +2 (forte hausse)

3. **Score Combiné** : Position + Variation
   - Attribution de points aux quadrants selon les règles métier
   - Le quadrant avec le score le plus élevé est sélectionné

### 2. Allocation Dynamique par Quadrant

Après avoir analysé les rendements,sharpe etc... des actifs sur chaque quadrants on établie un mapping :  
**Poids statiques par quadrant :**
- Q1 : 50% SmallCAP, 40% SP500, 10% US REIT
- Q2 : 50% Treasury, 40% Or, 10% Obligations
- Q3 : 50% Or, 40% SP500, 10% Obligations
- Q4 : 50% Or, 40% SmallCAP, 10% Treasury

### 3. Trend Following (Filtre de Protection)

**Appliqué uniquement sur SP500 et Or** pour limiter la complexité et les coûts de transaction.

**Règle du trend following :**
- Si l'actif (SP500 ou Or) est détenu dans le portefeuille
- ET passe 5 jours consécutifs sous sa Moyenne Mobile 150 jours
- → Basculer vers US Treasury 10Y
- retour vers lactif si 5 jours consécutifs au dessus de sa Moyenne mobile. 


### 4. Backtest & Calcul de Performance

**Inputs :**
- Données journalières des actifs depuis 2005
- Classification des quadrants mensuels
- Capital initial : 1000€

**Coûts intégrés :**
- **Frais de transaction** : 0.35% par mouvement (aller-retour)
- **TER annuels** : Variables par ETF (0.07% à 0.59%)
- Décomposition : coûts de rebalancement quadrant + coûts de trend following

**Métriques calculées :**
- Rendement annualisé
- Sharpe Ratio annualisé
- Maximum Drawdown
- Nombre de switches par actif (calcul des frais)
Visualisation Dasboard sur bibana 

## Architecture du Pipeline

```
1. fetch_data              → Récupération FRED API + Yahoo Finance
2. prepare_data            → Consolidation des indicateurs et actifs
3. format_data             → Nettoyage, interpolation, parquet
4. compute_quadrants       → Classification économique (Spark)
5. compute_performance     → Analyse par actif/quadrant
6. backtest_strategy       → Simulation complète avec coûts
7. index_elasticsearch     → Visualisation Kibana
```

##  Stack Technique

- **Orchestration** : Apache Airflow (DAG quotidien)
- **Processing** : PySpark (calculs distribués)
- **Data Sources** : FRED API, Yahoo Finance
- **Storage** : Parquet, CSV
- **Visualisation** : Elasticsearch + Kibana
- **Language** : Python 3.11

##  Installation & Usage

### Prérequis
```bash
# Créer l'environnement virtuel
python -m venv airflow_venv
source airflow_venv/bin/activate

# Installer les dépendances
pip install apache-airflow pandas numpy pyspark fredapi yfinance requests
```

### Lancer le pipeline complet
```bash
airflow standalone

airflow dags trigger macro_trading_dag
```

**Calcul des quadrants :**
```bash
python spark_jobs/compute_quadrants.py \
  data/Indicators.parquet \
  data/quadrants.parquet \
  data/quadrants.csv
```

**Backtest :**
```bash
python spark_jobs/backtest_strategy.py \
  data/quadrants.csv \
  data/Assets_daily.parquet \
  1000 \
  data/backtest_results
```

**Indexation Elasticsearch :**
```bash
cd index_jobs && python indexe.py
```

## 📈 Résultats

Les résultats du backtest sont disponibles dans :
- `data/backtest_results/backtest_timeseries.csv` : Évolution mensuelle
- `data/backtest_results/backtest_stats.csv` : Statistiques globales
- `data/backtest_results/backtest_costs.csv` : Décomposition des coûts

Visualisations disponibles sur **Kibana** (http://localhost:5601) après indexation.

## 🔄 Axes d'Amélioration

### 1. **Pondération Dynamique**
Actuellement, les poids des indicateurs dans le scoring des quadrants sont statiques et définis arbitrairement.  

**Améliorations possibles :**
- Utiliser une **optimisation moyenne-variance** (Markowitz) pour ajuster les poids en fonction de la volatilité récente
- Implémenter un **risk parity** pour équilibrer la contribution au risque de chaque actif
- Ajouter un **momentum scoring** pour surpondérer les actifs les plus performants du quadrant
- Tester des **allocations adaptatives** basées sur la confiance du score de quadrant

### 2. **Qualité et Justesse des Données**

**Problèmes identifiés :**
- **Latence des indicateurs** : Le PIB est publié trimestriellement avec retard (1-2 mois)
- **Révisions fréquentes** : Les données macro sont souvent révisées a posteriori
- **Interpolation mensuelle** : Peut créer des artefacts sur les indicateurs trimestriels
- **Survivorship bias** : Les ETFs récents n'ont pas d'historique complet

**Solutions proposées :**
- Utiliser uniquement des **indicateurs point-in-time** (pas de données révisées)
- Ajouter des **leading indicators** plus réactifs (ISM PMI, Initial Claims)
- Implémenter une **validation out-of-sample** stricte (train/test séparé)
- Tester la **robustesse avec bootstrap** pour évaluer la sensibilité aux données
- Forward-fill le PIB au lieu d'interpoler linéairement

### 4. **Gestion des Coûts**
- Ajouter un seuil de rebalancement (éviter les mouvements < 2%)
- Optimiser la fréquence de rebalancement (mensuel vs. trimestriel)

##  Structure du Projet

```
airflow/
├── dags/
│   └── macro_trading_dag.py          # Pipeline principal Airflow
├── spark_jobs/
│   ├── compute_quadrants.py          # Classification économique
│   ├── compute_assets_performance.py # Analyse par quadrant
│   └── backtest_strategy.py          # Simulation & backtesting
├── index_jobs/
│   └── indexe.py                     # Indexation Elasticsearch
├── data/
│   ├── backup/                       # Données brutes
│   ├── *.parquet                     # Données formatées
│   └── backtest_results/             # Résultats du backtest
├── airflow_venv/                     # Environnement virtuel
└── README.md
```

### FRED API Key
Modifier dans `dags/macro_trading_dag.py` :
```python
FRED_API_KEY = 'votre_clé_api'
```

**Note** : Ce système est à but éducatif et de recherche. Les performances passées ne garantissent pas les résultats futurs. Consultez un conseiller financier avant toute décision d'investissement.
        Projet développé dans le cadre d'apprentissage à l'analyse quantitative des cycles macro-économiques appliqués au trading systématique.

