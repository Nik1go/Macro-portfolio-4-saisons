# 🛠️ Guide d'installation (Windows / WSL)

Pour reprendre ce projet sur une machine Windows, voici la marche à suivre.

## 1. Pré-requis

L'idéal est d'utiliser **WSL (Windows Subsystem for Linux)** car Apache Airflow ne tourne pas nativement sur Windows.
Si tu veux juste lancer les analyses (Spark/Pandas) et l'interface (Streamlit), tu peux le faire sur Windows classique, mais Airflow ne fonctionnera pas.

### Option A : WSL (Recommandé pour tout le projet)
1. Installe Ubuntu via le Microsoft Store.
2. Ouvre ton terminal Ubuntu.

### Option B : Windows Classique (Juste pour Streamlit/Scripts)
1. Installe [Python 3.10+](https://www.python.org/downloads/).
2. Coche bien "Add Python to PATH" lors de l'installation.

---

## 2. Installation de l'environnement

Ouvre ton terminal (PowerShell ou WSL) dans le dossier du projet.

### Créer le venv (environnement virtuel)
```bash
python -m venv venv
```

### Activer le venv
- **Sur Windows (PowerShell)** :
  ```powershell
  .\venv\Scripts\Activate
  ```
- **Sur WSL / Linux / Mac** :
  ```bash
  source venv/bin/activate
  ```

### Installer les dépendances
Une fois le venv activé (tu devrais voir `(venv)` au début de ta ligne de commande) :
```bash
pip install -r requirements.txt
```

---

## 3. Lancer le projet

### A. Lancer l'interface de visualisation (Streamlit)
C'est le plus simple pour voir les résultats.
```bash
streamlit run streamlit_app.py
```

### B. Lancer les scripts de calcul (Spark)
Si tu veux recalculer les quadrants ou les performances manuellement :
```bash
# Exemple pour le backtest
python spark_jobs/backtest_strategy.py data/quadrants.csv data/Assets_daily.parquet 1000 data/backtest_results
```

### C. Lancer Airflow (Seulement sur WSL/Linux)
```bash
# Démarrer Airflow (dans un autre terminal)
airflow standalone
```

---

## 💡 Note sur les données
Assure-toi d'avoir le dossier `data/` avec les données sources, sinon les scripts ne pourront rien charger. Si tu repars de zéro, le DAG Airflow se charge de tout télécharger (Yahoo Finance + FRED).
