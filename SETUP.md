gi# 🛠️ Guide d'installation (Windows / WSL)

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

###  Lancer les scripts de calcul (Spark)
Si tu veux recalculer les quadrants ou les performances manuellement :
```bash
# Exemple pour lancer l'ensemble du dag
 airflow dags trigger macro_trading_dag 
```
### Lancer l'interface de visualisation (Streamlit)

C'est le plus simple pour visualiser les résultats.
```bash
streamlit run streamlit_app.py
```

###  Lancer Airflow (Seulement sur WSL/Linux)

⚠️ **Important pour la première fois :** 
Airflow a besoin d'une base de données. 

1. **Définir le dossier du projet pour Airflow**
   ```bash
   export AIRFLOW_HOME=$(pwd)
   ```

2. **Initialiser la base de données (si ce n'est pas déjà fait)**
   ```bash
   airflow db migrate
   ```
   *(Si `migrate` échoue ou n'existe pas, essaie `airflow db init`)*

3. **Créer un utilisateur Admin**
   ```bash
   airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin
   ```

4. **Lancer Airflow**
   ```bash
   airflow standalone
   ```
   Laisse ce terminal ouvert. Airflow va scanner le dossier `dags/` et trouver `macro_trading_dag.py`.
   Une fois lancé, tu peux aller sur `http://localhost:8080` (login: admin / password: admin).

5. **Déclencher le DAG**
   Dans un **nouveau** terminal (n'oublie pas le `source venv/bin/activate` et `export AIRFLOW_HOME=$(pwd)`):
   ```bash
   airflow dags trigger macro_trading_dag
   ```

---
