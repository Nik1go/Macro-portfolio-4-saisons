# 📸 Dossier Images pour Streamlit

Ce dossier contient les images utilisées dans l'application Streamlit.

## 🖼️ Images suggérées à ajouter :

### 1. **logo.png** (200x200px recommandé)
- Logo de votre projet
- Affiché dans la sidebar
- Format PNG transparent recommandé

### 2. **architecture.png** (1200x800px recommandé)
- Schéma du pipeline de données :
  ```
  FRED API + Yahoo Finance
         ↓
    Apache Airflow
         ↓
      PySpark (compute_quadrants, backtest)
         ↓
  Elasticsearch + Kibana + Streamlit
  ```

### 3. **quadrants_explained.png** (optionnel)
- Schéma explicatif des 4 quadrants économiques

### 4. **trend_following.png** (optionnel)
- Schéma de la règle MA150

## 📝 Comment créer ces images :

### Option 1 : Capture d'écran depuis Kibana
- Exporter vos graphiques Kibana
- Sauvegarder en PNG

### Option 2 : Outils en ligne
- **Canva** (gratuit) : https://www.canva.com
- **Excalidraw** (schémas) : https://excalidraw.com
- **Draw.io** (architecture) : https://app.diagrams.net

### Option 3 : Python (génération automatique)
```python
import plotly.graph_objects as go

fig = go.Figure(...)
fig.write_image("images/mon_graphique.png")
```

## 🚀 Utilisation dans Streamlit

```python
# Image simple
st.image("images/logo.png")

# Image avec caption et largeur
st.image("images/architecture.png", 
         caption="Architecture", 
         width=600)

# Image en colonnes
col1, col2 = st.columns(2)
with col1:
    st.image("images/img1.png")
with col2:
    st.image("images/img2.png")
```

## 📦 Formats supportés

- PNG (recommandé pour transparence)
- JPG/JPEG (photos)
- SVG (vectoriel)
- GIF (animé)

