import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

# Configuration de la page
st.set_page_config(
    page_title="Quantitative Finance Portfolio",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for Bloomberg-style theme
st.markdown("""
    <style>
    /* Main theme */
    .main {
        background-color: #0a0e27;
    }
    
    /* Card styling */
    .project-card {
        background: linear-gradient(135deg, #1e2139 0%, #2a2d4a 100%);
        padding: 25px;
        border-radius: 12px;
        border: 1px solid #3d4263;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3);
        margin: 10px 0;
        transition: transform 0.2s;
    }
    
    .project-card:hover {
        transform: translateY(-5px);
        border-color: #00d4ff;
    }
    
    /* Metrics styling */
    .metric-container {
        background: rgba(30, 33, 57, 0.6);
        padding: 15px;
        border-radius: 8px;
        border-left: 3px solid #00d4ff;
    }
    
    /* Headers */
    h1, h2, h3 {
        color: #00d4ff !important;
        font-weight: 600;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1a1d35 0%, #0a0e27 100%);
    }
    
    /* Button styling */
    .stButton>button {
        background: linear-gradient(90deg, #00d4ff 0%, #0099cc 100%);
        color: white;
        border: none;
        border-radius: 6px;
        padding: 10px 24px;
        font-weight: 600;
    }
    
    .stButton>button:hover {
        background: linear-gradient(90deg, #0099cc 0%, #007399 100%);
        transform: scale(1.02);
    }
    
    /* Divider */
    hr {
        border-color: #3d4263;
    }
    </style>
""", unsafe_allow_html=True)

# Sidebar
st.sidebar.title("Navigation")

# Logo (optionnel - mettre une image dans images/logo.png)
try:
    st.sidebar.image("images/logo.png", width=200)
except:
    pass  # Si pas de logo, on continue

# Bouton de rechargement des données
st.sidebar.markdown("---")
if st.sidebar.button("🔄 Recharger les données", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("---")

page = st.sidebar.radio("Navigation", [
    "🏠 Présentation",
    "🌍 Les 4 Quadrants",
    "📈 Performance Backtest",
    "💰 Analyse des Coûts",
    "⚖️ Allocation d'Actifs"
])

# Chargement des données
@st.cache_data
def load_data():
    import os
    backtest = None
    quadrants = None
    costs = None
    stats = None
    perf_by_quad = None
    assets_daily = None
    last_update = None
    
    try:
        if os.path.exists('data/US/backtest_results/backtest_timeseries.csv'):
            backtest = pd.read_csv('data/US/backtest_results/backtest_timeseries.csv', parse_dates=['date'])
            last_update = datetime.fromtimestamp(os.path.getmtime('data/US/backtest_results/backtest_timeseries.csv'))
    except Exception as e:
        print(f"Erreur chargement backtest: {e}")
    
    try:
        if os.path.exists('data/US/output_dag/quadrants.csv'):
            quadrants = pd.read_csv('data/US/output_dag/quadrants.csv', parse_dates=['date'])
    except Exception as e:
        print(f"Erreur chargement quadrants: {e}")
    
    try:
        if os.path.exists('data/US/backtest_results/backtest_costs.csv'):
            costs = pd.read_csv('data/US/backtest_results/backtest_costs.csv', parse_dates=['date'])
    except Exception as e:
        print(f"Erreur chargement costs: {e}")
    
    try:
        if os.path.exists('data/US/backtest_results/backtest_stats.csv'):
            stats = pd.read_csv('data/US/backtest_results/backtest_stats.csv')
    except Exception as e:
        print(f"Erreur chargement stats: {e}")
    
    try:
        if os.path.exists('data/US/output_dag/assets_performance_by_quadrant.parquet'):
            perf_by_quad = pd.read_parquet('data/US/output_dag/assets_performance_by_quadrant.parquet')
    except Exception as e:
        print(f"Erreur chargement perf_by_quad: {e}")
    
    try:
        if os.path.exists('data/US/output_dag/Assets_daily.parquet'):
            assets_daily = pd.read_parquet('data/US/output_dag/Assets_daily.parquet')
            assets_daily['date'] = pd.to_datetime(assets_daily['date'])
    except Exception as e:
        print(f"Erreur chargement assets_daily: {e}")
    
    return backtest, quadrants, costs, stats, perf_by_quad, assets_daily, last_update


backtest_df, quadrants_df, costs_df, stats_df, perf_df, assets_df, last_update = load_data()

# Afficher l'info de dernière MAJ dans la sidebar
if last_update:
    st.sidebar.caption(f"Dernière MAJ : {last_update.strftime('%d/%m/%Y %H:%M')}")

# ============================================
# PAGE 1: PRÉSENTATION
# ============================================
if page == "🏠 Présentation":
    st.title("Stratégie d'Asset Management Macro-Économique")
    st.markdown("### *All Weather Portfolio - Rotation d'actifs par Quadrants*")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ## Objectif du Projet
        
        Ce projet implémente une **stratégie de trading systématique** basée sur :
        - L'analyse des **cycles économiques** (4 quadrants)
        - Un système de **trend following** (MA150)
        - Une allocation dynamique mensuelle d'actifs
        
        ### Méthodologie
        
        **Classification des Quadrants :**
        - **Q1 (Goldilocks)** : Croissance ↑ + Inflation ↓
        - **Q2 (Reflation)** : Croissance ↑ + Inflation ↑
        - **Q3 (Stagflation)** : Croissance ↓ + Inflation ↑
        - **Q4 (Deflation)** : Croissance ↓ + Inflation ↓
    st.image("images/quadrants.jpeg", use_column_width=True)
        
        **Indicateurs utilisés :**
        - **Macro** : Inflation (CPI), Sentiment consommateur, Initial Claims, Housing Permits, Production Industrielle
        - **Marché** : High Yield Spread, 10-2Y Treasury Spread, Taux Fed, VIX
        """)
    
    with col2:
        st.markdown("""
        ### Stack Technique
        
        - **Orchestration** : Apache Airflow Dag
        - **Processing** : PySpark
        - **Data** : FRED API + Yahoo Finance
        - **Visualisation** : Elasticsearch + Kibana + Streamlit
        
        ### Période du Backtest
        
        **2005 - 2026** (20+ ans de données)
        """)
    
    st.markdown("---")
    
    # Schéma d'architecture (optionnel)
    st.markdown("## Architecture du Projet")
    try:
        st.image("images/architecture DAG.jpeg", caption="Pipeline du DAG Airflow", use_column_width=True)
    except Exception as e:
        st.error(f"Erreur lors de l'affichage de l'image : {e}")
    
    st.markdown("### Tâches du DAG Airflow")
    st.markdown(" - fetch data task : Récupération des données macro et financières (Yahoo Finance, FRED API)")    
    st.markdown(" - prepare indicators data task : Agrégation des differents indicateurs en un seul DataFrame")
    st.markdown(" - prepare assets data task : Agrégation des différents actifs en un seul DataFrame")
    st.markdown(" - format indicators data task : Nettoyage des indicateurs et export en Parquet")
    st.markdown(" - format assets data task : Nettoyage des actifs et export en Parquet")
    st.markdown(" - compute economic quadrants task : Exécution d'un script Spark pour classifier chaque période dans un des 4 quadrants économiques (Goldilocks, Reflation, Stagflation, Deflation)")
    st.markdown(" - compute assets performance task : Évaluation des performances des actifs dans chaque quadrant économique calculé ")
    st.markdown(" - backtest strategy task : Exécution d'un backtest spark qui alloue les actifs en fonction des quadrants économiques avec le systeme de trending")
    st.markdown(" - index to elasticsearch task : Indexation des données dans Elasticsearch pour la visualisation dans Kibana")


    st.markdown("### le DAG Airflow est exécuté chaque jour à 8h.")

    st.markdown("---")

    st.markdown("""
        ## Trend Following
        
        **Pour tenter d'optimiser les performances de la stratégie, un système de trending a été implémenté afin de réduire les drawdowns en période de baisse prolongée**
        **Appliqué uniquement sur SP500 et Or**
        **Règles :**
        - **Switch → Treasury** : Si 5 jours consécutifs sous MA150
        - **Retour → Actif** : Si 5 jours consécutifs au-dessus MA150
        st.markdown""")
    
    # Métriques clés
    if stats_df is not None and len(stats_df) > 0:
        try:
            st.markdown("## Résultats Globaux")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "Rendement Annuel Stratégie",
                    f"{stats_df['strategy_avg_year_return'].iloc[0]*100:.2f}%"
                )
            with col2:
                st.metric(
                    "Sharpe Ratio Stratégie",
                    f"{stats_df['strategy_sharpe_annual'].iloc[0]:.2f}"
                )
            with col3:
                st.metric(
                    "Max Drawdown Stratégie",
                    f"{stats_df['strategy_max_drawdown'].iloc[0]*100:.2f}%"
                )
            with col4:
                st.metric(
                    "Volatilité Annuelle",
                    f"{stats_df['strategy_vol_annual'].iloc[0]*100:.2f}%"
                )
            
            st.markdown("### Comparaison avec Benchmarks")
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**SP500**")
                st.metric("Rendement", f"{stats_df['SP500_avg_year_return'].iloc[0]*100:.2f}%")
                st.metric("Sharpe", f"{stats_df['SP500_sharpe_annual'].iloc[0]:.2f}")
                st.metric("Max DD", f"{stats_df['SP500_max_drawdown'].iloc[0]*100:.2f}%")
            
            with col2:
                st.markdown("**Or**")
                st.metric("Rendement", f"{stats_df['GOLD_avg_year_return'].iloc[0]*100:.2f}%")
                st.metric("Sharpe", f"{stats_df['GOLD_sharpe_annual'].iloc[0]:.2f}")
                st.metric("Max DD", f"{stats_df['GOLD_max_drawdown'].iloc[0]*100:.2f}%")
        except Exception as e:
            st.error(f"Erreur lors de l'affichage des métriques : {e}")
            st.info("Vérifiez que les fichiers de backtest ont été générés correctement.")
    else:
        st.warning("Les données de statistiques ne sont pas disponibles. Relancez le backtest pour générer les résultats.")


# ============================================
# PAGE 2: PERFORMANCE BACKTEST
# ============================================
elif page == "📈 Performance Backtest":
    st.title("Performance du Backtest")
    
    if backtest_df is not None:
        # Graphique principal de performance
        st.markdown("### Évolution du Capital (Base 1000€)")
        
        fig = go.Figure()
        
        # Stratégie (ligne la plus épaisse)
        fig.add_trace(go.Scatter(
            x=backtest_df['date'],
            y=backtest_df['wealth'],
            name='🎯 Stratégie All Weather',
            line=dict(color='#1f77b4', width=4),
            mode='lines'
        ))
        
        # SP500 (benchmark principal)
        fig.add_trace(go.Scatter(
            x=backtest_df['date'],
            y=backtest_df['SP500_wealth'],
            name='📈 SP500',
            line=dict(color='#ff7f0e', width=2.5, dash='dash'),
            mode='lines'
        ))
        
        # Gold (benchmark alternatif)
        fig.add_trace(go.Scatter(
            x=backtest_df['date'],
            y=backtest_df['GOLD_wealth'],
            name='🥇 Or',
            line=dict(color='#ffd700', width=2.5, dash='dot'),
            mode='lines'
        ))
        
        fig.update_layout(
            xaxis_title='Date',
            yaxis_title='Valeur du Portefeuille (€)',
            hovermode='x unified',
            height=600,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor="rgba(255, 255, 255, 0.8)"
            )
        )
        
        # Display current quadrant if available
        if 'smooth_quadrant' in backtest_df.columns:
            latest_quadrant = backtest_df['smooth_quadrant'].iloc[-1]
            latest_date = backtest_df['date'].iloc[-1]
            
            quad_names = {
                1: "Q1 - Goldilocks (Croissance ↑, Inflation ↓)",
                2: "Q2 - Reflation (Croissance ↑, Inflation ↑)",
                3: "Q3 - Stagflation (Croissance ↓, Inflation ↑)",
                4: "Q4 - Deflation (Croissance ↓, Inflation ↓)"
            }
            
            quad_colors = {1: '#2ecc71', 2: '#e74c3c', 3: '#e67e22', 4: '#3498db'}
            
            st.info(f"**Quadrant Actuel ({pd.to_datetime(latest_date).strftime('%d/%m/%Y')})**: {quad_names.get(latest_quadrant, 'N/A')}")
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Rendements mensuels
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Distribution des Rendements Mensuels")
            fig_hist = go.Figure()
            fig_hist.add_trace(go.Histogram(
                x=backtest_df['portfolio_return']*100,
                nbinsx=50,
                name='Stratégie',
                marker_color='#1f77b4'
            ))
            fig_hist.update_layout(
                xaxis_title='Rendement Mensuel (%)',
                yaxis_title='Fréquence',
                height=400
            )
            st.plotly_chart(fig_hist, use_container_width=True)
        
        with col2:
            st.markdown("### Rendements Cumulés Annuels")
            backtest_df['year'] = pd.to_datetime(backtest_df['date']).dt.year
            annual_returns = backtest_df.groupby('year')['portfolio_return'].apply(
                lambda x: (1 + x).prod() - 1
            ) * 100
            
            fig_bar = go.Figure(data=[
                go.Bar(x=annual_returns.index, y=annual_returns.values,
                       marker_color=['green' if x > 0 else 'red' for x in annual_returns.values])
            ])
            fig_bar.update_layout(
                xaxis_title='Année',
                yaxis_title='Rendement Annuel (%)',
                height=400
            )
            st.plotly_chart(fig_bar, use_container_width=True)

# ============================================
# PAGE 3: QUADRANTS ÉCONOMIQUES
# ============================================
# ============================================
# PAGE 3 & 6 MERGED: ANALYSE & PERFORMANCE QUADRANTS
# ============================================
elif page == "🌍 Les 4 Quadrants":
    st.title("Les 4 Quadrants")
    
    st.markdown("""
    Cette page combine l'analyse de l'évolution des quadrants économiques et la performance des actifs au sein de chaque régime.
    """)

    with st.expander("Méthodologie de Calcul des Quadrants", expanded=True):
        st.markdown("""
        ### Logique Algorithmique
        
        L'algorithme détermine le régime économique (Q1, Q2, Q3, Q4) pour chaque jour de trading afin d'ajuster dynamiquement l'allocation du portefeuille. 
        Le modèle repose sur une analyse multi-factorielle d'une dizaine d'indicateurs macro-économiques et financiers.
        
        #### 1. Normalisation et Scoring
        Pour chaque indicateur, nous calculons deux métriques distinctes pour évaluer la situation économique :
        
        *   **Score de Position (Z-Score)** : Mesure l'écart de la valeur actuelle par rapport à sa médiane historique.
            *   *Objectif : Déterminer si le niveau est historiquement haut ou bas.*
        *   **Score de Variation (Momentum)** : Mesure la dynamique récente (accélération ou décélération) sur une période court-terme.
            *   *Objectif : Anticiper les points d'inflexion avant qu'ils ne soient visibles dans les moyennes long terme.*

        #### 2. Pondération des Indicateurs
        Tous les indicateurs n'ont pas la même influence. Une pondération est appliquée pour favoriser les signaux avancés (Leading Indicators) qui anticipent les mouvements de marché.
        
        *   **Indicateurs Avancés (Coeff 1.2)** : Courbe des taux (10Y-2Y), Volatilité (VIX), Pétrole (WTI).
            *   *Rôle : Signaux d'alerte précoce sur les récessions ou chocs inflationnistes.*
        *   **Indicateurs Coïncidents (Coeff 1.1)** : Dollar Index, Cuivre, Spreads de Crédit.
            *   *Rôle : Confirmation de la tendance actuelle.*
        *   **Indicateurs Retardés (Coeff 1.0)** : Inflation (CPI), Chômage, Taux Fed.
            *   *Rôle : Validation macro-économique fondamentale.*
        
        #### 3. Classification Finale
        Chaque indicateur attribue des points aux quadrants pertinents (ex: Inflation en hausse donne des points à Q2 et Q3).
        Le quadrant retenu pour la journée est celui qui cumule le **score total pondéré le plus élevé**.
        """)

    if backtest_df is not None:
        # Préparation des couleurs et labels pour les autres graphiques
        quad_colors = {1: '#2ecc71', 2: '#e74c3c', 3: '#e67e22', 4: '#3498db'}
        quad_labels = {
            1: "Q1 Goldilocks", 
            2: "Q2 Reflation", 
            3: "Q3 Stagflation", 
            4: "Q4 Deflation"
        }

        # --- PARTIE 2: RÉPARTITION DU PORTEFEUILLE (STACKED AREA) ---
        st.markdown("### 1. Composition du Portefeuille (Historique)")
        st.caption("Évolution de l'allocation d'actifs en fonction des régimes économiques.")

        # Identification des colonnes de poids
        weight_cols = [c for c in backtest_df.columns if c.endswith('_weight') and not c.startswith('cash')]
        
        # Mapping couleurs actifs
        asset_colors = {
            'SP500_weight': '#1f77b4',       # Blue
            'NASDAQ_100_weight': '#aec7e8',  # Light Blue
            'GOLD_OZ_USD_weight': '#ffd700', # Gold
            'TREASURY_10Y_weight': '#2ca02c',# Green
            'COMMODITIES_weight': '#8c564b', # Brown
            'SmallCAP_weight': '#9467bd',    # Purple
            'US_REIT_VNQ_weight': '#e377c2', # Pink
            'OBLIGATION_weight': '#bcbd22'   # Olive
        }
        
        # Nettoyage des noms pour la légende
        def clean_name(col):
            return col.replace('_weight', '').replace('_', ' ')

        # Création du Stacked Area Chart
        fig_alloc = go.Figure()

        for col in weight_cols:
            # On ne plot que si l'actif a été utilisé au moins une fois
            if backtest_df[col].sum() > 0:
                fig_alloc.add_trace(go.Scatter(
                    x=backtest_df['date'],
                    y=backtest_df[col] * 100, # En pourcentage
                    mode='lines',
                    name=clean_name(col),
                    stackgroup='one', # C'est ça qui fait le stacking
                    line=dict(width=0.5),
                    fillcolor=asset_colors.get(col, None),
                    marker=dict(color=asset_colors.get(col, None))
                ))

        fig_alloc.update_layout(
            yaxis_title='Allocation (%)',
            xaxis_title='Date',
            height=450,
            hovermode='x unified',
            yaxis=dict(range=[0, 100]),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig_alloc, use_container_width=True)

        col1, col2 = st.columns([1, 2])
        with col1:
             # Pie chart Répartition
            st.markdown("#### Répartition Historique")
            quad_counts = quadrants_df['assigned_quadrant'].value_counts().sort_index()
            fig_pie = go.Figure(data=[go.Pie(
                labels=[quad_labels[i] for i in quad_counts.index],
                values=quad_counts.values,
                marker=dict(colors=[quad_colors[i] for i in quad_counts.index]),
                hole=0.4
            )])
            fig_pie.update_layout(height=300, margin=dict(t=20, b=20))
            st.plotly_chart(fig_pie, use_container_width=True)

        with col2:
             # Indicateurs Grid (Compact)
            st.markdown("#### Indicateurs Clés (Dernière Période)")
            # On affiche juste les courbes des 3 indicateurs les plus importants pour pas surcharger
            key_indicators = ['INFLATION', 'TAUX_FED', 'VIX']
            
            # Grille de mini charts
            idx_cols = st.columns(3)
            for i, ind in enumerate(key_indicators):
                if ind in quadrants_df.columns:
                    with idx_cols[i]:
                        mini_fig = px.line(quadrants_df, x='date', y=ind, title=ind)
                        mini_fig.update_layout(height=200, margin=dict(l=10,r=10,t=30,b=10), showlegend=False)
                        mini_fig.update_xaxes(showticklabels=False)
                        st.plotly_chart(mini_fig, use_container_width=True)

    st.markdown("---")

    if perf_df is not None and len(perf_df) > 0:
        st.markdown("### 2. Performance des Actifs par Régime")
        st.caption("Quels actifs performent le mieux dans chaque quadrant ? (Basé sur l'historique)")

        col_hm1, col_hm2 = st.columns(2)
        
        with col_hm1:
            st.markdown("#### Rendement Mensuel Moyen")
            # Heatmap Returns
            try:
                perf_df_copy = perf_df.copy()
                if 'annual_return' in perf_df_copy.columns and 'asset' in perf_df_copy.columns and 'quadrant' in perf_df_copy.columns:
                    perf_df_copy['monthly_return'] = perf_df_copy['annual_return'] / 12
                    # Ensure quadrants are integers
                    perf_df_copy['quadrant'] = perf_df_copy['quadrant'].astype(int)
                    
                    pivot_returns = perf_df_copy.pivot(index='asset', columns='quadrant', values='monthly_return')
                    pivot_returns = pivot_returns.fillna(0)
                    pivot_returns = pivot_returns.reindex(columns=[1, 2, 3, 4], fill_value=0)
                    
                    fig_hm1 = go.Figure(data=go.Heatmap(
                        z=pivot_returns.values * 100,
                        x=[f'Q{i}' for i in pivot_returns.columns],
                        y=pivot_returns.index,
                        colorscale='RdYlGn',
                        text=[[f'{v*100:+.1f}%' for v in row] for row in pivot_returns.values],
                        texttemplate='%{text}',
                        textfont={"size": 10}
                    ))
                    fig_hm1.update_layout(height=500, margin=dict(t=30))
                    st.plotly_chart(fig_hm1, use_container_width=True)
                else:
                    st.error("Colonnes manquantes dans perf_df pour la heatmap.")
            except Exception as e:
                st.error(f"Erreur heatmap returns: {e}")

        with col_hm2:
            st.markdown("#### Sharpe Ratio (Risque/Rendement)")
            # Heatmap Sharpe
            try:
                if 'sharpe' in perf_df.columns:
                    # Cast quadrant column to int just in case
                    pivot_data = perf_df.copy()
                    pivot_data['quadrant'] = pivot_data['quadrant'].astype(int)

                    pivot_sharpe = pivot_data.pivot(index='asset', columns='quadrant', values='sharpe')
                    pivot_sharpe = pivot_sharpe.fillna(0)
                    pivot_sharpe = pivot_sharpe.reindex(columns=[1, 2, 3, 4], fill_value=0)
                    
                    fig_hm2 = go.Figure(data=go.Heatmap(
                        z=pivot_sharpe.values,
                        x=[f'Q{i}' for i in pivot_sharpe.columns],
                        y=pivot_sharpe.index,
                        colorscale='Blues',
                        text=pivot_sharpe.values,
                        texttemplate='%{text:.2f}',
                        textfont={"size": 11}
                    ))
                    fig_hm2.update_layout(height=500, margin=dict(t=30))
                    st.plotly_chart(fig_hm2, use_container_width=True)
                else:
                    st.error("Colonne 'sharpe' manquante.")
            except Exception as e:
                st.error(f"Erreur heatmap sharpe: {e}")

    else:
        st.warning("⚠️ Les données de performance par quadrant ne sont pas encore calculées.")

# ============================================
# PAGE 4: ANALYSE DES COÛTS
# ============================================
elif page == "💰 Analyse des Coûts":
    st.title("Analyse des Coûts")
    
    if costs_df is not None and stats_df is not None:
        try:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if 'cum_transaction_cost' in stats_df.columns:
                    st.metric(
                        "Coûts de Transaction Totaux",
                        f"{stats_df['cum_transaction_cost'].iloc[0]*100:.2f}%"
                    )
                else:
                    st.metric("Coûts de Transaction Totaux", "N/A")
            
            with col2:
                if 'cum_ter_cost' in stats_df.columns:
                    st.metric(
                        "TER Cumulés",
                        f"{stats_df['cum_ter_cost'].iloc[0]*100:.2f}%"
                    )
                else:
                    st.metric("TER Cumulés", "N/A")
            
            with col3:
                if 'cum_transaction_cost' in stats_df.columns and 'cum_ter_cost' in stats_df.columns:
                    total_cost = (stats_df['cum_transaction_cost'].iloc[0] + 
                                 stats_df['cum_ter_cost'].iloc[0])
                    st.metric(
                        "Coûts Totaux",
                        f"{total_cost*100:.2f}%"
                    )
                else:
                    st.metric("Coûts Totaux", "N/A")
            
            # Évolution des coûts cumulés
            if 'cum_transaction_cost' in costs_df.columns and 'cum_ter_cost' in costs_df.columns:
                st.markdown("### Évolution des Coûts Cumulés")
                fig = go.Figure()
                
                fig.add_trace(go.Scatter(
                    x=costs_df['date'],
                    y=costs_df['cum_transaction_cost']*100,
                    name='Frais de Transaction',
                    fill='tonexty'
                ))
                
                fig.add_trace(go.Scatter(
                    x=costs_df['date'],
                    y=costs_df['cum_ter_cost']*100,
                    name='TER (Frais de Gestion)'
                ))
                
                fig.update_layout(
                    yaxis_title='Coûts Cumulés (%)',
                    xaxis_title='Date',
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Switches trend following
            st.markdown("### Activité Trend Following")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**SP500**")
                if 'nb_switch_sp500' in stats_df.columns:
                    st.metric("Nombre de Switches", f"{stats_df['nb_switch_sp500'].iloc[0]:.0f}")
                else:
                    st.info("Données de switches non disponibles")
            
            with col2:
                st.markdown("**Or**")
                if 'nb_switch_gold_oz_usd' in stats_df.columns:
                    st.metric("Nombre de Switches", f"{stats_df['nb_switch_gold_oz_usd'].iloc[0]:.0f}")
                else:
                    st.info("Données de switches non disponibles")
        
        except Exception as e:
            st.error(f"Erreur lors de l'affichage des coûts: {e}")
            st.info("Vérifiez que les fichiers de backtest ont été générés correctement.")

# ============================================
# PAGE 5: TREND FOLLOWING
# ============================================
elif page == "🔄 Trend Following":
    st.title("Système Trend Following")
    
    st.markdown("""
    ### Règle de Trend Following (MA150)
    
    **Appliqué uniquement sur SP500 et Or**
    
    - **Passage en risk-off** : 5 jours consécutifs sous la MA150 → Switch vers Treasury
    - **Retour en risk-on** : 5 jours consécutifs au-dessus de la MA150 → Retour vers l'actif
    
    **Objectif** : Réduire les drawdowns en période de baisse prolongée
    """)
    
    if backtest_df is not None or stats_df is not None:
        # Afficher les statistiques de switches si disponibles
        if stats_df is not None:
            st.markdown("### Statistiques des Switches")
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**SP500**")
                if 'nb_switch_sp500' in stats_df.columns:
                    nb_switches = stats_df['nb_switch_sp500'].iloc[0]
                    st.metric("Nombre Total de Switches", f"{nb_switches:.0f}")
                else:
                    st.info("Données de switches SP500 non disponibles")
            
            with col2:
                st.markdown("**Or (GOLD_OZ_USD)**")
                if 'nb_switch_gold_oz_usd' in stats_df.columns:
                    nb_switches = stats_df['nb_switch_gold_oz_usd'].iloc[0]
                    st.metric("Nombre Total de Switches", f"{nb_switches:.0f}")
                else:
                    st.info("Données de switches Or non disponibles")
        
        st.markdown("---")
        st.info("💡 Les switches correspondent aux moments où la stratégie passe d'un actif risqué (SP500/Or) vers le Treasury 10Y, et vice-versa, en fonction de la MA150.")
    else:
        st.warning("Les données de backtest ne sont pas disponibles.")



# ============================================
# PAGE 7: ALLOCATION D'ACTIFS
# ============================================
elif page == "⚖️ Allocation d'Actifs":
    st.title("Allocation d'Actifs dans le Temps")
    
    if backtest_df is not None:
        # Liste des colonnes de poids
        weight_columns = [col for col in backtest_df.columns if col.endswith('_weight')]
        
        if weight_columns:
            # Allocation actuelle (dernière ligne)
            st.markdown("### Allocation Actuelle du Portefeuille")
            
            latest_weights = backtest_df[weight_columns].iloc[-1]
            latest_date = backtest_df['date'].iloc[-1]
            
            # Nettoyer les noms pour l'affichage
            clean_names = [col.replace('_weight', '').replace('_', ' ') for col in weight_columns]
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Pie chart de l'allocation actuelle
                fig_pie = go.Figure(data=[go.Pie(
                    labels=clean_names,
                    values=latest_weights.values,
                    hole=0.3
                )])
                fig_pie.update_layout(
                    title=f'Allocation au {pd.to_datetime(latest_date).strftime("%d/%m/%Y")}',
                    height=400
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            
            with col2:
                st.markdown("#### Détail des Poids")
                for name, weight in zip(clean_names, latest_weights.values):
                    st.metric(name, f"{weight*100:.1f}%")
            
            # Évolution des poids dans le temps (Stacked Area Chart)
            st.markdown("### Évolution de l'Allocation dans le Temps")
            
            fig_area = go.Figure()
            
            for col, name in zip(weight_columns, clean_names):
                fig_area.add_trace(go.Scatter(
                    x=backtest_df['date'],
                    y=backtest_df[col] * 100,
                    name=name,
                    stackgroup='one',
                    mode='none'
                ))
            
            fig_area.update_layout(
                title='Allocation d\'Actifs (% du Portefeuille)',
                xaxis_title='Date',
                yaxis_title='Allocation (%)',
                hovermode='x unified',
                height=500
            )
            st.plotly_chart(fig_area, use_container_width=True)
            
            # Timeline plot - Vue individuelle de chaque actif
            st.markdown("### Vue Détaillée par Actif")
            
            for col, name in zip(weight_columns, clean_names):
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=backtest_df['date'],
                    y=backtest_df[col] * 100,
                    name=name,
                    fill='tozeroy',
                    line=dict(width=2)
                ))
                fig.update_layout(
                    title=f'{name} - Évolution du Poids',
                    xaxis_title='Date',
                    yaxis_title='Poids (%)',
                    height=300,
                    showlegend=False
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Statistiques d'allocation
            st.markdown("### Statistiques d'Allocation")
            
            stats_data = []
            for col, name in zip(weight_columns, clean_names):
                weights = backtest_df[col] * 100
                stats_data.append({
                    'Actif': name,
                    'Poids Moyen (%)': weights.mean(),
                    'Poids Min (%)': weights.min(),
                    'Poids Max (%)': weights.max(),
                    'Volatilité (%)': weights.std()
                })
            
            stats_table = pd.DataFrame(stats_data)
            st.dataframe(stats_table.style.format({
                'Poids Moyen (%)': '{:.2f}',
                'Poids Min (%)': '{:.2f}',
                'Poids Max (%)': '{:.2f}',
                'Volatilité (%)': '{:.2f}'
            }), use_container_width=True)
            
        else:
            st.warning("Les colonnes de poids (_weight) ne sont pas disponibles dans les données de backtest.")
    else:
        st.warning("Les données de backtest ne sont pas disponibles. Relancez le backtest pour générer les résultats.")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center'>
    <p><strong>Stratégie de Trading Macro-Économique</strong></p>
    <p>Projet Data Science - Pipeline Airflow + PySpark + Elasticsearch + Kibana</p>
    <p><a href='https://github.com/Nik1go/Macro-portfolio-4-saisons'>GitHub Repository</a></p>
</div>
""", unsafe_allow_html=True)

