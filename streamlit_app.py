import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

# Configuration de la page
st.set_page_config(
    page_title="Stratégie Asset Allocation Macro - All Weather Portfolio",
    page_icon="📊",
    layout="wide"
)

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

page = st.sidebar.radio("", [
    "🏠 Présentation",
    "📈 Performance Backtest",
    "🌍 Quadrants Économiques",
    "💰 Analyse des Coûts",
    "🔄 Trend Following",
    "📊 Performance par Quadrant"
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
    last_update = None
    
    try:
        if os.path.exists('data/backtest_results/backtest_timeseries.csv'):
            backtest = pd.read_csv('data/backtest_results/backtest_timeseries.csv', parse_dates=['date'])
            last_update = datetime.fromtimestamp(os.path.getmtime('data/backtest_results/backtest_timeseries.csv'))
    except Exception as e:
        print(f"Erreur chargement backtest: {e}")
    
    try:
        if os.path.exists('data/quadrants.csv'):
            quadrants = pd.read_csv('data/quadrants.csv', parse_dates=['date'])
    except Exception as e:
        print(f"Erreur chargement quadrants: {e}")
    
    try:
        if os.path.exists('data/backtest_results/backtest_costs.csv'):
            costs = pd.read_csv('data/backtest_results/backtest_costs.csv', parse_dates=['date'])
    except Exception as e:
        print(f"Erreur chargement costs: {e}")
    
    try:
        if os.path.exists('data/backtest_results/backtest_stats.csv'):
            stats = pd.read_csv('data/backtest_results/backtest_stats.csv')
    except Exception as e:
        print(f"Erreur chargement stats: {e}")
    
    try:
        if os.path.exists('data/assets_performance_by_quadrant.parquet'):
            perf_by_quad = pd.read_parquet('data/assets_performance_by_quadrant.parquet')
    except Exception as e:
        print(f"Erreur chargement perf_by_quad: {e}")
    
    return backtest, quadrants, costs, stats, perf_by_quad, last_update

backtest_df, quadrants_df, costs_df, stats_df, perf_df, last_update = load_data()

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
        - Chômage, Sentiment consommateur
        - Inflation (CPI), Spreads obligataires AOT vs High Yield Bond, 10-2Year Treasury Yield Bond, Taux Fed
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
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=backtest_df['date'],
            y=backtest_df['wealth'],
            name='Stratégie All Weather',
            line=dict(color='#1f77b4', width=3)
        ))
        
        fig.add_trace(go.Scatter(
            x=backtest_df['date'],
            y=backtest_df['SP500_wealth'],
            name='SP500',
            line=dict(color='#ff7f0e', width=2, dash='dash')
        ))
        
        fig.add_trace(go.Scatter(
            x=backtest_df['date'],
            y=backtest_df['GOLD_wealth'],
            name='Or',
            line=dict(color='#ffd700', width=2, dash='dot')
        ))
        
        fig.update_layout(
            title='Évolution du Capital (Base 1000€)',
            xaxis_title='Date',
            yaxis_title='Valeur du Portefeuille (€)',
            hovermode='x unified',
            height=600
        )
        
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
elif page == "🌍 Quadrants Économiques":
    st.title("Quadrants Économiques")
    
    if quadrants_df is not None:
        # Répartition des quadrants
        quad_counts = quadrants_df['assigned_quadrant'].value_counts().sort_index()
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("### Évolution des Quadrants dans le Temps")
            fig_quad = go.Figure()
            
            colors = {1: '#2ecc71', 2: '#e74c3c', 3: '#e67e22', 4: '#3498db'}
            
            for quad in [1, 2, 3, 4]:
                mask = quadrants_df['assigned_quadrant'] == quad
                fig_quad.add_trace(go.Scatter(
                    x=quadrants_df[mask]['date'],
                    y=[quad] * mask.sum(),
                    mode='markers',
                    name=f'Q{quad}',
                    marker=dict(size=8, color=colors[quad])
                ))
            
            fig_quad.update_layout(
                yaxis=dict(tickvals=[1, 2, 3, 4], title='Quadrant'),
                xaxis_title='Date',
                height=400
            )
            st.plotly_chart(fig_quad, use_container_width=True)
        
        with col2:
            st.markdown("### Répartition")
            fig_pie = go.Figure(data=[go.Pie(
                labels=[f'Q{i}' for i in quad_counts.index],
                values=quad_counts.values,
                marker=dict(colors=[colors[i] for i in quad_counts.index])
            )])
            fig_pie.update_layout(height=400)
            st.plotly_chart(fig_pie, use_container_width=True)
        
        # Indicateurs par quadrant
        st.markdown("### Indicateurs Économiques par Période")
        
        indicators = ['INFLATION', 'UNEMPLOYMENT', 'CONSUMER_SENTIMENT']
        
        for ind in indicators:
            if ind in quadrants_df.columns:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=quadrants_df['date'],
                    y=quadrants_df[ind],
                    name=ind,
                    line=dict(width=2)
                ))
                fig.update_layout(
                    title=f'{ind} dans le temps',
                    height=300
                )
                st.plotly_chart(fig, use_container_width=True)

# ============================================
# PAGE 4: ANALYSE DES COÛTS
# ============================================
elif page == "💰 Analyse des Coûts":
    st.title("Analyse des Coûts")
    
    if costs_df is not None and stats_df is not None:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "Coûts de Transaction Totaux",
                f"{stats_df['cum_transaction_cost'].iloc[0]*100:.2f}%"
            )
        with col2:
            st.metric(
                "TER Cumulés",
                f"{stats_df['cum_ter_cost'].iloc[0]*100:.2f}%"
            )
        with col3:
            total_cost = (stats_df['cum_transaction_cost'].iloc[0] + 
                         stats_df['cum_ter_cost'].iloc[0])
            st.metric(
                "Coûts Totaux",
                f"{total_cost*100:.2f}%"
            )
        
        # Évolution des coûts cumulés
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
            st.metric("Switches SP500", f"{stats_df['nb_switch_sp500'].iloc[0]:.0f}")
            st.metric("Coût SP500", f"{costs_df['cum_switch_cost_SP500'].iloc[-1]*100:.3f}%")
        
        with col2:
            st.metric("Switches Or", f"{stats_df['nb_switch_gold'].iloc[0]:.0f}")
            st.metric("Coût Or", f"{costs_df['cum_switch_cost_GOLD_OZ_USD'].iloc[-1]*100:.3f}%")

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
    
    if costs_df is not None:
        # Timeline des switches
        st.markdown("### Historique des Switches")
        
        switches_sp500 = costs_df[costs_df['switches_SP500'] > 0]
        switches_gold = costs_df[costs_df['switches_GOLD_OZ_USD'] > 0]
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**SP500**")
            if len(switches_sp500) > 0:
                st.dataframe(
                    switches_sp500[['date', 'switches_SP500', 'switch_cost_SP500']].tail(10),
                    use_container_width=True
                )
            else:
                st.info("Aucun switch détecté pour SP500")
        
        with col2:
            st.markdown("**Or**")
            if len(switches_gold) > 0:
                st.dataframe(
                    switches_gold[['date', 'switches_GOLD_OZ_USD', 'switch_cost_GOLD_OZ_USD']].tail(10),
                    use_container_width=True
                )
            else:
                st.info("Aucun switch détecté pour l'Or")

# ============================================
# PAGE 6: PERFORMANCE PAR QUADRANT
# ============================================
elif page == "📊 Performance par Quadrant":
    st.title("Performance des Actifs par Quadrant")
    
    if perf_df is not None and len(perf_df) > 0:
        # Heatmap des rendements mensuels (convertir depuis annualisé)
        st.markdown("### Heatmap des Rendements Mensuels")
        
        # Convertir annual_return en monthly_return (diviser par 12)
        perf_df_copy = perf_df.copy()
        perf_df_copy['monthly_return'] = perf_df_copy['annual_return'] / 12
        
        pivot_returns = perf_df_copy.pivot(index='asset', columns='quadrant', values='monthly_return')
        
        fig = go.Figure(data=go.Heatmap(
            z=pivot_returns.values * 100,
            x=[f'Q{i}' for i in pivot_returns.columns],
            y=pivot_returns.index,
            colorscale='RdYlGn',
            text=pivot_returns.values * 100,
            texttemplate='%{text:.2f}%',
            textfont={"size": 12}
        ))
        
        fig.update_layout(
            title='Rendement Mensuel Moyen par Actif et Quadrant (%)',
            height=500
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Sharpe ratios
        st.markdown("### Sharpe Ratios par Quadrant")
        
        pivot_sharpe = perf_df.pivot(index='asset', columns='quadrant', values='sharpe')
        
        fig = go.Figure(data=go.Heatmap(
            z=pivot_sharpe.values,
            x=[f'Q{i}' for i in pivot_sharpe.columns],
            y=pivot_sharpe.index,
            colorscale='Blues',
            text=pivot_sharpe.values,
            texttemplate='%{text:.2f}',
            textfont={"size": 12}
        ))
        
        fig.update_layout(
            title='Sharpe Ratio par Actif et Quadrant',
            height=500
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Les données de performance par quadrant ne sont pas disponibles. Relancez compute_assets_performance pour générer les résultats.")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center'>
    <p><strong>Stratégie de Trading Macro-Économique</strong></p>
    <p>Projet Data Science - Pipeline Airflow + PySpark + Elasticsearch + Kibana</p>
    <p><a href='https://github.com/Nik1go/Macro-portfolio-4-saisons'>GitHub Repository</a></p>
</div>
""", unsafe_allow_html=True)

