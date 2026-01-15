import pandas as pd
import numpy as np


def analyze_quadrant_behavior(file_path):
    # Charger les données (Assure-toi que c'est le fichier avec les indicateurs bruts ou les quadrants journaliers)
    # Si tu as le fichier final avec 'assigned_quadrant', on a besoin des scores bruts pour recalculer le vote
    # Ici, je suppose qu'on charge le fichier qui contient les scores journaliers bruts calculés par Spark
    # Si tu n'as que le assigned_quadrant, c'est dur de voir le "split vote".

    # On va simuler le vote sur la base des quadrants bruts quotidiens
    df = pd.read_parquet('data/US/output_dag/quadrants.parquet') if 'parquet' in file_path else pd.read_csv(file_path)

    # On trie par date
    df = df.sort_values('date')

    # Paramètre de la fenetre
    WINDOW = 18

    # On récupère le quadrant brut du jour (celui calculé par les Z-Scores du jour même)
    # Supposons que la colonne s'appelle 'raw_quadrant_day' ou qu'on la recalcule
    # (Adapte les noms de colonnes selon ton CSV)
    df['day_Q'] = df['assigned_quadrant']  # ATTENTION: Ici il faudrait le quadrant brut du jour avant lissage !
    # Si tu n'as pas le quadrant brut jour par jour dans ton CSV final, ce script ne verra que le résultat lissé.

    # --- SIMULATION DU VOTE ---
    # On crée des dummies pour compter les voix
    dummies = pd.get_dummies(df['day_Q'], prefix='Vote')

    # On compte les votes sur 15 jours glissants
    rolling_votes = dummies.rolling(window=WINDOW).sum()

    anomalies_split_vote = []
    flickering_events = []

    print(f"--- ANALYSE SUR {WINDOW} JOURS ---")

    for i in range(WINDOW, len(df)):
        row = df.iloc[i]
        votes = rolling_votes.iloc[i]
        date = row['date']

        # 1. DETECTER LE SPLIT VOTE (Déni de Crise)
        # Votes Bullish (Q1 + Q2) vs Votes Bearish (Q3 + Q4)
        votes_risk_on = votes.get('Vote_1', 0) + votes.get('Vote_2', 0)
        votes_risk_off = votes.get('Vote_3', 0) + votes.get('Vote_4', 0)

        current_winner = votes.idxmax().replace('Vote_', '')

        # Si la majorité est Risk Off (Q3+Q4 > Q1+Q2) MAIS que le gagnant individuel est Q1 ou Q2
        if votes_risk_off > votes_risk_on and int(current_winner) in [1, 2]:
            anomalies_split_vote.append({
                'date': date,
                'winner': current_winner,
                'votes_Q1': votes.get('Vote_1', 0),
                'votes_Q3': votes.get('Vote_3', 0),
                'votes_Q4': votes.get('Vote_4', 0),
                'total_defensive': votes_risk_off
            })

    # 2. DETECTER LE FLICKERING (Changement incessant)
    # On regarde le quadrant assigné final sur une fenêtre de 5 jours
    df['shifted_1'] = df['assigned_quadrant'].shift(1)
    df['change'] = df['assigned_quadrant'] != df['shifted_1']

    # Somme des changements sur 5 jours glissants
    df['flicker_count'] = df['change'].rolling(5).sum()

    flickering_days = df[df['flicker_count'] >= 3]  # Si ça a changé 3 fois ou plus en 5 jours

    # --- RAPPORT ---
    print(f"\n🚨 CAS DE 'SPLIT VOTE' DÉTECTÉS : {len(anomalies_split_vote)}")
    print("(Cas où Q3 + Q4 sont majoritaires ensemble, mais Q1 gagne tout seul)")
    if anomalies_split_vote:
        print("Exemple des 5 pires cas :")
        for case in anomalies_split_vote[:5]:
            print(f"  Date: {case['date']} | Gagnant: Q{case['winner']} (Votes: {case['votes_Q1']}) "
                  f"| MAIS Q3({case['votes_Q3']}) + Q4({case['votes_Q4']}) = {case['total_defensive']} votes défensifs !")

    print(f"\n😵 PERIODES DE 'FLICKERING' : {len(flickering_days)}")
    print("(Moments où l'algo change d'avis tous les jours)")
    if not flickering_days.empty:
        print(f"Dernières dates : {flickering_days['date'].tail(5).values}")

analyze_quadrant_behavior('data/US/output_dag/quadrants.parquet')