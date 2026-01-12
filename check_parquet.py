import pandas as pd
import sys
import os

# Pour afficher toutes les colonnes sans coupure
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 50)


def inspect_parquet(file_path):
    if not os.path.exists(file_path):
        print(f" Erreur : Le fichier n'existe pas : {file_path}")
        return

    try:
        print(f"\n🔍 INSPECTION DE : {file_path}")
        print("=" * 50)

        # Lecture du Parquet
        df = pd.read_parquet(file_path)

        # 1. Infos Générales
        print(f"📊 Dimensions : {df.shape[0]} lignes x {df.shape[1]} colonnes")
        print(f"📅 Dates couvertes : de {df['date'].min()} à {df['date'].max()}")

        # 2. Aperçu du Début (Janvier/Février)
        print("\n--- 5 Premières lignes (Début de l'historique) ---")
        print(df.head(5))

        # 3. Aperçu de la Fin (Le plus important pour toi !)
        print("\n--- 20 Dernières lignes (Temps Réel) ---")
        print(df.tail(20))

        # 4. Check spécifique sur Janvier 2026 (si dispo)
        print("\n--- Zoom sur Janvier 2026 (Si existe) ---")
        try:
            # Filtre basique sur string ou datetime selon ton format
            zoom = df[df['date'].astype(str).str.contains("2026-01")]
            if not zoom.empty:
                print(zoom)
            else:
                print("Pas de données pour 2026-01")
        except:
            pass

    except Exception as e:
        print(f" Erreur de lecture : {e}")


if __name__ == "__main__":
    # Utilisation par défaut si pas d'argument
    default_file = "data/quadrants.parquet"

    # Si tu donnes un argument en ligne de commande, on l'utilise
    if len(sys.argv) > 1:
        target_file = sys.argv[1]
    else:
        target_file = default_file

    inspect_parquet(target_file)