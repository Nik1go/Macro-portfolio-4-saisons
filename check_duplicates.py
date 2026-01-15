import pandas as pd

try:
    df = pd.read_csv('data/US/output_dag/quadrants.csv')
    print(f"Total rows: {len(df)}")
    print(f"Unique dates: {len(df['date'].unique())}")
    print(df['date'].value_counts().head())
    
    # Check if dates are unique
    if len(df) == len(df['date'].unique()):
        print("✅ Dates are unique.")
    else:
        print("❌ Dates are NOT unique. Duplicates found.")
except Exception as e:
    print(f"Error: {e}")
