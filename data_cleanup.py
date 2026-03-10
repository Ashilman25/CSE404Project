import pandas as pd
from pathlib import Path


input_path = Path("data/combined/all_years_combined_with_lebron.csv")
output_folder = Path("data/model_datasets")
output_folder.mkdir(parents=True, exist_ok=True)

target_col = "All_NBA_Target"


df = pd.read_csv(input_path)

df = df[df["Player"] != "League Average"].copy()


id_cols = [
    "Rk",
    "Player",
    "Awards",
    "Player-additional",
    "All_NBA_Label"
]

target_cols = [
    "All_NBA_Target"
]

meta_cols = [
    "Season_End_Year"
]

categorical_cols = [
    "Team",
    "Pos"
]

basic_cols = [
    "Age", "G", "GS", "MP",
    "FG", "FGA", "FG%",
    "3P", "3PA", "3P%",
    "2P", "2PA", "2P%",
    "eFG%",
    "FT", "FTA", "FT%",
    "ORB", "DRB", "TRB",
    "AST", "STL", "BLK",
    "TOV", "PF", "PTS",
    "Trp-Dbl"
]

advanced_cols = [
    "PER", "TS%", "3PAr", "FTr",
    "ORB%", "DRB%", "TRB%",
    "AST%", "STL%", "BLK%",
    "TOV%", "USG%",
    "OWS", "DWS", "WS", "WS/48",
    "OBPM", "DBPM", "BPM", "VORP"
]

impact_cols = [
    "WS", "WS/48", "BPM", "VORP",
    "LEBRON", "O-LEBRON", "D-LEBRON", "WAR"
]

hybrid_trimmed_cols = [
    "Age", "Pos", "G", "GS", "MP",
    "PTS", "TRB", "AST", "STL", "BLK", "TOV",
    "FG%", "3P%", "FT%", "eFG%", "TS%",
    "PER", "USG%",
    "WS", "WS/48", "BPM", "VORP",
    "LEBRON", "O-LEBRON", "D-LEBRON", "WAR",
    "Season_End_Year"
]


def keep_existing(columns, dataframe):
    return [col for col in columns if col in dataframe.columns]

def make_dataset(name, feature_cols, include_categoricals=False):
    cols = []

    if include_categoricals:
        cols += keep_existing(categorical_cols, df)

    cols += keep_existing(feature_cols, df)
    cols += keep_existing(target_cols, df)

    cols = list(dict.fromkeys(cols))

    dataset = df[cols].copy()
    output_path = output_folder / f"{name}.csv"
    dataset.to_csv(output_path, index=False)

    print("=" * 60)
    print(f"Saved {name} : {output_path}")
    print("Shape:", dataset.shape)
    print("Columns:")
    print(dataset.columns.tolist())


full_drop_cols = keep_existing(id_cols, df)
full_dataset = df.drop(columns=full_drop_cols, errors="ignore")
full_output_path = output_folder / "full_dataset.csv"
full_dataset.to_csv(full_output_path, index=False)

print("=" * 60)
print(f"Saved full_dataset : {full_output_path}")
print("Shape:", full_dataset.shape)


make_dataset(
    name="basic_dataset",
    feature_cols=basic_cols + meta_cols,
    include_categoricals=True
)


make_dataset(
    name="advanced_dataset",
    feature_cols=["Age"] + advanced_cols + meta_cols,
    include_categoricals=True
)


make_dataset(
    name="impact_dataset",
    feature_cols=["Age", "G", "GS", "MP"] + impact_cols + meta_cols,
    include_categoricals=True
)

make_dataset(
    name="hybrid_trimmed_dataset",
    feature_cols=hybrid_trimmed_cols,
    include_categoricals=False
)
