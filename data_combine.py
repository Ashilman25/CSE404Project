import pandas as pd
from io import StringIO
from pathlib import Path


start_year = 2011
end_year = 2025

input_folder = Path("data")
output_folder = Path("data/combined")
output_folder.mkdir(parents=True, exist_ok=True)

master_output_path = output_folder / "all_years_combined.csv"

label_map = {
    "None": 0,
    "NBA3": 1,
    "NBA2": 2,
    "NBA1": 3
}


def split_regular_and_advanced(lines):
    advanced_start_idx = None

    for i, line in enumerate(lines):
        if line.startswith("Rk,Player,Age,Team,Pos,G,GS,MP,PER,TS%"):
            advanced_start_idx = i
            break

    if advanced_start_idx is None:
        raise ValueError("Could not find advanced stats header in the file.")

    regular_csv_text = "".join(lines[:advanced_start_idx])
    advanced_csv_text = "".join(lines[advanced_start_idx:])

    return regular_csv_text, advanced_csv_text


def clean_headers(df):
    if "Rk" in df.columns:
        df = df[df["Rk"] != "Rk"].copy()
    return df.reset_index(drop=True)


def extract_all_nba_label(awards_value):
    if pd.isna(awards_value):
        return "None"

    awards_str = str(awards_value).strip()

    if "NBA1" in awards_str:
        return "NBA1"
    elif "NBA2" in awards_str:
        return "NBA2"
    elif "NBA3" in awards_str:
        return "NBA3"
    else:
        return "None"


def keep_one_season_row(df):
    """
    Keep exactly one row per player:
    - if a total row like 2TM/3TM exists, keep that
    - otherwise keep the first row
    """
    df = df.copy()

    if "Player-additional" in df.columns:
        df = df.dropna(subset=["Player-additional"])

    cleaned_rows = []

    for player_id, group in df.groupby("Player-additional", dropna=False):
        multi_team = group[group["Team"].astype(str).str.endswith("TM", na=False)]

        if not multi_team.empty:
            cleaned_rows.append(multi_team.iloc[0])
        else:
            cleaned_rows.append(group.iloc[0])

    return pd.DataFrame(cleaned_rows).reset_index(drop=True)


def process_one_year(year):
    input_path = input_folder / f"{year}.csv"
    output_path = output_folder / f"{year}_combined.csv"

    if not input_path.exists():
        print(f"Skipping {year}: file not found at {input_path}")
        return None

    print("=" * 60)
    print(f"Processing {year}...")


    with open(input_path, "r", encoding="utf-8") as f:
        lines = f.readlines()


    regular_csv_text, advanced_csv_text = split_regular_and_advanced(lines)


    regular_df = pd.read_csv(StringIO(regular_csv_text))
    advanced_df = pd.read_csv(StringIO(advanced_csv_text))


    regular_df = clean_headers(regular_df)
    advanced_df = clean_headers(advanced_df)


    regular_clean = keep_one_season_row(regular_df)
    advanced_clean = keep_one_season_row(advanced_df)


    identity_cols = [
        "Rk", "Player", "Age", "Team", "Pos",
        "G", "GS", "MP", "Awards", "Player-additional"
    ]

    advanced_extra_cols = [
        col for col in advanced_clean.columns
        if col not in identity_cols
    ]

    combined_df = regular_clean.merge(
        advanced_clean[["Player-additional"] + advanced_extra_cols],
        on="Player-additional",
        how="inner"
    )


    combined_df["All_NBA_Label"] = combined_df["Awards"].apply(extract_all_nba_label)
    combined_df["All_NBA_Target"] = combined_df["All_NBA_Label"].map(label_map)

    combined_df["Season_End_Year"] = year

    combined_df.to_csv(output_path, index=False)

    print("Regular table shape:", regular_df.shape)
    print("Advanced table shape:", advanced_df.shape)
    print("Regular clean shape:", regular_clean.shape)
    print("Advanced clean shape:", advanced_clean.shape)
    print("Combined shape:", combined_df.shape)
    print(f"Saved combined file to: {output_path}")

    return combined_df



all_years = []

for year in range(start_year, end_year + 1):
    try:
        combined_df = process_one_year(year)
        if combined_df is not None:
            all_years.append(combined_df)
    except Exception as e:
        print(f"Failed for {year}: {e}")


if all_years:
    master_df = pd.concat(all_years, ignore_index=True)
    master_df.to_csv(master_output_path, index=False)
    print("=" * 60)
    print(f"Saved master file to: {master_output_path}")
else:
    print("No files were processed successfully.")