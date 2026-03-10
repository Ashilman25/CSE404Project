import pandas as pd


main_path = "data/combined/all_years_combined.csv"
lebron_path = "data/lebron.csv"
output_path = "data/combined/all_years_combined_with_lebron.csv"


main_df = pd.read_csv(main_path)
lebron_df = pd.read_csv(lebron_path)


lebron_df = lebron_df[[
    "Player", "Season", "LEBRON", "O-LEBRON", "D-LEBRON", "WAR"
]].copy()


main_df["Player"] = main_df["Player"].astype(str).str.strip()
lebron_df["Player"] = lebron_df["Player"].astype(str).str.strip()

main_df["Season_End_Year"] = pd.to_numeric(main_df["Season_End_Year"], errors="coerce")
lebron_df["Season"] = pd.to_numeric(lebron_df["Season"], errors="coerce")


merged_df = main_df.merge(
    lebron_df,
    left_on=["Player", "Season_End_Year"],
    right_on=["Player", "Season"],
    how="left"
)

merged_df = merged_df.drop(columns=["Season"])

lebron_ready_df = merged_df.dropna(subset=["LEBRON"]).copy()

lebron_ready_df.to_csv(output_path, index=False)

print(f"Saved merged file to: {output_path}")
