import sqlite3
from pathlib import Path

import pandas as pd


# -----------------------------
# PATHS
# -----------------------------
DATA_PATH = Path("data/processed/final_dataset.csv")
DB_PATH = Path("data/events.db")


# -----------------------------
# LOAD DATA INTO SQLITE
# -----------------------------
def load_to_sqlite(df: pd.DataFrame, db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    df.to_sql("events", conn, if_exists="replace", index=False)
    conn.close()


# -----------------------------
# RUN SAMPLE QUERIES
# -----------------------------
def run_queries(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)

    print("\nTotal number of events:")
    print(pd.read_sql("SELECT COUNT(*) AS total_events FROM events;", conn))

    print("\nTop 10 most expensive weekend-night events:")
    print(pd.read_sql(
        """
        SELECT name, price, venue_name
        FROM events
        WHERE is_weekend_night = 1
        ORDER BY price DESC
        LIMIT 10;
        """,
        conn,
    ))

    conn.close()


# -----------------------------
# MAIN
# -----------------------------
def main() -> None:
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Dataset not found: {DATA_PATH}")

    df = pd.read_csv(DATA_PATH)
    print("Dataset loaded:", df.shape)

    load_to_sqlite(df, DB_PATH)
    print(f"SQLite database created at: {DB_PATH}")

    run_queries(DB_PATH)


if __name__ == "__main__":
    main()
