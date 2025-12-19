import sqlite3
from pathlib import Path

import pandas as pd

# Import functions to test
from src.feature_engineering import categorize_price
from src.data_cleaning import parse_address


# -----------------------------
# PATHS
# -----------------------------
DB_PATH = Path("data/events.db")


# -----------------------------
# UNIT TESTS
# -----------------------------
def test_price_categorization():
    assert categorize_price(0) == "free"
    assert categorize_price(5) == "low"
    assert categorize_price(15) == "medium"
    assert categorize_price(50) == "high"

    print("✅ Price categorization tests passed.")


def is_weekend_event(day_name: str) -> int:
    return 1 if day_name in ["Saturday", "Sunday"] else 0


def test_weekend_logic():
    assert is_weekend_event("Monday") == 0
    assert is_weekend_event("Saturday") == 1
    assert is_weekend_event("Sunday") == 1

    print("✅ Weekend logic tests passed.")


def test_address_parser():
    # Perfect input
    perfect_input = "['Grafton Street', 'Dublin 2']"
    street, line2, city = parse_address(perfect_input)

    assert street == "Grafton Street"
    assert city == "Dublin"

    # Edge case
    broken_input = "[]"
    street, line2, city = parse_address(broken_input)

    assert street is None
    assert city is None

    print("✅ Address parsing tests passed.")


# -----------------------------
# INTEGRATION TEST
# -----------------------------
def test_database_integration():
    if not DB_PATH.exists():
        raise AssertionError("❌ Database file does not exist.")

    conn = sqlite3.connect(DB_PATH)

    df_count = pd.read_sql(
        "SELECT COUNT(*) AS cnt FROM events;",
        conn,
    )

    conn.close()

    count = df_count["cnt"][0]
    assert count > 0, "❌ Integration test failed: database table is empty."

    print(f"✅ Integration test passed: {count} rows found in database.")


# -----------------------------
# DATA VALIDATION
# -----------------------------
def check_event_distribution_by_month():
    conn = sqlite3.connect(DB_PATH)

    query = """
    SELECT
        event_month,
        COUNT(*) AS event_count
    FROM events
    GROUP BY event_month
    ORDER BY event_month;
    """

    df_months = pd.read_sql_query(query, conn)
    conn.close()

    assert not df_months.empty, "❌ No monthly distribution found."

    print("✅ Event month distribution:")
    print(df_months)


# -----------------------------
# MAIN
# -----------------------------
def main():
    print("\nRunning unit tests...")
    test_price_categorization()
    test_weekend_logic()
    test_address_parser()

    print("\nRunning integration tests...")
    test_database_integration()

    print("\nRunning data validation checks...")
    check_event_distribution_by_month()

    print("\n🎉 All tests passed successfully.")


if __name__ == "__main__":
    main()
