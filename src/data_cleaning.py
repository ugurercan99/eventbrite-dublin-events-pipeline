import json
import ast
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# -----------------------------
# PATHS
# -----------------------------
INPUT_PATH = Path("data/raw/eventbrite_dublin_live.json")
OUTPUT_PATH = Path("data/processed/events_clean_base.csv")


# -----------------------------
# IO
# -----------------------------
def load_events_json(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def normalize_events(events: List[Dict[str, Any]]) -> pd.DataFrame:
    return pd.json_normalize(events)


# -----------------------------
# HELPERS
# -----------------------------
def drop_fully_null_columns(df: pd.DataFrame) -> pd.DataFrame:
    null_cols = df.columns[df.isna().sum() == len(df)]
    return df.drop(columns=null_cols)


def safe_drop(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    return df.drop(columns=[c for c in cols if c in df.columns])


def extract_from_locations(row: Any, target_type: str) -> Optional[str]:
    try:
        data = ast.literal_eval(row) if isinstance(row, str) else row
    except Exception:
        return None

    if not isinstance(data, list):
        return None

    for item in data:
        if isinstance(item, dict) and item.get("type") == target_type:
            return item.get("name")
    return None


def parse_address(row: Any) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    try:
        data = ast.literal_eval(row) if isinstance(row, str) else row
    except Exception:
        return None, None, None

    if not isinstance(data, list) or len(data) < 2:
        return None, None, None

    street = data[0]
    line2 = data[1]

    city_match = re.search(r"\bDublin\b", str(line2))
    city = "Dublin" if city_match else None

    return street, line2, city


def extract_tags(row: Any) -> Tuple[List[str], str]:
    try:
        data = ast.literal_eval(row) if isinstance(row, str) else row
    except Exception:
        return [], ""

    if not isinstance(data, list):
        return [], ""

    names: List[str] = []
    for item in data:
        if isinstance(item, dict):
            name = item.get("display_name")
            if name:
                names.append(name)

    return names, ", ".join(names)


def to_utc_naive_datetime(series: pd.Series) -> pd.Series:
    """
    Convert to datetime in UTC, then drop timezone to make it tz-naive.
    This prevents tz-aware vs tz-naive errors later.
    """
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    return dt.dt.tz_convert(None)


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.str.lower()
        .str.replace(".", "_", regex=False)
        .str.replace(" ", "_", regex=False)
    )
    return df


# -----------------------------
# PIPELINE
# -----------------------------
def clean_events(df: pd.DataFrame) -> pd.DataFrame:
    # 1) Drop fully-null columns
    df = drop_fully_null_columns(df)

    # 2) Drop noisy nested columns (if they exist)
    df = safe_drop(df, [
        "urgency_signals.messages",
        "urgency_signals.categories",
        "public_collections.creator_collections.collections",
    ])

    # 3) Extract city/neighbourhood from locations
    if "locations" in df.columns:
        df["city"] = df["locations"].apply(lambda x: extract_from_locations(x, "locality"))
        df["neighbourhood"] = df["locations"].apply(lambda x: extract_from_locations(x, "neighbourhood"))

    # 4) Extract venue address parts
    addr_col = "primary_venue.address.localized_multi_line_address_display"
    if addr_col in df.columns:
        df["venue_street"], df["venue_line2"], df["venue_city"] = zip(
            *df[addr_col].apply(parse_address)
        )

    # 5) Extract tags_list + tags_string
    if "tags" in df.columns:
        df["tags_list"], df["tags_string"] = zip(*df["tags"].apply(extract_tags))

    # 6) Drop sales timezone columns (if present)
    df = safe_drop(df, [
        "event_sales_status.start_sales_date.timezone",
        "event_sales_status.start_sales_date.local",
        "event_sales_status.start_sales_date.utc",
        "event_sales_status.end_sales_date.timezone",
        "event_sales_status.end_sales_date.local",
        "event_sales_status.end_sales_date.utc",
        "open_discount.end_date",
    ])

    # 7) Drop the raw nested fields after extraction
    df = safe_drop(df, [
        "locations",
        "tags",
        "primary_venue.address.localized_multi_line_address_display",
    ])

    # 8) Datetime normalization (only if columns exist)
    if "published" in df.columns:
        df["published"] = to_utc_naive_datetime(df["published"])

    # Note: start_datetime is NOT created in cleaning.
    # That is feature-engineering responsibility (date+time -> datetime).

    # 9) Standardize column names
    df = standardize_column_names(df)

    # 10) Fill missing values (only where columns exist)
    if "summary" in df.columns:
        df["summary"] = df["summary"].fillna("")
    if "venue_city" in df.columns:
        df["venue_city"] = df["venue_city"].fillna("unknown")
    if "city" in df.columns:
        df["city"] = df["city"].fillna("unknown")
    if "neighbourhood" in df.columns:
        df["neighbourhood"] = df["neighbourhood"].fillna("unknown")
    
    df = df.drop_duplicates(subset="id", keep="first")

    return df


def main() -> None:
    events = load_events_json(INPUT_PATH)
    df_raw = normalize_events(events)

    print("Shape BEFORE cleaning:", df_raw.shape)

    df_clean = clean_events(df_raw)

    print("Shape AFTER cleaning:", df_clean.shape)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_csv(OUTPUT_PATH, index=False)
    
    print(f"Cleaned data saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
