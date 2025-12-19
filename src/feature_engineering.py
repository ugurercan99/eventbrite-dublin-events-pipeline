import ast
import json
from pathlib import Path
from typing import List, Dict, Any
import re

import pandas as pd


# -----------------------------
# PATHS
# -----------------------------
INPUT_PATH = Path("data/processed/events_clean_base.csv")
OUTPUT_PATH = Path("data/processed/final_dataset.csv")


# -----------------------------
# HELPERS
# -----------------------------
def categorize_price(x: float) -> str:
    if x == 0:
        return "free"
    elif x < 10:
        return "low"
    elif x < 30:
        return "medium"
    else:
        return "high"

def build_full_address(row):
    parts = [
        row.get("street"),
        row.get("address_line2"),
        row.get("city_resolved"),
        row.get("eircode"),
    ]

    return ", ".join([p for p in parts if isinstance(p, str) and p.lower() != "unknown"])

def resolve_city(row):
    venue_city = row.get("venue_city")
    location_city = row.get("location_city")

    if isinstance(venue_city, str) and venue_city.lower() != "unknown":
        return venue_city
    return location_city

def dublin_area_cluster(row):
    district = row.get("dublin_postal_district")

    if district is None:
        return "unknown"
    if district <= 2:
        return "city_centre"
    if district in [4, 6]:
        return "south_inner"
    if district >= 7:
        return "outer_dublin"
    return "other"



def safe_json_parse_and_normalize(x) -> List[str]:
    if isinstance(x, str):
        try:
            data = json.loads(x)
        except Exception:
            try:
                data = ast.literal_eval(x)
            except Exception:
                data = []
    else:
        data = x if isinstance(x, list) else []

    return [str(tag).lower().strip() for tag in data]

EIRCODE_PATTERN = re.compile(r"\b([A-Z]\d{2})\s?([A-Z0-9]{4})\b")

def extract_eircode(address: str):
    if not isinstance(address, str):
        return None, None, None

    match = EIRCODE_PATTERN.search(address.upper())
    if not match:
        return None, None, None

    routing_key = match.group(1)
    unique_id = match.group(2)
    full_eircode = f"{routing_key} {unique_id}"

    return full_eircode, routing_key, unique_id


POSTAL_DISTRICT_PATTERN = re.compile(r"DUBLIN\s+(\d{1,2})", re.IGNORECASE)

def extract_dublin_postal_district(address: str):
    if not isinstance(address, str):
        return None

    match = POSTAL_DISTRICT_PATTERN.search(address)
    return int(match.group(1)) if match else None


# -----------------------------
# CORE FEATURE ENGINEERING
# -----------------------------
def engineer_features(df: pd.DataFrame) -> pd.DataFrame:

    # -------------------------
    # Datetime composition
    # -------------------------
    df["start_datetime"] = pd.to_datetime(
        df["start_date"] + " " + df["start_time"], errors="coerce"
    )
    df["end_datetime"] = pd.to_datetime(
        df["end_date"] + " " + df["end_time"], errors="coerce"
    )

    df["duration_hours"] = (
        df["end_datetime"] - df["start_datetime"]
    ).dt.total_seconds() / 3600

    df["published"] = pd.to_datetime(df["published"], errors="coerce")

    # -------------------------
    # Price features
    # -------------------------
    df["price"] = pd.to_numeric(
        df["ticket_availability_minimum_ticket_price_major_value"],
        errors="coerce",
    )

    df["is_free"] = df["price"] == 0
    df["price_category"] = df["price"].apply(categorize_price)

    # -------------------------
    # Column projection
    # -------------------------
    final_columns = [
        "id", "name", "summary", "url", "published",
        "start_datetime", "end_datetime", "duration_hours",
        "primary_venue_name",
        "venue_street", "venue_line2", "venue_city",
        "city", "neighbourhood",
        "price", "is_free", "price_category",
        "tags_list", "tags_string",
    ]

    df = df[final_columns].copy()

    # -------------------------
    # Tag normalization
    # -------------------------
    df["tags_list"] = df["tags_list"].apply(
        safe_json_parse_and_normalize
    )

    
    
    aggregation_map = {
    "tag_music_event": [
        "music", "concert or performance", "livemusic", "concert", "musical",
        "edm / electronic", "techno", "rock", "pop", "indie", "folk", "classical",
        "blues & jazz", "latin", "house", "alternative", "dj", "tribute",
        "livemusicvenue", "liveevent", "choir", "irishmusic", "live"
    ],
    "tag_nightlife_social": [
        "party or social gathering", "party", "nightlife", "nightclub",
        "celebration", "fun", "dance", "dinner or gala", "social"
    ],
    "tag_dating_singles": [
        "dating", "singles", "speeddating", "speed_dating", "matchmaking",
        "matchmaker", "dublin_speed_dating", "speed_date"
    ],
    "tag_education_business": [
        "class, training, or workshop", "workshop", "conference", "business & professional",
        "seminar or talk", "networking", "meeting or networking event",
        "startups & small business", "training", "education", "science & technology",
        "tradeshow, consumer show, or expo"
    ],
    "tag_arts_culture": [
        "performing & visual arts", "art", "painting", "theatre", "performance",
        "film, media & entertainment", "creativity", "creative", "craft", "diy",
        "community & culture", "community", "comedy", "standupcomedy",
        "standup", "comedy_club"
    ],
    "tag_wellness_health": [
        "health & wellness", "yoga", "meditation", "mindfulness", "relaxation",
        "soundbath", "soundhealing", "personal health", "medicine", "wellness",
        "cacao", "religion & spirituality", "women_empowerment"
    ],
    "tag_food_drink": [
        "food & drink", "food", "wine", "cacao"
    ],
    "tag_holiday": [
        "christmas", "seasonal & holiday", "festive", "holiday", "christmas_events",
        "new years eve", "newyearseve", "countdown", "carols"
    ],
    "tag_hobbies_lifestyle": [
        "hobbies & special interest", "home & lifestyle", "tour", "travel",
        "travel & outdoor", "sports & fitness", "family & education",
        "family_friendly", "game or competition", "nature"
    ]
}

    for new_col, tags in aggregation_map.items():
        df[new_col] = df["tags_list"].apply(
            lambda x: 1 if any(tag in x for tag in tags) else 0
        )

    # -------------------------
    # Temporal features
    # -------------------------
    df["event_month"] = df["start_datetime"].dt.month
    df["event_weekday"] = df["start_datetime"].dt.day_name()
    df["event_hour"] = df["start_datetime"].dt.hour
    df["event_minute"] = df["start_datetime"].dt.minute

    df["is_weekend"] = df["event_weekday"].isin(
        ["Saturday", "Sunday"]
    )

    df["is_weekend_night"] = (
        ((df["event_weekday"] == "Friday") |
         (df["event_weekday"] == "Saturday")) &
        (df["event_hour"] >= 18)
    )

    # -------------------------
    # Lead time feature
    # -------------------------
    df["days_published_before_event"] = (
        df["start_datetime"] - df["published"]
    ).dt.days

    # -------------------------
    # Final formatting
    # -------------------------
    rename_map = {
        "primary_venue_name": "venue_name",
        "venue_street": "street",
        "venue_line2": "address_line2",
        "city": "location_city",
        "start_datetime": "start_time",
        "end_datetime": "end_time",
    }

    df = df.rename(columns=rename_map)

    df.columns = (
        df.columns
        .str.lower()
        .str.replace(".", "_", regex=False)
        .str.replace(" ", "_", regex=False)
    )

    df["tags_list"] = df["tags_list"].apply(json.dumps)
    df = df.drop_duplicates(subset="id", keep="first")

    for col in ["is_free", "is_weekend", "is_weekend_night"]:
        df[col] = df[col].astype(int)

    
    df[["eircode", "eircode_routing_key", "eircode_unique_id"]] = (
    df["address_line2"]
    .apply(lambda x: extract_eircode(x))
    .apply(pd.Series)
)
    
    df["dublin_postal_district"] = df["address_line2"].apply(
    extract_dublin_postal_district
)

    df["full_address"] = df.apply(build_full_address, axis=1)

    df["city"] = df.apply(resolve_city, axis=1)

    df["dublin_area_cluster"] = df.apply(
    dublin_area_cluster, axis=1
)
    
    df["price_missing"] = df["price"].isna().astype(int)
    df["price"] = df["price"].fillna(0)
    
    df["eircode"] = df["eircode"].fillna("unknown")
    df["eircode_routing_key"] = df["eircode_routing_key"].fillna("unknown")
    df["eircode_unique_id"] = df["eircode_unique_id"].fillna("unknown")

    df["dublin_postal_district"] = df["dublin_postal_district"].fillna(-1).astype(int)
    df["tags_string"] = df["tags_string"].fillna("")


    return df


def order_columns(df_features: pd.DataFrame) -> pd.DataFrame:
    
      
    # -----------------------------
    # COLUMN ORDERING
    # -----------------------------
    preferred_order = [
    # -------------------------
    # Identifiers & metadata
    # -------------------------
    "id",
    "name",
    "summary",
    "url",

    # -------------------------
    # Time & scheduling
    # -------------------------
    "published",
    "start_time",
    "end_time",
    "duration_hours",
    "days_published_before_event",
    "event_month",
    "event_weekday",
    "event_hour",
    "event_minute",
    "is_weekend",
    "is_weekend_night",

    # -------------------------
    # Location – venue & address
    # -------------------------
    "venue_name",
    "city",
    "neighbourhood",
    "street",
    "address_line2",
    "eircode",
    "eircode_routing_key",
    "eircode_unique_id",
    "dublin_postal_district",
    "full_address",
    "dublin_area_cluster",

    # -------------------------
    # Pricing
    # -------------------------
    "price",
    "is_free",
    "price_category",

    # -------------------------
    # Tags (raw)
    # -------------------------
    "tags_string",
    "tags_list",

    # -------------------------
    # Tags (engineered categories)
    # -------------------------
    "tag_music_event",
    "tag_nightlife_social",
    "tag_dating_singles",
    "tag_education_business",
    "tag_arts_culture",
    "tag_wellness_health",
    "tag_food_drink",
    "tag_holiday",
    "tag_hobbies_lifestyle",
]

    ordered_columns = [c for c in preferred_order if c in df_features.columns]
    df_features = df_features[ordered_columns]
    return df_features
    

# -----------------------------
# MAIN
# -----------------------------
def main() -> None:
    df = pd.read_csv(INPUT_PATH)
    print("Input shape:", df.shape)

    df_features = engineer_features(df)

    df_features = order_columns(df_features)
    
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_features.to_csv(OUTPUT_PATH, index=False)

    print("Output shape:", df_features.shape)
  
    print(f"Feature dataset saved to: {OUTPUT_PATH}")



if __name__ == "__main__":
    main()
