import requests
import json
import time
from pathlib import Path

# -------------------------------
# CONFIG IMPORT (LOCAL, NOT COMMITTED)
# -------------------------------
try:
    from config.config_local import (
        STABLE_ID,
        BASE_URL,
        HEADERS,
        COOKIES,
    )
except ImportError as e:
    raise ImportError(
        "config_local.py not found. "
        "Create it from config_template.py and fill in your credentials."
    ) from e


# -------------------------------
# OUTPUT PATH
# -------------------------------
OUTPUT_PATH = Path("data/raw/eventbrite_dublin_live.json")
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)


# -------------------------------
# PAYLOAD BUILDER
# -------------------------------
def build_payload(page: int) -> dict:
    return {
        "browse_surface": "search",
        "event_search": {
            "dates": "current_future",
            "dedup": True,
            "online_events_only": False,
            "places": ["101751737"],  # Dublin place_id
            "page": page,
            "page_size": 20,
        },
        "expand.destination_event": [
            "primary_venue",
            "image",
            "ticket_availability",
            "saves",
            "event_sales_status",
            "primary_organizer",
            "public_collections",
        ],
    }


# -------------------------------
# FETCH SINGLE PAGE
# -------------------------------
def fetch_page(session: requests.Session, page: int) -> dict | None:
    url = f"{BASE_URL}?stable_id={STABLE_ID}"
    payload = build_payload(page)

    response = session.post(url, json=payload, timeout=30)
    print(f"Page {page} status:", response.status_code)

    if response.status_code != 200:
        print("Request failed:", response.text[:300])
        return None

    return response.json()


# -------------------------------
# SCRAPE ALL PAGES
# -------------------------------
def scrape_all(delay_s: float = 0.5) -> list[dict]:
    if not STABLE_ID or STABLE_ID.startswith("PASTE"):
        raise RuntimeError("STABLE_ID is not set. Live fetching is disabled.")

    session = requests.Session()
    session.headers.update(HEADERS)
    session.cookies.update(COOKIES)

    first = fetch_page(session, page=1)
    if first is None:
        raise RuntimeError("Failed on page 1. Check cookies / headers.")

    total_pages = first["events"]["pagination"]["page_count"]
    print("Total pages:", total_pages)

    all_events = first["events"]["results"]

    for page in range(2, total_pages + 1):
        time.sleep(delay_s)
        data = fetch_page(session, page)
        if data:
            all_events.extend(data["events"]["results"])

    print("Total events collected:", len(all_events))
    return all_events


# -------------------------------
# MAIN
# -------------------------------
def main():
    events = scrape_all()

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False, indent=2)

    print(f"Live raw JSON saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
