import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pathlib import Path


# -----------------------------
# PATHS
# -----------------------------
INPUT_PATH = Path("data/processed/final_dataset.csv")
OUTPUT_DIR = Path("charts/outputs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# -----------------------------
# CONFIG
# -----------------------------
sns.set_style("whitegrid")


# -----------------------------
# LOAD DATA
# -----------------------------
def load_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input dataset not found: {path}")
    return pd.read_csv(path)


# -----------------------------
# CHARTS
# -----------------------------
def plot_event_category_distribution(df: pd.DataFrame) -> None:
    tag_cols = [
        col for col in df.columns
        if col.startswith("tag_") and col not in {"tags_list", "tags_string"}
    ]

    tag_counts = df[tag_cols].sum().sort_values(ascending=False)

    plt.figure(figsize=(10, 6))
    sns.barplot(x=tag_counts.values, y=tag_counts.index)
    plt.title("Distribution of Event Categories in Dublin")
    plt.xlabel("Number of Events")
    plt.ylabel("Category")
    plt.tight_layout()

    plt.savefig(OUTPUT_DIR / "event_category_distribution.png")
    plt.close()


def plot_price_by_weekday(df: pd.DataFrame) -> None:
    order = [
        "Monday", "Tuesday", "Wednesday",
        "Thursday", "Friday", "Saturday", "Sunday"
    ]

    filtered = df[df["price"] < 100]

    plt.figure(figsize=(10, 6))
    sns.boxplot(
        data=filtered,
        x="event_weekday",
        y="price",
        order=order
    )
    plt.title("Ticket Price Distribution by Day (Events < €100)")
    plt.xlabel("Day of Week")
    plt.ylabel("Price (€)")
    plt.tight_layout()

    plt.savefig(OUTPUT_DIR / "price_by_weekday.png")
    plt.close()


def plot_lead_time_free_vs_paid(df: pd.DataFrame) -> None:
    plt.figure(figsize=(10, 6))
    sns.kdeplot(
        data=df,
        x="days_published_before_event",
        hue="is_free",
        fill=True
    )
    plt.title("Lead Time Distribution: Free vs Paid Events")
    plt.xlabel("Days Published Before Event")
    plt.xlim(0, 100)
    plt.tight_layout()

    plt.savefig(OUTPUT_DIR / "lead_time_free_vs_paid.png")
    plt.close()


def plot_event_density_heatmap(df: pd.DataFrame) -> None:
    heatmap_data = (
        df.groupby(["event_weekday", "event_hour"])
        .size()
        .unstack(fill_value=0)
    )

    days_order = [
        "Monday", "Tuesday", "Wednesday",
        "Thursday", "Friday", "Saturday", "Sunday"
    ]
    heatmap_data = heatmap_data.reindex(days_order)

    plt.figure(figsize=(12, 6))
    sns.heatmap(
        heatmap_data,
        cmap="YlGnBu",
        linewidths=0.5
    )
    plt.title("Event Density Heatmap: Day vs Hour")
    plt.ylabel("Day of Week")
    plt.xlabel("Hour of Day (24h)")
    plt.tight_layout()

    plt.savefig(OUTPUT_DIR / "event_density_heatmap.png")
    plt.close()


def plot_top_venues(df: pd.DataFrame) -> None:
    top_venues = df["venue_name"].value_counts().head(10)

    plt.figure(figsize=(10, 6))
    sns.barplot(
        x=top_venues.values,
        y=top_venues.index
    )
    plt.title("Top 10 Busiest Event Venues in Dublin")
    plt.xlabel("Number of Events")
    plt.ylabel("Venue")
    plt.tight_layout()

    plt.savefig(OUTPUT_DIR / "top_venues.png")
    plt.close()


# -----------------------------
# MAIN
# -----------------------------
def main() -> None:
    df = load_data(INPUT_PATH)
    print("Dataset loaded:", df.shape)

    plot_event_category_distribution(df)
    plot_price_by_weekday(df)
    plot_lead_time_free_vs_paid(df)
    plot_event_density_heatmap(df)
    plot_top_venues(df)

    print(f"Charts saved to: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
