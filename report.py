"""Prints and saves a short revenue summary from the gold table."""
from pathlib import Path

import pandas as pd

from src.config import GOLD, REPORTS


def main() -> None:
    df = pd.read_parquet(GOLD)
    latest = df["order_date"].max()
    by_region = (
        df.groupby("region")[["revenue", "orders"]].sum().sort_values("revenue", ascending=False)
    )
    daily = df.groupby("order_date")["revenue"].sum().tail(7)

    text = (
        f"# Revenue report (data up to {latest})\n\n"
        f"## Totals by region\n\n{by_region.to_markdown()}\n\n"
        f"## Last 7 days, all regions\n\n{daily.to_markdown()}\n"
    )
    Path(REPORTS).mkdir(parents=True, exist_ok=True)
    Path(f"{REPORTS}/latest.md").write_text(text)
    print(text)


if __name__ == "__main__":
    main()
