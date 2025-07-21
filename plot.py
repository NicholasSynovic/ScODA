import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def plot_benchmark_total_time_to_batch_write_tables(db_dir: Path) -> None:
    # Path to directory containing .sqlite3 files

    # Collect all .sqlite3 files
    db_files = list(db_dir.glob("*.sqlite3"))

    # Load and tag data from each database
    all_data = []
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        df = pd.read_sql_query(
            "SELECT seconds FROM benchmark_total_time_to_batch_write_tables", conn
        )
        df["source"] = db_file.stem.split("_")[
            0
        ]  # Use filename (without extension) as label
        conn.close()
        all_data.append(df)

    # Combine all into one DataFrame
    combined_df = pd.concat(all_data, ignore_index=True)

    # Compute average seconds per database
    mean_seconds = (
        combined_df.groupby("source")["seconds"].mean().sort_values(ascending=False)
    )
    ordered_sources = mean_seconds.index.tolist()

    # Create the box-and-whisker plot
    plt.figure()
    sns.boxplot(x="source", y="seconds", data=combined_df, order=ordered_sources)

    # Customize plot
    plt.title("Batch Data Ingress Performance", fontsize=16)
    plt.xlabel("Database", fontsize=14)
    plt.ylabel("Seconds", fontsize=14)
    # plt.xticks(rotation=45)
    plt.grid(True)

    plt.tight_layout()
    plt.savefig("test.png")


def plot_benchmark_total_time_to_sequential_write_tables(db_dir: Path) -> None:
    # Path to directory containing .sqlite3 files

    # Collect all .sqlite3 files
    db_files = list(db_dir.glob("*.sqlite3"))

    # Load and tag data from each database
    all_data = []
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        df = pd.read_sql_query(
            "SELECT seconds FROM benchmark_total_time_to_sequential_write_tables", conn
        )
        df["source"] = db_file.stem.split("_")[
            0
        ]  # Use filename (without extension) as label
        conn.close()
        all_data.append(df)

    # Combine all into one DataFrame
    combined_df = pd.concat(all_data, ignore_index=True)

    # Compute average seconds per database
    mean_seconds = (
        combined_df.groupby("source")["seconds"].mean().sort_values(ascending=False)
    )
    ordered_sources = mean_seconds.index.tolist()

    # Create the box-and-whisker plot
    plt.figure()
    sns.boxplot(x="source", y="seconds", data=combined_df, order=ordered_sources)

    # Customize plot
    plt.title("Sequential Data Ingress Performance", fontsize=16)
    plt.xlabel("Database", fontsize=14)
    plt.ylabel("Seconds", fontsize=14)
    # plt.xticks(rotation=45)
    plt.grid(True)

    plt.tight_layout()
    plt.savefig("test.png")


if __name__ == "__main__":
    plot_benchmark_total_time_to_sequential_write_tables(
        db_dir=Path("benchmark_results/evaluate")
    )
