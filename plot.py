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
    plt.xticks(rotation=45)
    plt.grid(True)

    plt.tight_layout()
    plt.savefig("plot_benchmark_total_time_to_batch_write_tables.png")


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
    plt.xticks(rotation=45)
    plt.grid(True)

    plt.tight_layout()
    plt.savefig("plot_benchmark_total_time_to_sequential_write_tables.png")


def plot_benchmark_total_time_to_batch_read_tables(db_dir: Path) -> None:
    # Path to directory containing .sqlite3 files

    # Collect all .sqlite3 files
    db_files = list(db_dir.glob("*.sqlite3"))

    # Load and tag data from each database
    all_data = []
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        df = pd.read_sql_query(
            "SELECT seconds FROM benchmark_total_time_to_batch_read_tables", conn
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
    plt.title("Batch Data Egress Performance", fontsize=16)

    plt.xlabel("Database", fontsize=14)
    plt.ylabel("Seconds", fontsize=14)
    plt.xticks(rotation=45)
    plt.grid(True)

    plt.tight_layout()
    plt.savefig("plot_benchmark_total_time_to_batch_read_tables.png")


def plot_benchmark_total_time_to_sequential_read_tables(db_dir: Path) -> None:
    # Path to directory containing .sqlite3 files

    # Collect all .sqlite3 files
    db_files = list(db_dir.glob("*.sqlite3"))

    # Load and tag data from each database
    all_data = []
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        df = pd.read_sql_query(
            "SELECT seconds FROM benchmark_total_time_to_sequential_read_tables", conn
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
    plt.title("Sequential Data Egress Performance", fontsize=16)

    plt.xlabel("Database", fontsize=14)
    plt.ylabel("Seconds", fontsize=14)
    plt.xticks(rotation=45)
    plt.grid(True)

    plt.tight_layout()
    plt.savefig("plot_benchmark_total_time_to_sequential_read_tables.png")


def plot_query_average(db_dir: Path) -> None:
    # Path to directory containing .sqlite3 files

    # Collect all .sqlite3 files
    db_files = list(db_dir.glob("*.sqlite3"))

    # Load and tag data from each database
    all_data = []
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        df = pd.read_sql_query(
            "SELECT seconds FROM benchmark_query_average_value_per_table", conn
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
    plt.title("Average Column Value Query Performance", fontsize=16)

    plt.xlabel("Database", fontsize=14)
    plt.ylabel("Seconds", fontsize=14)
    plt.xticks(rotation=45)
    plt.grid(True)

    plt.tight_layout()
    plt.savefig("plot_query_average.png")


def plot_query_groupby(db_dir: Path) -> None:
    # Path to directory containing .sqlite3 files

    # Collect all .sqlite3 files
    db_files = list(db_dir.glob("*.sqlite3"))

    # Load and tag data from each database
    all_data = []
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        df = pd.read_sql_query(
            "SELECT seconds FROM benchmark_query_groupby_per_table", conn
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
    plt.title("Groupby Value Query Performance", fontsize=16)

    plt.xlabel("Database", fontsize=14)
    plt.ylabel("Seconds", fontsize=14)
    plt.xticks(rotation=45)
    plt.grid(True)

    plt.tight_layout()
    plt.savefig("plot_query_groupby.png")


def plot_query_max(db_dir: Path) -> None:
    # Path to directory containing .sqlite3 files

    # Collect all .sqlite3 files
    db_files = list(db_dir.glob("*.sqlite3"))

    # Load and tag data from each database
    all_data = []
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        df = pd.read_sql_query(
            "SELECT seconds FROM benchmark_query_max_value_per_table", conn
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
    plt.title("Minimum Column Value Query Performance", fontsize=16)

    plt.xlabel("Database", fontsize=14)
    plt.ylabel("Seconds", fontsize=14)
    plt.xticks(rotation=45)
    plt.grid(True)

    plt.tight_layout()
    plt.savefig("plot_query_max.png")


def plot_query_min(db_dir: Path) -> None:
    # Path to directory containing .sqlite3 files

    # Collect all .sqlite3 files
    db_files = list(db_dir.glob("*.sqlite3"))

    # Load and tag data from each database
    all_data = []
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        df = pd.read_sql_query(
            "SELECT seconds FROM benchmark_query_min_value_per_table", conn
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
    plt.title("Maximum Column Value Query Performance", fontsize=16)

    plt.xlabel("Database", fontsize=14)
    plt.ylabel("Seconds", fontsize=14)
    plt.xticks(rotation=45)
    plt.grid(True)

    plt.tight_layout()
    plt.savefig("plot_query_min.png")


def plot_query_mode(db_dir: Path) -> None:
    # Path to directory containing .sqlite3 files

    # Collect all .sqlite3 files
    db_files = list(db_dir.glob("*.sqlite3"))

    # Load and tag data from each database
    all_data = []
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        try:
            df = pd.read_sql_query(
                "SELECT seconds FROM benchmark_query_mode_value_per_table", conn
            )
        except pd.errors.DatabaseError:
            print(db_file)
            quit()

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
    plt.title("Mode Column Value Query Performance", fontsize=16)

    plt.xlabel("Database", fontsize=14)
    plt.ylabel("Seconds", fontsize=14)
    plt.xticks(rotation=45)
    plt.grid(True)

    plt.tight_layout()
    plt.savefig("plot_query_mode.png")


if __name__ == "__main__":
    db_dir = Path("benchmark_results/evaluate")

    plot_query_mode(db_dir)
    plot_query_min(db_dir)
    plot_query_max(db_dir)
    plot_query_groupby(db_dir)
    plot_query_average(db_dir)
    plot_benchmark_total_time_to_sequential_read_tables(db_dir)
    plot_benchmark_total_time_to_batch_read_tables(db_dir)
    plot_benchmark_total_time_to_sequential_write_tables(db_dir)
    plot_benchmark_total_time_to_batch_write_tables(db_dir)
