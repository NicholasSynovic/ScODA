import pandas as pd
from pandas import DataFrame
from pathlib import Path
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
import seaborn as sns

SUPTITLE_FONT_SIZE: int = 16
TITLE_FONT_SIZE: int = 14
X_Y_LABEL_FONT_SIZE: int = 12


def plot_llnl_last_table_size() -> None:
    data: dict[str, list[str | int | float]] = {
        "dataset": [],
        "records": [],
        "filesize_kb": [],
    }

    csv_files: list[Path] = [
        Path("LAST/Power-Provisioning-Dataset/Cori_power_30_sec.csv").resolve(),
        Path("LAST/Power-Provisioning-Dataset/Hawk_power_15_min.csv").resolve(),
        Path("LAST/Power-Provisioning-Dataset/Lumi_power_10_min.csv").resolve(),
        Path("LAST/Power-Provisioning-Dataset/Marconi100_power_60_sec.csv").resolve(),
        Path("LAST/Power-Provisioning-Dataset/Perlmutter_power_60_sec.csv").resolve(),
        Path("LAST/Power-Provisioning-Dataset/lumi_hpcg_data/lumi_hpcg.csv").resolve(),
        Path("LAST/Power-Provisioning-Dataset/hlrs_hpl_hpcg_data/hpl_uc.csv").resolve(),
        Path(
            "LAST/Power-Provisioning-Dataset/hlrs_hpl_hpcg_data/hpl_spc.csv"
        ).resolve(),
        Path(
            "LAST/Power-Provisioning-Dataset/hlrs_hpl_hpcg_data/hpl_dpc.csv"
        ).resolve(),
        Path(
            "LAST/Power-Provisioning-Dataset/hlrs_hpl_hpcg_data/hpcg_uc.csv"
        ).resolve(),
        Path(
            "LAST/Power-Provisioning-Dataset/hlrs_hpl_hpcg_data/hpcg_dpc.csv"
        ).resolve(),
        Path(
            "LAST/Power-Provisioning-Dataset/hlrs_hpl_hpcg_data/hpcg_spc.csv"
        ).resolve(),
    ]

    dfs: list[DataFrame] = [
        pd.read_csv(filepath_or_buffer=csv_fp) for csv_fp in csv_files
    ]

    idx: int
    for idx in range(len(dfs)):
        df: DataFrame = dfs[idx]
        name: str = csv_files[idx].name

        data["dataset"].append(idx)
        data["records"].append(df.shape[0])
        data["filesize_kb"].append(csv_files[idx].stat().st_size / 1024)

    df: DataFrame = DataFrame(data=data)

    # axes: list[Axes]
    # _, axes = plt.subplots(nrows=1, ncols=2)
    sns.barplot(data=df, x="dataset", y="records", color="steelblue")
    plt.title(label="Records per Dataset", fontsize=TITLE_FONT_SIZE)
    plt.xlabel(xlabel="Dataset", fontsize=X_Y_LABEL_FONT_SIZE)
    plt.ylabel(ylabel="Records (log-scaled)", fontsize=X_Y_LABEL_FONT_SIZE)
    plt.yscale(value="log")

    # sns.barplot(data=df, x="dataset", y="filesize_kb", ax=axes[1])
    # axes[1].set_title(label="File Size per Dataset")
    # axes[1].set_xlabel(xlabel="Dataset")
    # axes[1].set_ylabel(ylabel="File Size (kb)")

    plt.suptitle(t="LLNL/LAST Dataset Hueristics", fontsize=SUPTITLE_FONT_SIZE)
    plt.tight_layout()
    plt.savefig("llnl-last-dataset-heuristics.png")
    plt.clf()
    plt.close(fig="all")

    pass


def plot_llnl_last_total_seconds_to_write_llnl_last_tables() -> None:
    write_all_tables_df: DataFrame = DataFrame()

    benchmark_results: list[Path] = [
        Path("data/mariadb_10.sqlite3").resolve(),
        Path("data/mysql_10.sqlite3").resolve(),
        Path("data/postgres_10.sqlite3").resolve(),
        Path("data/sqlite3_10.sqlite3").resolve(),
        Path("data/sqlite3-memory_10.sqlite3").resolve(),
    ]

    dfs: list[DataFrame] = [
        pd.read_sql_table(
            table_name="benchmark_write_all_tables",
            con=create_engine(url=f"sqlite:///{br}"),
            index_col="id",
        )
        for br in benchmark_results
    ]

    write_all_tables_df["MariaDB"] = dfs[0]["seconds"]
    write_all_tables_df["MySQL"] = dfs[1]["seconds"]
    write_all_tables_df["PostgreSQL"] = dfs[2]["seconds"]
    write_all_tables_df["SQLite3"] = dfs[3]["seconds"]
    write_all_tables_df["SQLite3 (Memory)"] = dfs[4]["seconds"]

    sorted_columns = write_all_tables_df.mean().sort_values(ascending=False).index
    write_all_tables_df = write_all_tables_df[sorted_columns]

    write_all_tables_df["MariaDB"] = write_all_tables_df["MariaDB"].mean()
    write_all_tables_df["MySQL"] = write_all_tables_df["MySQL"].mean()
    write_all_tables_df["PostgreSQL"] = write_all_tables_df["PostgreSQL"].mean()
    write_all_tables_df["SQLite3"] = write_all_tables_df["SQLite3"].mean()
    write_all_tables_df["SQLite3 (Memory)"] = write_all_tables_df[
        "SQLite3 (Memory)"
    ].mean()

    # Create a boxplot using Seaborn
    ax = sns.barplot(data=write_all_tables_df, color="steelblue")

    # Annotate the boxplot with median values
    for i, column in enumerate(write_all_tables_df.columns):
        median_value = write_all_tables_df[column].mean()
        ax.annotate(
            f"{median_value:.5f}",
            xy=(i, median_value),
            xytext=(0, 2),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=8,
            color="black",
        )

    plt.suptitle(t="Total Seconds To Write All Datasets", fontsize=SUPTITLE_FONT_SIZE)
    plt.title(label="Mean Value Shown After 10 Iterations", fontsize=TITLE_FONT_SIZE)
    plt.ylabel(ylabel="Seconds", fontsize=X_Y_LABEL_FONT_SIZE)
    plt.xlabel(xlabel="Database", fontsize=X_Y_LABEL_FONT_SIZE)
    plt.tight_layout()
    plt.savefig("llnl-last-total-seconds-writing-tables.png")
    plt.clf()
    plt.close(fig="all")


def plot_writes_per_table() -> None:
    table_names: list[str] = [
        "Cori_power_30_sec_seconds".lower(),
        "Hawk_power_15_min_seconds".lower(),
        "Lumi_power_10_min_seconds".lower(),
        "Marconi100_power_60_sec_seconds".lower(),
        "Perlmutter_power_60_sec_seconds".lower(),
        "lumi_hpcg_seconds".lower(),
        "hpl_uc_seconds".lower(),
        "hpl_spc_seconds".lower(),
        "hpl_dpc_seconds".lower(),
        "hpcg_uc_seconds".lower(),
        "hpcg_dpc_seconds".lower(),
        "hpcg_spc_seconds".lower(),
    ]

    df: DataFrame = pd.read_sql_table(
        table_name="benchmark_per_table_write",
        con=create_engine(url=f"sqlite:///data/mariadb_10.sqlite3"),
        index_col="id",
    )

    sorted_columns = df.median().sort_values(ascending=False).index

    # Reorder DataFrame columns
    df_sorted = df[sorted_columns].iloc[:, 0:4]
    df_sorted.columns = df_sorted.columns.str.lower()
    df_sorted = df_sorted.rename(
        columns={name: i for i, name in enumerate(table_names)}
    )
    df_sorted[0] = df_sorted[0].mean()
    df_sorted[3] = df_sorted[3].mean()
    df_sorted[4] = df_sorted[4].mean()
    df_sorted[7] = df_sorted[7].mean()

    ax = sns.barplot(data=df_sorted, color="steelblue")

    # Annotate the boxplot with median values
    for i, column in enumerate(df_sorted.columns):
        median_value = df_sorted[column].mean()
        ax.annotate(
            f"{median_value:.5f}",
            xy=(i, median_value),
            xytext=(0, 2),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=8,
            color="black",
        )

    plt.suptitle(
        t="Mean Time To Write Specific Datasets to MariaDB", fontsize=SUPTITLE_FONT_SIZE
    )
    plt.title(
        label="10 Iterations Conducted; Longest 4 Datasets Shown",
        fontsize=TITLE_FONT_SIZE,
    )
    plt.xlabel(xlabel="Datasets", fontsize=X_Y_LABEL_FONT_SIZE)
    plt.ylabel(ylabel="Seconds", fontsize=X_Y_LABEL_FONT_SIZE)

    plt.savefig("llnl-last-time-to-write-specific-datasets-to-mariadb.png")
    plt.clf()
    plt.close()


plot_llnl_last_table_size()
plot_llnl_last_total_seconds_to_write_llnl_last_tables()
plot_writes_per_table()
