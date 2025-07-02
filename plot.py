import pandas as pd
from pandas import DataFrame
from pathlib import Path
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns

write_all_tables_df: DataFrame = DataFrame()

benchmark_results: list[Path] = [
    Path("postgresql.sqlite3").resolve(),
    Path("mysql.sqlite3").resolve(),
]

write_all_tables_dfs: list[DataFrame] = [
    pd.read_sql_table(
        table_name="benchmark_write_all_tables",
        con=create_engine(url=f"sqlite:///{br}"),
        index_col="id",
    )
    for br in benchmark_results
]

write_all_tables_df["postgres"] = write_all_tables_dfs[0]["seconds"]
write_all_tables_df["mysql"] = write_all_tables_dfs[1]["seconds"]

# Create a boxplot using Seaborn
ax = sns.boxplot(data=write_all_tables_df)

# Annotate the boxplot with median values
for i, column in enumerate(write_all_tables_df.columns):
    median_value = write_all_tables_df[column].median()
    ax.annotate(
        f"{median_value:.5f}",
        xy=(i, median_value),
        xytext=(0, -10),
        textcoords="offset points",
        ha="center",
        va="bottom",
        fontsize=8,
        color="black",
    )

plt.title(label="Benchmark: Write All Tables To Database")
plt.ylabel(ylabel="Seconds")
plt.xlabel(xlabel="Database")
plt.savefig("test.png")
