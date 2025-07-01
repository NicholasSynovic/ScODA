import streamlit as st
from scoda.api.db import DB
import pandas as pd
from pandas import DataFrame
from pathlib import Path
from collections import defaultdict
from time import time
import pickle

DATA_DIR: Path = Path(
    Path(__file__).parent.parent.resolve(),
    "LAST/Power-Provisioning-Dataset",
)

DATASET_PATHS: dict[str, Path] = {
    "cori_power_30_sec": Path(DATA_DIR, "Cori_power_30_sec.csv"),
    "hawk_power_15_min": Path(DATA_DIR, "Hawk_power_15_min.csv"),
    "lumi_power_10_min": Path(DATA_DIR, "Lumi_power_10_min.csv"),
    "marconi100_power_60_sec": Path(DATA_DIR, "Marconi100_power_60_sec.csv"),
    "perlmutter_power_60_sec": Path(DATA_DIR, "Perlmutter_power_60_sec.csv"),
    "lumi_hpcg": Path(DATA_DIR, "lumi_hpcg_data/lumi_hpcg.csv"),
    "hpcg_dpc": Path(DATA_DIR, "hlrs_hpl_hpcg_data/hpcg_dpc.csv"),
    "hpcg_spc": Path(DATA_DIR, "hlrs_hpl_hpcg_data/hpcg_spc.csv"),
    "hpcg_uc": Path(DATA_DIR, "hlrs_hpl_hpcg_data/hpcg_uc.csv"),
    "hpl_dpc": Path(DATA_DIR, "hlrs_hpl_hpcg_data/hpl_dpc.csv"),
    "hpl_spc": Path(DATA_DIR, "hlrs_hpl_hpcg_data/hpl_spc.csv"),
    "hpl_uc": Path(DATA_DIR, "hlrs_hpl_hpcg_data/hpl_uc.csv"),
}


def initialize_state() -> None:
    # Manages availible database URIs
    if "database_uris" not in st.session_state:
        st.session_state["database_uris"] = [
            "postgresql+psycopg2://admin:example@localhost:5432/research",
            "mysql+pymysql://root:example@localhost:3306/research",
        ]

    if "dataset_dfs" not in st.session_state:
        st.session_state["dataset_dfs"] = defaultdict(DataFrame)

    # Manage database connection
    if "db" not in st.session_state:
        st.session_state["db"] = None
    if "db_connected" not in st.session_state:
        st.session_state["db_connected"] = False


def _update_db_connected() -> None:
    value: bool = not st.session_state["db_connected"]
    st.session_state["db_connected"] = value


def main() -> None:
    # Configure the page
    st.set_page_config(page_title="ScODA", layout="centered")

    # Initialize application statefulness
    initialize_state()

    # Title
    st.markdown(
        body="# ScODA: Supercomputing Operational Data Analytics DB Benchmarking",
    )
    st.markdown(body="> Argonne National Labs, Summer 2025")
    st.divider()

    # 0: Connect to database
    st.markdown(body="## Connect to a database")
    database_uri: str = st.selectbox(
        label="Select Database URI",
        options=st.session_state["database_uris"],
    )
    if st.button(
        label="Connect to database",
        disabled=st.session_state["db_connected"],
        on_click=_update_db_connected,
    ):
        st.toast(body=f"Attempting to connect to: {database_uri}")
        db: DB = DB(uri=database_uri)
        st.session_state["db"] = db
        st.toast(body=f"Connected to: {database_uri}")
    st.divider()

    # 1. Read datasets into memory
    st.markdown(body="## Read datasets into memory")
    if st.button(label="Read Datasets", disabled=False):
        for name, fp in DATASET_PATHS.items():
            _df: DataFrame = pd.read_csv(
                filepath_or_buffer=fp,
            )

            _df.columns = _df.columns.str.replace(pat=" ", repl="_")

            try:
                _df["Time"] = _df["Time"].apply(func=pd.Timestamp)
            except KeyError:
                pass

            st.session_state["dataset_dfs"][name] = _df
        st.toast(body="Read datasets into memory")

    st.divider()

    # 2. Benchmark writing to database
    st.markdown(body="Benchmark writing data to the database")
    if st.button(
        label="Write data to the database",
        disabled=len(st.session_state["dataset_dfs"].items()) < 1,
    ):
        st.toast(body="Writing to database")

        progress_value: int = 0
        progess_text: str = "Writing to database"
        bar = st.progress(value=progress_value, text=progess_text)

        data: dict[str, list[float]] = {"time": []}

        for _ in range(5):
            df_name: str
            df: DataFrame

            start_time: float = time()
            for df_name, df in st.session_state["dataset_dfs"].items():
                df.to_sql(
                    name=df_name,
                    con=st.session_state["db"].engine,
                    if_exists="append",
                    index=False,
                )

            end_time: float = time()
            data["time"].append(end_time - start_time)

            st.session_state["db"].recreate_tables()

            progress_value += 1
            bar.progress(value=progress_value, text=progess_text)

        with open("file.pickle", "wb") as fp:
            pickle.dump(obj=data, file=fp)
            fp.close()

        st.toast(body="Wrote to database")
    st.divider()

    if st.button(label="Recreate database"):
        st.session_state["db"].recreate_tables()


if __name__ == "__main__":
    main()
