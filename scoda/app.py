import streamlit as st
from scoda.api.db import DB
import pandas as pd
from pandas import DataFrame
from pathlib import Path

DATA_DIR: Path = Path(
    Path(__file__).parent.parent.resolve(),
    "LAST/Power-Provisioning-Dataset",
)


def initialize_state() -> None:
    # Manages availible database URIs
    if "database_uris" not in st.session_state:
        st.session_state["database_uris"] = [
            "postgresql+psycopg2://admin:example@localhost:5432/research",
        ]

    # Manage dataset paths as a dict formatted {NAME, PATH}
    if "dataset_paths" not in st.session_state:
        st.session_state["dataset_paths"] = {
            "cori_power_30_sec": Path(DATA_DIR, "Cori_power_30_sec.csv")
        }
    if "dataset_df" not in st.session_state:
        st.session_state["dataset_df"] = DataFrame()
    if "dataset_name" not in st.session_state:
        st.session_state["dataset_name"] = ""

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

    # 1. Read data into memory
    st.markdown(body="## Read data into memory")
    dataset_selection: str = st.selectbox(
        label="Select Dataset",
        options=st.session_state["dataset_paths"].keys(),
    )
    if st.button(label="Read Dataset", disabled=False):
        st.toast(body=f"Reading: {database_uri}")
        st.session_state["dataset_name"] = dataset_selection
        st.session_state["dataset_df"] = pd.read_csv(
            filepath_or_buffer=st.session_state["dataset_paths"][dataset_selection],
        )
        st.toast(body=f"Read dataset into memory")
    st.divider()

    # 2. Benchmark writing to database
    st.markdown(body="Benchmark writing data to the database")
    if st.button(
        label="Write data to the database",
        disabled=st.session_state["dataset_df"].empty,
    ):
        st.toast(body=f"Writing to database")
        st.session_state["dataset_df"].to_sql(
            name=st.session_state["dataset_name"],
            con=st.session_state["db"].engine,
            if_exists="append",
            index=False,
        )

        st.toast(body=f"Wrote to database")


if __name__ == "__main__":
    main()
