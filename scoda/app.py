import streamlit as st
from scoda.api.db import DB
from scoda.api.data import read_csv
from pandas import DataFrame
from pathlib import Path

RELATIVE_EXECUTION_DIR: Path = Path(__file__).parent.resolve()


def initialize_state() -> None:
    # Manages availible database URIs
    if "database_uris" not in st.session_state:
        st.session_state["database_uris"] = [
            "postgresql+psycopg2://admin:example@localhost:5432/research",
        ]

    # Manage dataset paths
    if "dataset_paths" not in st.session_state:
        print(RELATIVE_EXECUTION_DIR)


def main() -> None:
    st.set_page_config(page_title="sc-oda", layout="centered")

    initialize_state()
    quit()

    db: DB | None = None

    st.markdown(
        body="# Supercomputing Operational Data Analytics DB Benchmarking",
    )
    st.markdown(body="> Argonne National Labs, Summer 2025")

    database_uri: str = st.selectbox(
        label="Select Database URI",
        options=DATABASE_URIS,
    )
    if st.button(label="Connect", disabled=True):
        # Here you would add the logic to connect to the database using the selected URI
        st.info(body=f"Attempting to connect to: {database_uri}")
        db = DB(uri=database_uri)
        st.success(body=f"Connected to: {database_uri}")

    if st.button(label="Read data into memory"):
        st.info(body=f"Reading data into memory")
        df: DataFrame = read_csv(dataset="cori_30s")
        st.success(body=f"Read data into memory")

        print(df)


if __name__ == "__main__":
    main()
