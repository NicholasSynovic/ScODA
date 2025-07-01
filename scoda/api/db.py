from sqlalchemy import Engine, create_engine, MetaData, Table, Integer, Column


class DB:
    def __init__(self, uri: str) -> None:
        self.uri: str = uri
        self.engine: Engine = create_engine(url=self.uri)
        self.metadata: MetaData = MetaData()

        self.create_tables()

    def create_tables(self) -> None:
        _: Table = Table(
            "cori_power_30_sec",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("timestamp_secs", Integer),
            Column("measured_kW", Integer),
        )

        _: Table = Table(
            "hawk_power_15_min",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("timestamp_secs", Integer),
            Column("measured_kW", Integer),
        )

        _: Table = Table(
            "lumi_power_10_min",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("timestamp_secs", Integer),
            Column("measured_kW", Integer),
        )

        _: Table = Table(
            "marconi100_power_60_sec",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("timestamp_secs", Integer),
            Column("measured_kW", Integer),
        )

        _: Table = Table(
            "perlmutter_power_60_sec",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("timestamp_secs", Integer),
            Column("measured_kW", Integer),
        )

        _: Table = Table(
            "lumi_hpcg",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("timestamp_secs", Integer),
            Column("measured_kW", Integer),
        )

        self.metadata.create_all(bind=self.engine, checkfirst=True)
