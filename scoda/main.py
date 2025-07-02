from scoda.cli import CLI
import scoda.api.db as scoda_db


def create_db(db_name: str) -> scoda_db.DB | bool:
    match db_name:
        case "postgres":
            return scoda_db.PostgreSQL()
        case "mysql":
            return scoda_db.MySQL()
        case _:
            return False


def main() -> int:
    cli: CLI = CLI()
    args = cli.parse_args().__dict__

    db: scoda_db.DB | bool = create_db(db_name=args["db"][0])
    if isinstance(db, bool):
        return 1

    return 0


if __name__ == "__main__":
    main()
