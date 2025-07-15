import requests
from requests.auth import HTTPBasicAuth


def set_couchdb_config(
    host: str = "http://localhost:5984",
    node_name: str = "nonode@nohost",
    username: str = "root",
    password: str = "example",
    config_updates: dict = {},
) -> None:
    """
    Set multiple CouchDB config values on a specific node.

    Args:
        host: CouchDB host URL.
        node_name: CouchDB node name.
        username: Admin username.
        password: Admin password.
        config_updates: Dictionary of {section: {key: value}} to update.
    """
    if config_updates is None:
        config_updates = {}

    auth = HTTPBasicAuth(username, password)
    headers = {"Content-Type": "application/json"}

    for section, settings in config_updates.items():
        for key, value in settings.items():
            url = f"{host}/_node/{node_name}/_config/{section}/{key}"
            response = requests.put(url, auth=auth, headers=headers, data=f'"{value}"')

            if response.status_code == 200:
                print(f"✅ Set [{section}][{key}] = {value}")
            else:
                print(
                    f"❌ Failed to set [{section}][{key}]: {response.status_code} - {response.text}"
                )


if __name__ == "__main__":
    config = {
        "couchdb": {
            "max_document_size": 4294967296  # 4 GiB
        },
        "os_processes": {
            "os_process_timeout": 6 * 60 * 1000  # 6 minutes in milliseconds
        },
    }

    set_couchdb_config(config_updates=config)
