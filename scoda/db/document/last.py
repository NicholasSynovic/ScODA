import scoda.db.document as scoda_document
from requests import get, put, Response, delete, post
from requests.auth import HTTPBasicAuth
import scoda.api.dataset as scoda_dataset


class LAST(scoda_document.DocumentDB):
    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        database: str = "research",
    ):
        self.db_uri: str = self.uri + "/" + self.database
        self.auth: HTTPBasicAuth = HTTPBasicAuth(
            username=self.username,
            password=self.password,
        )
        super().__init__(uri, username, password, database)


class CouchDB(LAST):
    def __init__(self):
        self.headers: dict[str, str] = {"Content-Type": "application/json"}
        super().__init__(
            uri="http://localhost:5984",
            username="root",
            password="example",
            database="research",
        )

    def create(self) -> None:
        resp: Response = get(self.db_uri, auth=self.auth)
        if resp.status_code != 200:
            put(url=self.db_uri, auth=self.auth)

    def recreate(self) -> None:
        delete(url=self.db_uri, auth=self.auth)
        self.create()

    def batch_upload(self, data: scoda_dataset.Dataset) -> None:
        post(
            url=self.db_uri,
            auth=self.auth,
            headers=self.headers,
            data=data.json_str,
        )

    def sequential_upload(self, data: scoda_dataset.Dataset) -> None:
        json_str: str
        for json_str in data.json_list_str:
            post(
                url=self.db_uri,
                auth=self.auth,
                headers=self.headers,
                data=json_str,
            )
