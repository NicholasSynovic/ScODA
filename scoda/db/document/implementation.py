import pandas as pd
import requests
import requests.auth

import scoda.datasets.generic
from scoda.db.document.generic import DocumentDB


class CouchDB(DocumentDB):
    def __init__(self) -> None:
        super().__init__(convert_time_column_to_int=False)
        self.uri: str = "http://localhost:5984/research"
        self.headers: dict[str, str] = {"Content-Type": "application/json"}
        self.auth: requests.auth.HTTPBasicAuth = requests.auth.HTTPBasicAuth(
            username="root",
            password="example",
        )

        self.create()

    def batch_upload(self, dataset: scoda.datasets.generic.Dataset) -> None:
        resp = requests.post(
            url=f"{self.uri}/_bulk_docs",
            auth=self.auth,
            headers=self.headers,
            data='{"docs": ' + dataset.json_data_str + "}",
            timeout=600,
        )

    def batch_read(self, table_name: str) -> None:
        json_data: str = '{"include_docs": True}'
        requests.get(
            url=f"{self.uri}/_all_docs?include_docs=true",
            auth=self.auth,
            headers=self.headers,
            data=json_data,
            timeout=600,
        )

    def create(self) -> None:
        resp: requests.Response = requests.get(
            url=self.uri,
            auth=self.auth,
            timeout=600,
        )
        if resp.status_code != 200:
            requests.put(url=self.uri, auth=self.auth, timeout=600)

    def delete(self) -> None:
        requests.delete(url=self.uri, auth=self.auth, timeout=600)

    def query_average_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None:
        resp: requests.Response = requests.get(
            url=f"{self.uri}/_all_docs?include_docs=true",
            auth=self.auth,
            headers=self.headers,
            timeout=600,
        )

        pd.DataFrame(data=resp.json())["rows"][0]

    def query_groupby_time_window_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None:
        resp: requests.Response = requests.get(
            url=f"{self.uri}/_all_docs?include_docs=true",
            auth=self.auth,
            headers=self.headers,
            timeout=600,
        )

        pd.DataFrame(data=resp.json())["rows"][0]

    def query_max_value(self, table_name: str, column_name: str) -> None:
        resp: requests.Response = requests.get(
            url=f"{self.uri}/_all_docs?include_docs=true",
            auth=self.auth,
            headers=self.headers,
            timeout=600,
        )

        pd.DataFrame(data=resp.json())["rows"][0]

    def query_min_value(self, table_name: str, column_name: str) -> None:
        resp: requests.Response = requests.get(
            url=f"{self.uri}/_all_docs?include_docs=true",
            auth=self.auth,
            headers=self.headers,
            timeout=600,
        )

        pd.DataFrame(data=resp.json())["rows"][0]

    def query_mode_value(self, table_name: str, column_name: str) -> None:
        resp: requests.Response = requests.get(
            url=f"{self.uri}/_all_docs?include_docs=true",
            auth=self.auth,
            headers=self.headers,
            timeout=600,
        )

        pd.DataFrame(data=resp.json())["rows"][0]

    def sequential_read(self, table_name: str, rows: int) -> None:
        idx: int
        for idx in range(rows):
            json_body: str = '{"selector": {"id":' + str(idx) + '}, "limit": 1}'
            requests.post(
                url=f"{self.uri}/_find",
                auth=self.auth,
                headers=self.headers,
                data=json_body,
            )

    def sequential_upload(
        self,
        dataset: scoda.datasets.generic.Dataset,
    ) -> None:
        json_str: str
        for json_str in dataset.json_data_list:
            resp = requests.post(
                url=self.uri,
                auth=self.auth,
                headers=self.headers,
                data=json_str,
                timeout=600,
            )


# class MongoDB(DocumentDB):
#     def batch_upload(self, dataset: scoda.datasets.generic.Dataset) -> None: ...

#     def batch_read(self, table_name: str) -> None: ...

#     def create(self) -> None: ...

#     def delete(self) -> None: ...

#     def query_average_value(
#         self,
#         table_name: str,
#         column_name: str,
#     ) -> None: ...

#     def query_groupby_time_window_value(
#         self,
#         table_name: str,
#         column_name: str,
#     ) -> None: ...

#     def query_max_value(self, table_name: str, column_name: str) -> None: ...

#     def query_min_value(self, table_name: str, column_name: str) -> None: ...

#     def query_mode_value(self, table_name: str, column_name: str) -> None: ...

#     def sequential_read(self, table_name: str, rows: int) -> None: ...

#     def sequential_upload(self, dataset: scoda.datasets.generic.Dataset) -> None: ...
