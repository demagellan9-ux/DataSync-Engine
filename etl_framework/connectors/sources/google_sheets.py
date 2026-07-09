import pandas as pd

from etl_framework.connectors.base import BaseSourceConnector
from etl_framework.logger import audit, get_logger
from etl_framework.models import RawSheet

logger = get_logger()


class GoogleSheetsConnector(BaseSourceConnector):
    def __init__(self, spreadsheet_id: str, service_account_json: str) -> None:
        self._spreadsheet_id      = spreadsheet_id
        self._service_account_json = service_account_json
        self._service             = None   # built once, reused across cycles

    @property
    def source_name(self) -> str:
        return self._spreadsheet_id

    def _get_service(self):
        if self._service is not None:
            return self._service
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        creds = service_account.Credentials.from_service_account_file(
            self._service_account_json,
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
        )
        self._service = build("sheets", "v4", credentials=creds)
        return self._service

    def extract(self) -> list[RawSheet]:
        service     = self._get_service()
        spreadsheet = service.spreadsheets().get(spreadsheetId=self._spreadsheet_id).execute()
        tab_names   = [s["properties"]["title"] for s in spreadsheet["sheets"]]

        result: list[RawSheet] = []
        for tab in tab_names:
            resp = service.spreadsheets().values().get(
                spreadsheetId=self._spreadsheet_id, range=tab
            ).execute()
            rows = resp.get("values", [])
            if not rows or len(rows) < 2:
                audit("extract_skip", sheet=tab, trigger="empty tab")
                continue
            headers = [str(h).strip() for h in rows[0]]
            data    = [
                dict(zip(headers, r + [""] * (len(headers) - len(r))))
                for r in rows[1:]
            ]
            result.append(RawSheet(name=tab, rows=data))
            audit("extract_complete", sheet=tab, rows_total=len(data))

        return result

    def fingerprint(self) -> str:
        """MD5 of all raw cell content — used by the Sheets poller to detect changes."""
        import hashlib
        service     = self._get_service()
        spreadsheet = service.spreadsheets().get(spreadsheetId=self._spreadsheet_id).execute()
        tab_names   = [s["properties"]["title"] for s in spreadsheet["sheets"]]
        hasher      = hashlib.md5()
        for tab in tab_names:
            resp = service.spreadsheets().values().get(
                spreadsheetId=self._spreadsheet_id, range=tab
            ).execute()
            hasher.update(str(resp.get("values", "")).encode())
        return hasher.hexdigest()
