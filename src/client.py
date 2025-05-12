import time 
import requests
from typing import Dict, Any, List
from enum import Enum
from dataclasses import dataclass
import threading
from queue import Queue

class Disposition(Enum):
    EXTERNAL_LINKS = "EXTERNAL_LINKS"
    INLINE = "INLINE"

class Format(Enum):
    JSON = "JSON_ARRAY"
    ARROW = "ARROW_STREAM"

@dataclass
class RequestOpts:
    catalog: str
    schema: str
    warehouse_id: str
    disposition: Disposition
    format: Format 


class QueryExecutor:
    def __init__(self, host: str, token: str):
        host = host.rstrip('/')
        self.base_url = f"https://{host}/api/2.0/sql/statements"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    def _get_statement_status(self, statement_id: str) -> Dict[str, Any]:
        response = requests.get(
            f"{self.base_url}/{statement_id}",
            headers=self.headers
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to get statement status: {response.text}")
            
        return response.json()

    # TODO: consider cancelling if too many poll fails 
    # def cancel_statement(self, statement_id: str) -> Dict[str, Any]:
    #     response = requests.post(
    #         f"{self.base_url}/{statement_id}/cancel",
    #         headers=self.headers
    #     )
        
    #     if response.status_code != 200:
    #         raise Exception(f"Failed to cancel statement: {response.text}")
            
    #     return response.json()

    def _wait_for_response(self, statement_id: str) -> Dict[str, Any]:
        while True:
            status_resp = self._get_statement_status(statement_id)

            if status_resp["status"]["state"] not in ["PENDING", "RUNNING"]:
                return status_resp 

            time.sleep(1) # TODO: fix a poll rate 

    def execute_query(self, query: str, opts: RequestOpts) -> Dict[str, Any]:
        # NOTE: catalog and schema are separate keys in the payload 
        #   in the tutorial, hence not included in query 
        payload = {
            "statement": query,
            "catalog": opts.catalog,
            "schema": opts.schema,
            "warehouse_id": opts.warehouse_id,
            "disposition": opts.disposition.value,
            "format": opts.format.value,
        }
        
        response = requests.post(
            self.base_url,
            headers=self.headers,
            json=payload
        )

        if response.status_code != 200:
            raise Exception(f"Failed to execute query: {response.text}")
        
        statement_id = response.json()["statement_id"]
        return self._wait_for_response(statement_id)
    