from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import csv
from pathlib import Path

class ResultFetcher(ABC):
    @abstractmethod
    def __init__(self, response: Dict[str, Any]):
        if response["status"]["state"] != "SUCCEEDED":
            raise ValueError("Unable to fetch results: query failed")
        
        self._columns = []
        for col in response["manifest"]["schema"]["columns"]:
            self._columns.append(col["name"])

    @abstractmethod
    def get_columns(self) -> List[str]:
        pass

    @abstractmethod
    def get_row(self, row_number: int) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def num_rows(self) -> int:
        pass

class InlineResultFetcher(ResultFetcher):
    def __init__(self, response: Dict[str, Any]):
        super().__init__(response)
        if "result" not in response or "data_array" not in response["result"]:
            raise ValueError("Invalid response format: missing result.data_array")
        

        self._rows =  response["result"]["data_array"]

    def num_rows(self) -> int:
        return len(self._rows)

    def get_columns(self) -> List[str]:
        return self._columns

    def get_row(self, row_number: int) -> Optional[Dict[str, Any]]:
        if not 0 <= row_number < self.num_rows():
            return None
        return self._rows[row_number]

class ResultFetcherFactory:
    @staticmethod
    def create_fetcher(response: Dict[str, Any]) -> ResultFetcher:
        if "result" not in response: 
            raise ValueError("Invalid response format: missing result")
        if "external_links" not in response["result"]:
            return InlineResultFetcher(response)
        raise ValueError(f"unsupported result fetching format")

def write_fetcher_to_csv(fetcher: ResultFetcher, output_path: str) -> None:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        cols = fetcher.get_columns()
        writer.writerow(cols)

        for i in range(fetcher.num_rows()):
            row = fetcher.get_row(i)
            if not row:
                continue 
            writer.writerow(str(val) for val in row)

