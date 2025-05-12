import os
import json
import threading
import argparse
from dotenv import load_dotenv
from typing import List
from queue import Queue

from client import QueryExecutor, RequestOpts, Disposition, Format
from results import ResultFetcherFactory, write_fetcher_to_csv
from utils import atomic_print

def run_query(
    id: int,
    query: str,
    catalog: str,
    schema: str,
    warehouse_id: str,
    disposition: Disposition,
    format: Format,
    output_dir: str,
    client: QueryExecutor,
    error_queue: Queue
) -> None:
    try:
        opts = RequestOpts(catalog, schema, warehouse_id, disposition, format)
        results = client.execute_query(query, opts)

        file_name_base = os.path.join(output_dir, f"{catalog}_{schema}_{results['statement_id']}")

        file_name_json = file_name_base + ".json"
        with open(file_name_json, 'w') as f:
            json.dump(results, f, indent=2, sort_keys=True)
        atomic_print(f"{id}: wrote query results to {file_name_json}")

        if results["status"]["state"] != "SUCCEEDED":
            raise Exception(f"{id}: query {results['statement_id']} failed: {results['status']['state']}")

        file_name_csv = file_name_base + ".csv"
        fetcher = ResultFetcherFactory.create_fetcher(results)
        write_fetcher_to_csv(fetcher, file_name_csv)
        atomic_print(f"{id}: wrote data returned to {file_name_csv}")
    except Exception as e:
        error_queue.put(f"{id}: Error in query execution: {str(e)}")
    
def main(disposition: Disposition, format: Format, output_dir: str):
    if format != Format.JSON:
        print("Error: only JSON format is supported")
        return
    if disposition != Disposition.INLINE:
        print("Error: only INLINE disposition is supported")
        return

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    load_dotenv()
    
    warehouse_id = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID")
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    client = QueryExecutor(host, token)

    if not all([host, token, warehouse_id]):
        print("Please set env vars DATABRICKS_HOST, DATABRICKS_TOKEN, and DATABRICKS_SQL_WAREHOUSE_ID")
        return
    
    while True:
        print("-" * 50)

        catalog = input("catalog: ").strip()
        schema = input("schema: ").strip()
        
        queries: List[str] = []
        print("\nEnter your queries (one per line). Enter an empty line when done:")
        while True:
            query = input().strip()
            if not query:
                break
            queries.append(query)
        
        if not queries:
            continue
            
        print(f"executing {len(queries)} queries...")

        # NOTE: Queue is thread-safe  
        error_queue = Queue()
        
        # threads are better for IO-bound tasks, multiprocessing is better for CPU-bound tasks
        threads = []
        for i, query in enumerate(queries):
            thread = threading.Thread(
                target=run_query,
                args=(i + 1, query, catalog, schema, warehouse_id, disposition, format, output_dir, client, error_queue)
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        while not error_queue.empty():
            print(error_queue.get())

        if input("continue? (y/n): ").strip().lower() == "n":
            break

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Execute SQL queries using the Statement Execution API')

    parser.add_argument('--disposition', type=str, default=Disposition.INLINE.value,
                      choices=[d.value for d in Disposition],
                      help='Query disposition')
    parser.add_argument('--format', type=str, default=Format.JSON.value,
                      choices=[f.value for f in Format],
                      help='Query format')
    parser.add_argument('--output-dir', type=str, default='output',
                      help='Directory for output files')
    
    args = parser.parse_args()
    
    main(
        disposition=Disposition(args.disposition),
        format=Format(args.format),
        output_dir=args.output_dir
    ) 