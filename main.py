import psycopg2
import sys
from typing import List, Union
import json
from config.config import PG_CONFIG, BATCH_SIZE, DATA_DIR

QUERY = open("queries/transaction_query.sql").read()


def process_batch(
        batch_rows: List[Union[float, str]],
        batch_column_names: List[str],
        batch_file_name: str,
) -> None:

    f = open(batch_file_name, "w")

    for row in batch_rows:

        obj = dict()
        for i, val in enumerate(row):
            if val is not None:
                obj[batch_column_names[i]] = val

        json.dump(obj, f)
        f.write("\n")

    f.close()


if __name__ == "__main__":

    years = sys.argv[1:]

    with psycopg2.connect(**PG_CONFIG) as conn:

        for year in years:

            with conn.cursor(name="SS_CURSOR") as cursor:

                cursor.execute(QUERY, dict(file_year=year))

                rows = cursor.fetchmany(BATCH_SIZE)
                batch_counter = 1
                column_names = [desc[0] for desc in cursor.description]

                while rows:
                    print(f"Year {year} - Batch {batch_counter}")
                    process_batch(
                        batch_rows=rows,
                        batch_column_names=column_names,
                        batch_file_name=f"{DATA_DIR}/{year}/batch-{batch_counter:05d}.json",
                    )
                    rows = cursor.fetchmany(BATCH_SIZE)
                    batch_counter += 1
