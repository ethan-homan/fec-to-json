import argparse
import json
import psycopg2
import sys
from config import PG_CONFIG, BATCH_SIZE, DATA_DIR
from typing import List, Union

QUERY = open("queries/transaction_flat.sql").read()


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


def run(years: List[int]) -> None:
    """
    :param years: a list of years to
    :return:
    """
    with psycopg2.connect(**PG_CONFIG) as conn:

        for year in years:
            # Set up a server-side cursor since results are in the 10's of GBs.
            with conn.cursor(name="SS_CURSOR") as cursor:

                cursor.execute(QUERY, dict(file_year=year))

                rows = cursor.fetchmany(BATCH_SIZE)
                batch_counter = 1

                # Fetch column names once.
                column_names = [desc[0] for desc in cursor.description]

                # Scroll through one batch at a time.
                while rows:
                    print(f"Year {year} - Batch {batch_counter}")
                    process_batch(
                        batch_rows=rows,
                        batch_column_names=column_names,
                        batch_file_name=f"{DATA_DIR}/{year}/batch-{batch_counter:05d}.json",
                    )
                    rows = cursor.fetchmany(BATCH_SIZE)
                    batch_counter += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flattens FEC data loaded into Postres")
    parser.add_argument(
        '--years',
        help="A list of election cycles to flatten and write to JSON",
        type=int,
        nargs='+',
        choices=[2006, 2008, 2010, 2012, 2014, 2016, 2018, 2020, 2022],
        required=True,
    )
    opts = parser.parse_args()

    year_args: List[int] = opts.years
    run(year_args)
