"""
generates a csv containing the number of articles associated with
each headline length from the DJ database
"""
from importlib import import_module
from datetime import datetime
import dask.dataframe as dd
import pandas as pd
import multiprocessing
import psycopg2
import random
import sys
import os


def headline_counter(date):
    """generates headline counts"""

    sql = """SELECT headline_term_count FROM doc_%s""" % date
    return sql


def temporary_writer(db, schema, date, data_directory, log_file):
    """generates all the temporary files"""

    t0 = datetime.now()
    conn = psycopg2.connect(host="localhost", database=db,
                            user="postgres", password="postgres")
    cursor = conn.cursor()
    cursor.execute("SET search_path TO %s" % schema)
    conn.commit()

    sql = headline_counter(date)
    cursor.execute(sql)
    count_df = pd.DataFrame(cursor.fetchall(), columns=["count"])
    count_df.to_csv(os.path.join(data_directory,
                                   "headline_%s.csv" % date),
                      index=False)

    t1 = datetime.now()
    with open(log_file, "a") as fd:
        fd.write("%s,%s,%s\n" % (date, str(t1), str(t1 - t0)))


schema = sys.argv[1]
query_label = sys.argv[2]


# import schema config parameters
model = "%s_%s" % (schema, query_label)
config = import_module("..query_config.%s.config" % model)

db = config.db
processes = config.processes
dates = config.dates
thresholds = [r for r in range(0, 510, 10)]
main_dir = "/home/lbybee/scratch_shared/DJN/headlines"
if not os.path.exists(main_dir):
    os.mkdir(main_dir)
out_data_dir = os.path.join(main_dir, "temp_files")
if not os.path.exists(out_data_dir):
    os.mkdir(out_data_dir)
log_file = os.path.join(main_dir, "test.log")

if __name__ == "__main__":

    pool = multiprocessing.Pool(processes)
    pool.starmap(temporary_writer, [(db, schema, d, out_data_dir, log_file)
                                    for d in dates])

    df = dd.read_csv(os.path.join(out_data_dir, "headline_*.csv"))
    df = df.compute()

    df.to_csv(os.path.join(main_dir, "headlines.csv"), index=False)
