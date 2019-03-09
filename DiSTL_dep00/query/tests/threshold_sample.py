from importlib import import_module
from datetime import datetime
import dask.dataframe as dd
import pandas as pd
import multiprocessing
import psycopg2
import random
import sys
import os


def word_count_sql(date, upper_thresh, lower_thresh):
    """generates the doc sql to select documents above word count"""

    sql = """SELECT headline, body_txt, display_date
                FROM (SELECT headline, body_txt, display_date
                      FROM doc_%s
                      WHERE (body_term_count + headline_term_count) > %d
                      AND (body_term_count + headline_term_count) <= %d
                     ) AS tmp_doc
                WHERE random() < 0.01 limit 100
          """ % (date, lower_thresh, upper_thresh)
    return sql


def temporary_writer(db, schema, date, upper_thresh, lower_thresh,
                     data_dir, log_file):
    """generates all the temporary files"""

    t0 = datetime.now()
    conn = psycopg2.connect(host="localhost", database=db,
                            user="postgres", password="postgres")
    cursor = conn.cursor()
    cursor.execute("SET search_path TO %s" % schema)
    conn.commit()

    sql = word_count_sql(date, upper_thresh, lower_thresh)
    cursor.execute(sql)
    df = pd.DataFrame(cursor.fetchall(), columns=["headline", "body_txt",
                                                  "display_date"])
    df.to_csv(os.path.join(data_dir,
                           "sample_%s_%d.csv" % (date, upper_thresh)),
              index=False)

    t1 = datetime.now()
    with open(log_file, "a") as fd:
        fd.write("%s,%d,%s,%s\n" % (date, upper_thresh, str(t1),
                                    str(t1 - t0)))


schema = sys.argv[1]
query_label = sys.argv[2]


# import schema config parameters
model = "%s_%s" % (schema, query_label)
config = import_module("config.%s.config" % model)

db = config.db
processes = config.processes
dates = config.dates
lower_thresholds = [r for r in range(0, 100, 10)]
upper_thresholds = [r for r in range(10, 110, 10)]
main_dir = "/home/lbybee/scratch_shared/DJN/threshold_test"
if not os.path.exists(main_dir):
    os.mkdir(main_dir)
out_data_dir = os.path.join(main_dir, "temp_sample")
if not os.path.exists(out_data_dir):
    os.mkdir(out_data_dir)
log_file = os.path.join(main_dir, "test_sample.log")

if __name__ == "__main__":

    pool = multiprocessing.Pool(processes)
    pool.starmap(temporary_writer, [(db, schema, d, upper_thresh,
                                     lower_thresh, out_data_dir, log_file)
                                    for d in dates
                                    for upper_thresh, lower_thresh in
                                    zip(upper_thresholds, lower_thresholds)])

    for upper_thresh in upper_thresholds:
        df = dd.read_csv(os.path.join(out_data_dir,
                                      "sample_*_%d.csv" % upper_thresh))
        df = df.compute()
        df = df.sample(100)
        df["headline"] = df["headline"].str.strip()
        df["body_txt"] = df["body_txt"].str.strip()
        df.to_csv(os.path.join(main_dir, "sample_%d.csv" % upper_thresh),
                  index=False)
