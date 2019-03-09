from importlib import import_module
from datetime import datetime
import dask.dataframe as dd
import pandas as pd
import multiprocessing
import psycopg2
import random
import sys
import os


def gen_ticker_docs(date):
    """generates the ticker dates"""

    sql = """SELECT tag_label,
                    doc_%s.doc_id,
                    tmp_tag_doc.tag_id,
                    date(display_date) AS day_date,
                    (headline_term_count + body_term_count) AS term_count
             FROM (SELECT tag_label, tag_link_djn_company_%s.tag_id, doc_id
                   FROM
                   (SELECT tag_label, tag_id
                    FROM tag_djn_company
                   ) AS tmp_tag
                   INNER JOIN tag_link_djn_company_%s
                   ON tmp_tag.tag_id = tag_link_djn_company_%s.tag_id
                  ) AS tmp_tag_doc
             INNER JOIN doc_%s
             ON doc_%s.doc_id = tmp_tag_doc.tag_id
          """ % (date, date, date, date, date, date)
    return sql


def word_count_sql(ticker_sql, threshold):
    """generates the doc sql to select documents above word count"""

    sql = """SELECT doc_id, term_count, day_date, tag_id
                FROM (%s) AS tmp_doc
                WHERE term_count > %d
          """ % (ticker_sql, threshold)
    return sql


def gen_article_count(current_sql):

    sql = """SELECT count(*) FROM
                    (SELECT tag_id
                     FROM (%s) AS foo
                     GROUP BY foo.tag_id, foo.day_date
                    ) AS tmp_doc
          """ % current_sql
    return sql


def gen_unique_ticker_counts(current_sql):
    """gets unique tickers and article counts"""

    sql = """SELECT COUNT(term_count), SUM(term_count)
             FROM (%s) AS tmp_doc
             GROUP BY tmp_doc.tag_id, tmp_doc.day_date
          """ % (current_sql)
    return sql


def temporary_writer(db, schema, date, threshold, data_directory,
                     log_file):
    """generates all the temporary files"""

    t0 = datetime.now()
    conn = psycopg2.connect(host="localhost", database=db,
                            user="postgres", password="postgres")
    cursor = conn.cursor()
    cursor.execute("SET search_path TO %s" % schema)
    conn.commit()

    ticker_sql = gen_ticker_docs(date)
    ticker_sql = word_count_sql(ticker_sql, threshold)

    article_sql = gen_article_count(ticker_sql)
    cursor.execute(article_sql)
    article_df = pd.DataFrame(cursor.fetchall(), columns=["count"])
    article_df.to_csv(os.path.join(data_directory,
                                   "article_%s_%d.csv" % (date, threshold)),
                      index=False)

    ticker_sql = gen_unique_ticker_counts(ticker_sql)
    cursor.execute(ticker_sql)
    ticker_df = pd.DataFrame(cursor.fetchall(), columns=["doc_count",
                                                         "term_count"])
    ticker_df.to_csv(os.path.join(data_directory,
                                  "ticker_%s_%d.csv" % (date, threshold)),
                     index=False)

    t1 = datetime.now()
    with open(log_file, "a") as fd:
        fd.write("%s,%d,%s,%s\n" % (date, threshold, str(t1), str(t1 - t0)))


schema = sys.argv[1]
query_label = sys.argv[2]

# import schema config parameters
model = "%s_%s" % (schema, query_label)
config = import_module("config.%s.config" % model)

db = config.db
processes = config.processes
dates = config.dates
thresholds = [r for r in range(0, 510, 10)]
main_dir = "/home/lbybee/scratch_shared/DJN/threshold_test"
if not os.path.exists(main_dir):
    os.mkdir(main_dir)
out_data_dir = os.path.join(main_dir, "temp_files")
if not os.path.exists(out_data_dir):
    os.mkdir(out_data_dir)
log_file = os.path.join(main_dir, "test.log")

if __name__ == "__main__":

    pool = multiprocessing.Pool(processes)
    pool.starmap(temporary_writer, [(db, schema, d, threshold, out_data_dir,
                                     log_file)
                                    for d in dates
                                    for threshold in thresholds])


    threshold_values = []
    for threshold in thresholds:
        article_df = dd.read_csv(os.path.join(out_data_dir,
                                              "article_*_%d.csv" % threshold))
        article_count = article_df["count"].sum().compute()

        ticker_df = dd.read_csv(os.path.join(out_data_dir,
                                             "ticker_*_%d.csv" % threshold))
        ticker_df = ticker_df.compute()
        res = {"pm_%s_%d" % (l, int(100 * q)): ticker_df[l].quantile(q)
               for l in ["doc_count", "term_count"]
               for q in [0.25, 0.5, 0.75]}
        res["threshold"] = threshold
        res["article_count"] = article_count
        print(res)
        threshold_values.append(res)
    df = pd.DataFrame(threshold_values)
    df["article_count"] = df["article_count"][0] - df["article_count"]
    df["article_diff"] = df["article_count"] - df["article_count"].shift(1)
    df = df[(["threshold", "article_count", "article_diff"] +
             ["pm_%s_%d" % (l, int(100 * q))
              for l in ["doc_count", "term_count"]
              for q in [0.25, 0.5, 0.75]])]
    df.to_csv(os.path.join(main_dir, "aggregates.csv"), index=False)
