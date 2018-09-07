"""
extracts the ticker months from the DJN schema in postgres

If this is needed again this code should be cleaned somewhat
"""
from datetime import datetime
import pandas as pd
import multiprocessing
import psycopg2
import random
import sys
import os


def tag_query(db, schema, tag_cat, date, log_file = None):
    """runs a query on the tags"""

    t0 = datetime.now()
    conn = psycopg2.connect(host="localhost", database=db,
                            user="postgres", password="postgres")
    cursor = conn.cursor()
    cursor.execute("SET search_path TO %s" % schema)
    conn.commit()

    sql = """SELECT tag_label,
                    tyear,
                    tmonth,
                    term_count,
                    doc_count
                FROM tag_%s_%s
                     INNER JOIN
                     (SELECT tag_id,
                             tyear,
                             tmonth,
                             sum(term_count) AS term_count,
                             count(term_count) AS doc_count
                      FROM
                      (SELECT extract(year FROM display_date) AS tyear,
                              extract(month FROM display_date) AS tmonth,
                              tag_id,
                              term_count
                       FROM doc_%s
                       INNER JOIN tag_link_%s_%s
                       ON doc_%s.doc_id = tag_link_%s_%s.doc_id
                      ) AS tmp_doc
                     GROUP BY (tyear, tmonth, tag_id)
                     ) AS tmp_tag_agg
                ON tag_%s_%s.tag_id = tmp_tag_agg.tag_id
          """ % (tag_cat, date, date, tag_cat, date, date,
                 tag_cat, date, tag_cat, date)

    # now dump table to csv
    f = os.path.join(data_dir, "%s_%s.csv" % (tag_cat, date))
    with open(f, "w") as fd:
        cursor.copy_to(fd, "(%s)" % sql, sep=",")

    if log_file:
        t1 = datetime.now()
        with open(log_file, "a") as fd:
            fd.write("%s,%s,%s\n" % (tag_cat, date, str(t1 - t0)))
    cursor.close()
    conn.close()


# run code
tag_cat = "djn_company"

proj_dir = "/home/lbybee/Dropbox/BK_LB_Projects/DJN_comp_cs_returns"
date_labels = pd.read_csv(os.path.join(proj_dir, "raw", "date_labels.csv"),
                          header=None)[0].tolist()
random.shuffle(date_labels)
data_dir = os.path.join(proj_dir, "intermediate", "company_counts")
log_dir = os.path.join(proj_dir, "logs")
log_file = os.path.join(log_dir, "company_counts.log")
if not os.path.exists(data_dir):
    os.mkdir(data_dir)
if not os.path.exists(log_dir):
    os.mkdir(log_dir)
processes = 12

if __name__ == "__main__":

    pool = multiprocessing.Pool(processes)

    pool.starmap(tag_query, [("test", "DJNmultigrams", "djn_company", d,
                              log_file) for d in date_labels])
