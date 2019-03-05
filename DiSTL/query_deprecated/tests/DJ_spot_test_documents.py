from importlib import import_module
from datetime import datetime
import pandas as pd
import multiprocessing
import psycopg2
import random
import sys
import os


def word_count_sql(date, threshold):
    """generates the doc sql to select documents above word count"""

    sql = """SELECT doc_id,
                    display_date,
                    headline,
                    body_txt
             FROM doc_%s
             WHERE term_count > %d
          """ % (date, threshold)
    return sql


def weekday_sql(current_sql):
    """constrains to weekdays"""

    sql = """SELECT doc_id,
                    display_date,
                    headline,
                    body_txt
             FROM (%s) AS tmp_doc
             WHERE EXTRACT(week FROM tmp_doc.display_date)
                   NOT IN (6, 7)
          """ % (current_sql)
    return sql


def headline_sql(current_sql, headline_l, author_l):
    """drops headline articles"""

    sql = """SELECT doc_id,
                    display_date,
                    headline,
                    body_txt
             FROM (%s) AS tmp_doc
             WHERE headline !~* %s
             AND headline !~* %s
          """ % (current_sql,
                 "'^(" + "|".join(headline_l) + ").*'",
                 "'.*(" + "|".join(author_l) + ").*'")
    return sql


def gen_tag_sql(date, tag_drop_dict):
    """generates query to identify doc_ids which we want to drop based on
    tags
    """

    sql_full = "SELECT doc_id FROM"
    sql_l = []
    for tag_cat in tag_drop_dict:
        sql = """SELECT doc_id
                    FROM tag_link_%s_%s
                    INNER JOIN
                    (SELECT tag_id
                     FROM tag_%s
                     WHERE tag_label IN (%s)) tmp_tag
                    ON tag_link_%s_%s.tag_id
                       = tmp_tag.tag_id
          """ % (tag_cat, date, tag_cat,
                 ",".join(["'%s'" % t for t in tag_drop_dict[tag_cat]]),
                 tag_cat, date)
        sql_l.append(sql)
    sql_full += " (%s) AS foo" % " UNION ".join(sql_l)
    return sql_full


def gen_tag_doc_sql(doc_sql, tag_sql):
    """generates a query which extracts the document metadata based on the
    tag query and previous doc queries
    """

    sql = """SELECT doc_id,
                    display_date,
                    headline,
                    body_txt
                FROM (%s) l
                WHERE NOT EXISTS
                (SELECT doc_id
                 FROM (%s) r
                 WHERE r.doc_id = l.doc_id
                )
          """ % (doc_sql, tag_sql)
    return sql


def gen_ticker_sql(current_sql, company_id_l, date):
    """selects the tickers which we know have permnos
    """

    sql = """SELECT tmp_doc.doc_id,
                    display_date,
                    headline,
                    body_txt,
                    tag_label
             FROM (SELECT tag_label, tag_link_djn_company_%s.tag_id, doc_id
                   FROM
                   (SELECT tag_label, tag_id
                    FROM tag_djn_company
                    WHERE tag_label IN (%s)
                   ) AS tmp_tag
                   INNER JOIN tag_link_djn_company_%s
                   ON tmp_tag.tag_id = tag_link_djn_company_%s.tag_id
                  ) AS tmp_tag_doc
             INNER JOIN (%s) AS tmp_doc
             ON tmp_tag_doc.doc_id = tmp_doc.doc_id
          """ % (date, ",".join(["'%s'" % t for t in company_id_l]),
                 date, date, current_sql)

    return sql


schema = sys.argv[1]
query_label = sys.argv[2]
date = sys.argv[3]
tmp_dir = sys.argv[4]


# import schema config parameters
model = "%s_%s" % (schema, query_label)
config = import_module("config.%s.config" % model)

db = config.db
processes = config.processes
project_dir = config.project_dir
dates = config.dates
tag_drop_dict = config.tag_drop_dict
headline_l = config.headline_l
author_l = config.author_l
doc_threshold = config.doc_threshold
company_id_l = config.company_id_l
ngram_term_threshold_dict = config.ngram_term_threshold_dict
ngram_stop_words_dict = config.ngram_stop_words_dict
ngram_regex_stop_words_dict = config.ngram_regex_stop_words_dict
ngram_l = [n for n in ngram_term_threshold_dict.keys()]

# set correct db connection
conn = psycopg2.connect(host="localhost", database=db,
                        user="postgres", password="postgres")
cursor = conn.cursor()
cursor.execute("SET search_path TO %s" % schema)
conn.commit()

doc_sql = """SELECT doc_id,
                    display_date,
                    headline,
                    body_txt
             FROM doc_%s""" % date
cursor.execute(doc_sql)
df = pd.DataFrame(cursor.fetchall(), columns=["doc_id", "display_date",
                                              "headline", "body_txt"])
df.to_csv(os.path.join(tmp_dir, "base.csv"), index=False)

# word counts
doc_sql = word_count_sql(date, doc_threshold)
cursor.execute(doc_sql)
df = pd.DataFrame(cursor.fetchall(), columns=["doc_id", "display_date",
                                              "headline", "body_txt"])
df.to_csv(os.path.join(tmp_dir, "term_count.csv"), index=False)

# weekday
doc_sql = weekday_sql(doc_sql)
cursor.execute(doc_sql)
df = pd.DataFrame(cursor.fetchall(), columns=["doc_id", "display_date",
                                              "headline", "body_txt"])
df.to_csv(os.path.join(tmp_dir, "weekend.csv"), index=False)

# headlines
doc_sql = headline_sql(doc_sql, headline_l, author_l)
cursor.execute(doc_sql)
df = pd.DataFrame(cursor.fetchall(), columns=["doc_id", "display_date",
                                              "headline", "body_txt"])
df.to_csv(os.path.join(tmp_dir, "headline.csv"), index=False)

# tags
tag_sql = gen_tag_sql(date, tag_drop_dict)
doc_sql = gen_tag_doc_sql(doc_sql, tag_sql)
cursor.execute(doc_sql)
df = pd.DataFrame(cursor.fetchall(), columns=["doc_id", "display_date",
                                              "headline", "body_txt"])
df.to_csv(os.path.join(tmp_dir, "tags.csv"), index=False)

# tickers
doc_sql = gen_ticker_sql(doc_sql, company_id_l, date)
cursor.execute(doc_sql)
df = pd.DataFrame(cursor.fetchall(), columns=["doc_id", "display_date",
                                              "headline", "body_txt",
                                              "tag_label"])
df.to_csv(os.path.join(tmp_dir, "tickers.csv"), index=False)
