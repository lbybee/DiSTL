import pandas as pd
import psycopg2
import sys
import os


# params
date = sys.argv[1]
schema = sys.argv[2]
db = sys.argv[3]
section = sys.argv[4]
ngram = int(sys.argv[5])

# set correct db connection
conn = psycopg2.connect(host="localhost", database=db,
                        user="postgres", password="postgres")
cursor = conn.cursor()
cursor.execute("SET search_path TO %s" % schema)
conn.commit()
sql = """SELECT pub_date, headline, %s, doc_id
            FROM doc_%s
      """ % (section, date)
cursor.execute(sql)
full_results = pd.DataFrame(cursor.fetchall(),
                            columns=["pub_date", "headline", section,
                                     "doc_id"])



sql = """SELECT count_%s_%dgram_%s.doc_id, term_label, count FROM
            count_%s_%dgram_%s
            INNER JOIN
            (SELECT term_label, term_id FROM term_%dgram) AS tmp_term
            ON count_%s_%dgram_%s.term_id = tmp_term.term_id
            INNER JOIN
            (SELECT doc_id FROM doc_%s) AS tmp_doc
            ON count_%s_%dgram_%s.doc_id = tmp_doc.doc_id;
      """ % (section, ngram, date, section, ngram, date, ngram,
             section, ngram, date, date, section, ngram, date)
cursor.execute(sql)
count_results = pd.DataFrame(cursor.fetchall(),
                             columns=["doc_id", "term", "count"])
