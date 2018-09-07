import pandas as pd
import psycopg2
import sys
import os


# params
ticker = sys.argv[1]
date = sys.argv[2]
schema = sys.argv[3]
db = sys.argv[4]

# set correct db connection
conn = psycopg2.connect(host="localhost", database=db,
                        user="postgres", password="postgres")
cursor = conn.cursor()
cursor.execute("SET search_path TO %s" % schema)
conn.commit()
sql = """SELECT accession_number, headline, display_date,
                body_txt, doc_%s.doc_id FROM
            doc_%s INNER JOIN
            (SELECT doc_id FROM
             tag_link_djn_company_%s INNER JOIN
             (SELECT tag_id FROM tag_djn_company
              WHERE tag_label = '%s'
             ) AS foo
             ON tag_link_djn_company_%s.tag_id = foo.tag_id
            ) AS foobar
            ON doc_%s.doc_id = foobar.doc_id;
      """ % (date, date, date, ticker, date, date)
cursor.execute(sql)
full_results = pd.DataFrame(cursor.fetchall(),
                            columns=["accession_number", "headline",
                                     "display_date", "body_txt",
                                     "doc_id"])


sql = """SELECT count_body_1gram_%s.doc_id, term_label, count FROM
            count_body_1gram_%s
            INNER JOIN
            (SELECT term_label, term_id FROM
             term_1gram
            ) AS tmp_term
            ON count_body_1gram_%s.term_id = tmp_term.term_id
            INNER JOIN
            (SELECT doc_id FROM
             tag_link_djn_company_%s INNER JOIN
             (SELECT tag_id FROM tag_djn_company
              WHERE tag_label = '%s'
             ) AS foo
             ON tag_link_djn_company_%s.tag_id = foo.tag_id
            ) AS tmp_doc
            ON count_body_1gram_%s.doc_id = tmp_doc.doc_id;
      """ % (date, date, date, date, ticker, date, date)
cursor.execute(sql)
body_1gram_results = pd.DataFrame(cursor.fetchall(),
                                  columns=["doc_id", "term", "count"])


sql = """SELECT count_body_2gram_%s.doc_id, term_label, count FROM
            count_body_2gram_%s
            INNER JOIN
            (SELECT term_label, term_id FROM
             term_2gram
            ) AS tmp_term
            ON count_body_2gram_%s.term_id = tmp_term.term_id
            INNER JOIN
            (SELECT doc_id FROM
             tag_link_djn_company_%s INNER JOIN
             (SELECT tag_id FROM tag_djn_company
              WHERE tag_label = '%s'
             ) AS foo
             ON tag_link_djn_company_%s.tag_id = foo.tag_id
            ) AS tmp_doc
            ON count_body_2gram_%s.doc_id = tmp_doc.doc_id;
      """ % (date, date, date, date, ticker, date, date)
cursor.execute(sql)
body_2gram_results = pd.DataFrame(cursor.fetchall(),
                                  columns=["doc_id", "term", "count"])


sql = """SELECT count_headline_1gram_%s.doc_id, term_label, count FROM
            count_headline_1gram_%s
            INNER JOIN
            (SELECT term_label, term_id FROM
             term_1gram
            ) AS tmp_term
            ON count_headline_1gram_%s.term_id = tmp_term.term_id
            INNER JOIN
            (SELECT doc_id FROM
             tag_link_djn_company_%s INNER JOIN
             (SELECT tag_id FROM tag_djn_company
              WHERE tag_label = '%s'
             ) AS foo
             ON tag_link_djn_company_%s.tag_id = foo.tag_id
            ) AS tmp_doc
            ON count_headline_1gram_%s.doc_id = tmp_doc.doc_id;
      """ % (date, date, date, date, ticker, date, date)
cursor.execute(sql)
headline_1gram_results = pd.DataFrame(cursor.fetchall(),
                                      columns=["doc_id", "term", "count"])


sql = """SELECT count_headline_2gram_%s.doc_id, term_label, count FROM
            count_headline_2gram_%s
            INNER JOIN
            (SELECT term_label, term_id FROM
             term_2gram
            ) AS tmp_term
            ON count_headline_2gram_%s.term_id = tmp_term.term_id
            INNER JOIN
            (SELECT doc_id FROM
             tag_link_djn_company_%s INNER JOIN
             (SELECT tag_id FROM tag_djn_company
              WHERE tag_label = '%s'
             ) AS foo
             ON tag_link_djn_company_%s.tag_id = foo.tag_id
            ) AS tmp_doc
            ON count_headline_2gram_%s.doc_id = tmp_doc.doc_id;
      """ % (date, date, date, date, ticker, date, date)
cursor.execute(sql)
headline_2gram_results = pd.DataFrame(cursor.fetchall(),
                                      columns=["doc_id", "term", "count"])
