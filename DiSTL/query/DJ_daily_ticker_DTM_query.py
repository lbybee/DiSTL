"""
generates a ticker-day DTM from the DJ schemas in postgres
"""
from importlib import import_module
from datetime import datetime
import multiprocessing
import psycopg2
import random
import sys
import os


def gen_tag_sql(date, tag_drop_dict):
    """generates query to identify doc_ids which we want to drop based on
    tags

    Parameters
    ----------
    date : str
        date for corresponding tables
    tag_drop_dict : dict-like
        dictionary containing tag categories to drop (keys) as well as list of
        actual tags to drop (values)

    Returns
    -------
    string corresponding to sql query
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


def gen_tag_doc_sql(sql, date):
    """generates a query which extracts the document metadata based on the
    tag query

    Parameters
    ----------
    sql : str
        query returned by gen_tag_query
    date : str
        date for current tables

    Returns
    -------
    updated sql query
    """

    sql = """SELECT accession_number, headline, display_date, doc_id,
                    headline_term_count, body_term_count
                FROM doc_%s l
                WHERE NOT EXISTS
                (SELECT doc_id
                 FROM (%s) r
                 WHERE r.doc_id = l.doc_id
                )
          """ % (date, sql)
    return sql


def gen_doc_sql(sql, headline_l, author_l, date):
    """
    adds the metadata queries to the results returned by gen_tag_doc_query

    This does the following:

    1. remove weekends
    2. drop documents matching headline start patterns
    3. drop documents containing author patterns
    4. drop documents below term_count threshold

    Parameters
    ----------
    sql : str
        query returned by gen_tag_doc_query
    headline_l : list
        list of headline patterns to drop
    author_l : list
        list of author patterns to drop
    date : str
        date label for current table

    Returns
    -------
    update sql query
    """

    sql = """SELECT doc_id,
                    display_date,
                    headline_term_count,
                    body_term_count
                FROM (%s) AS foo
                WHERE EXTRACT(DOW FROM display_date) NOT IN (6, 0)
                AND headline !~* %s
                AND headline !~* %s
          """ % (sql,
                 "'^(" + "|".join(headline_l) + ").*'",
                 "'.*(" + "|".join(author_l) + ").*'")

    return sql


def gen_company_sql(doc_sql, company_id_l, date):
    """joins the desired company ids with the doc sql

    Parameters
    ----------
    doc_sql : str
        query returned by gen_doc_sql
    company_id_l : list of str
        list of tickers to extract
    date : str
        date label

    Returns
    -------
    updated query
    """

    sql = """SELECT tag_label,
                    date(display_date) AS day_date,
                    tmp_doc.doc_id,
                    (tmp_doc.headline_term_count
                     + tmp_doc.body_term_count) AS term_count,
                    dense_rank()
                    OVER(ORDER BY tag_label,
                        date(display_date))
                    AS new_doc_id
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
                 date, date, doc_sql)

    return sql


def gen_threshold_sql(doc_sql, threshold):
    """selects the permno-days from the company sql which have more
    terms than the threshold

    Parameters
    ----------
    doc_sql : str
        query returned by gen_company_sql
    threshold : int
        term count threshold for permno-day

    Returns
    -------
    updated sql query
    """

    sql = """SELECT tag_label,
                    day_date,
                    doc_id,
                    main_doc.new_doc_id
             FROM (%s) AS main_doc
             LEFT JOIN (SELECT new_doc_id
                        FROM
                        (SELECT new_doc_id,
                                sum(term_count) AS term_count
                         FROM (%s) AS foo
                         GROUP BY new_doc_id
                        ) AS foobar
                        WHERE term_count > %d
                       ) AS tmp_doc
            ON main_doc.new_doc_id = tmp_doc.new_doc_id
          """ % (doc_sql, doc_sql, threshold)
    return sql


def gen_doc_id_sql(company_sql):
    """generates sql that can be dumped as doc id (by grouping by keys)"""

    sql = """SELECT DISTINCT ON (new_doc_id)
                tag_label, day_date, new_doc_id
                FROM (%s) as tmp_doc""" % company_sql
    return sql


def create_temp_term_table(db, schema, ngram, threshold, stop_words,
                           regex_stop_words, data_dir, log_file = None):
    """generates a table containing the term_label and
    term_id for the ngrams kept after applying threshold,
    stop_words and regex_stop_words filters

    Note that we can't use a temporary table here because we need this table
    to exist across sessions so that the count building can happen in
    parallel

    Parameters
    ----------
    db : str
        database name
    schema : str
        schema name
    ngram : str
        label for ngram e.g. 1gram, 2gram
    threshold : int
        count below which we drop terms
    stop_words : list
        list of stop words
    regex_stop_words : list
        list of regex stop words
    data_dir : str
        location where output file will be stored
    log_file : None or str
        if None location where log is written when done

    Returns
    -------
    None
    """

    # establish connection
    t0 = datetime.now()
    conn = psycopg2.connect(host="localhost", database=db,
                            user="postgres", password="postgres")
    cursor = conn.cursor()
    cursor.execute("SET search_path TO %s" % schema)
    conn.commit()

    sql = """CREATE TABLE tmp_term_%s AS
                SELECT term_label, term_id,
                       dense_rank() OVER(ORDER BY term_id) AS new_term_id
                FROM term_%s
                WHERE doc_count > %d
                AND term_label NOT IN (%s)
                AND term_label !~ %s
          """ % (ngram, ngram, threshold,
                 ",".join(["'%s'" % w for w in stop_words]),
                 "'.*(" + "|".join(regex_stop_words) + ").*'")

    cursor.execute(sql)
    conn.commit()

    copy_sql = "(SELECT term_label, new_term_id FROM tmp_term_%s)" % ngram

    # now dump table to csv
    with open(os.path.join(data_dir, "term_id_%s.csv" % ngram), "w") as fd:
        cursor.copy_to(fd, copy_sql, sep=",")

    if log_file:
        t1 = datetime.now()
        with open(log_file, "a") as fd:
            fd.write("term,%s,%s,%s\n" % (ngram, str(t1), str(t1 - t0)))
    cursor.close()
    conn.close()


def drop_temp_term_table(db, schema, ngram):
    """drops all the temp term tables

    Parameters
    ----------
    db : str
        database name
    schema : str
        schema name
    ngram : str
        label of ngram to drop

    Returns
    -------
    None
    """

    # establish connection
    conn = psycopg2.connect(host="localhost", database=db,
                            user="postgres", password="postgres")
    cursor = conn.cursor()
    cursor.execute("SET search_path TO %s" % schema)
    conn.commit()

    sql = "DROP TABLE tmp_term_%s;" % ngram
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()


def gen_count_sql(doc_sql, t_label, ngram, date):
    """joins the temporary term and doc tables with the counts and
    dumps the results to the provided file

    Parameters
    ----------
    sql : str
        current sql query
    t_label : str
        label for current txt category
    ngram : str
        ngram label
    date : str
        date label

    Returns
    -------
    update sql for writing counts
    """

    sql = """SELECT tmp_doc.new_doc_id, tmp_term_%s.new_term_id, sum(count)
                FROM count_%s_%s_%s
                INNER JOIN (%s) AS tmp_doc
                ON count_%s_%s_%s.doc_id = tmp_doc.doc_id
                INNER JOIN tmp_term_%s
                ON count_%s_%s_%s.term_id = tmp_term_%s.term_id
                GROUP BY tmp_doc.new_doc_id, tmp_term_%s.new_term_id
            """ % (ngram, t_label, ngram, date, doc_sql, t_label, ngram,
                   date, ngram, t_label, ngram, date, ngram, ngram)

    return sql


def count_query(db, schema, date, tag_drop_dict, headline_l, author_l,
                doc_threshold, txt_labels, ngram_l, company_id_l, data_dir,
                log_file = None):
    """selects the counts which meet the search criteria for terms and docs
    and writes them to a set of output files

    Parameters
    ----------
    db : str
        name of database
    schema : str
        name of schema
    date : str
        date label
    tag_drop_dict : dict-like
        dictionary containing tag categories and drop lists
    headline_l : list
        list of headlines to drop
    author_l : list
        list of authors to drop
    doc_threshold : int
        term threshold for dropping documents
    txt_labels : list
        list of labels for txt categories (headline, body)
    ngram_l : list
        list of ngram labels
    company_id_l : list of str
        list of tickers to extract
    data_dir : str
        location where output is to be stored
    log_file : None or str
        if None location where log is written when done

    Returns
    -------
    None
    """

    t0 = datetime.now()
    conn = psycopg2.connect(host="localhost", database=db,
                            user="postgres", password="postgres")
    cursor = conn.cursor()
    cursor.execute("SET search_path TO %s" % schema)
    conn.commit()

    # write docs for corresponding date
    doc_sql = gen_tag_sql(date, tag_drop_dict)
    doc_sql = gen_tag_doc_sql(doc_sql, date)
    doc_sql = gen_doc_sql(doc_sql, headline_l, author_l, date)
    doc_sql = gen_company_sql(doc_sql, company_id_l, date)
    doc_sql = gen_threshold_sql(doc_sql, doc_threshold)
    with open(os.path.join(data_dir, "doc_id_%s.csv" % date), "w") as fd:
        cursor.copy_to(fd, "(%s)" % gen_doc_id_sql(doc_sql), sep=",")
    if log_file:
        t1 = datetime.now()
        with open(log_file, "a") as fd:
            fd.write("doc,%s,%s,%s\n" % (date, str(t1), str(t1 - t0)))

    for ngram in ngram_l:
        for t_label in txt_labels:
            t0 = datetime.now()
            count_sql = gen_count_sql(doc_sql, t_label, ngram, date)

            count_f = os.path.join(data_dir, "count_%s_%s_%s.csv" % (t_label,
                                                                     ngram,
                                                                     date))
            with open(count_f, "w") as fd:
                cursor.copy_to(fd, "(%s)" % count_sql, sep=",")

        if log_file:
            t1 = datetime.now()
            with open(log_file, "a") as fd:
                fd.write("count,%s,%s,%s,%s,%s\n" % (t_label, ngram, date,
                                                     str(t1), str(t1 - t0)))


# params
schema = sys.argv[1]
query_label = sys.argv[2]

if __name__ == "__main__":

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
    txt_labels = config.txt_labels
    ngram_term_threshold_dict = config.ngram_term_threshold_dict
    ngram_stop_words_dict = config.ngram_stop_words_dict
    ngram_regex_stop_words_dict = config.ngram_regex_stop_words_dict
    ngram_l = [n for n in ngram_term_threshold_dict.keys()]
    company_id_l = config.company_id_l

    # shuffle date labels to avoid skew with multiprocessing
    random.shuffle(dates)

    # initialize directories
    mod_dir = os.path.join(project_dir, model)
    if not os.path.exists(mod_dir):
        os.mkdir(mod_dir)
    data_dir = os.path.join(mod_dir, "DTM")
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)
    log_dir = os.path.join(mod_dir, "logs")
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    log_file = os.path.join(log_dir, "DTM.log")
    open(log_file, "a")

    # initialize multiprocessing
    pool = multiprocessing.Pool(processes)

    # create tmp term tables and save
    pool.starmap(create_temp_term_table, [(db, schema, ngram,
                                           ngram_term_threshold_dict[ngram],
                                           ngram_stop_words_dict[ngram],
                                           ngram_regex_stop_words_dict[ngram],
                                           data_dir, log_file)
                                          for ngram in ngram_l])

    # count (and doc) query
    pool.starmap(count_query, [(db, schema, d, tag_drop_dict, headline_l,
                                author_l, doc_threshold, txt_labels, ngram_l,
                                company_id_l, data_dir, log_file)
                               for d in dates])

    # drop tmp tables
    pool.starmap(drop_temp_term_table, [(db, schema, ngram) for ngram in
                                        ngram_l])
