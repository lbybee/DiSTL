from datetime import datetime
import multiprocessing
import psycopg2
import os


def create_temp_term_table(db, schema, ngram, threshold, stop_words,
                           regex_stop_words, data_dir, log_file):
    """generates a table containing the term_label and term_id for the
    ngrams kept after applying threshold, stop_words and regex_stop_words
    filters

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
    log_file : str
        location where log is written when done

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


def count_query(db, schema, date, txt_labels, ngram_l, gen_full_doc_sql,
                gen_count_sql, data_dir, log_file,
                gen_full_doc_sql_kwds=None):
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
    txt_labels : list
        list of labels for txt categories
    ngram_l : list
        list of ngram labels
    gen_full_doc_sql : function
        method for generating doc sql
    gen_count_sql : function
        method for generating count sql
    data_dir : str
        location where output is to be stored
    log_file : None or str
        if None location where log is written when done
    gen_full_doc_sql_kwds : dict-like or None
        key words passed to gen_full_doc_sql

    Returns
    -------
    None

    Writes
    ------
    1. doc_id files
    2. count files
    """

    if gen_full_doc_sql_kwds is None:
        gen_full_doc_sql_kwds = {}

    t0 = datetime.now()
    conn = psycopg2.connect(host="localhost", database=db,
                            user="postgres", password="postgres")
    cursor = conn.cursor()
    cursor.execute("SET search_path TO %s" % schema)
    conn.commit()

    # prep doc sql and doc_id_sql
    doc_sql, doc_id_sql = gen_full_doc_sql(date, **gen_full_doc_sql_kwds)

    # write doc id
    with open(os.path.join(data_dir, "doc_id_%s.csv" % date), "w") as fd:
        cursor.copy_to(fd, "(%s)" % doc_id_sql, sep=",")

    t1 = datetime.now()
    with open(log_file, "a") as fd:
        fd.write("doc,%s,%s,%s\n" % (date, str(t1), str(t1 - t0)))

    # for each ngram and term label extract counts
    for ngram in ngram_l:
        for t_label in txt_labels:
            t0 = datetime.now()
            count_sql = gen_count_sql(doc_sql, date, t_label, ngram)

            count_f = os.path.join(data_dir, "count_%s_%s_%s.csv" % (t_label,
                                                                     ngram,
                                                                     date))
            with open(count_f, "w") as fd:
                cursor.copy_to(fd, "(%s)" % count_sql, sep=",")

            t1 = datetime.now()
            with open(log_file, "a") as fd:
                fd.write("count,%s,%s,%s,%s,%s\n" % (t_label, ngram, date,
                                                     str(t1), str(t1 - t0)))


def DTM_wrapper(db, schema, processes, dates, txt_labels, ngram_l,
                gen_full_doc_sql, gen_count_sql, data_dir, log_file,
                ngram_term_threshold_dict, ngram_stop_words_dict,
                ngram_regex_stop_words_dict, gen_full_doc_sql_kwds=None):
    """wrapper called by run script to build DTM

    Parameters
    ----------
    db : str
        name of database
    schema : str
        name of schema
    processes : int
        number of processes for multiprocessing pool
    dates : list
        list of date label
    txt_labels : list
        list of labels for txt categories
    ngram_l : list
        list of ngram labels
    gen_full_doc_sql : function
        method for generating doc sql
    gen_count_sql : function
        method for generating count sql
    data_dir : str
        location where output is to be stored
    log_file : None or str
        if None location where log is written when done
    ngram_term_threshold_dict : dict-like
        dictionary mapping each ngram to a term threshold
    ngram_stop_words_dict : dict-like
        dictionary mapping each ngram to stop words list
    ngram_regex_stop_words_dict : dict-like
        dictionary mapping each ngram to regex stop words list
    gen_full_doc_sql_kwds : dict-like or None
        key words passed to gen_full_doc_sql

    Returns
    -------
    None

    Writes
    ------
    1. doc_id files
    2. count files
    3. term files
    """

    open(log_file, "w")

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
    pool.starmap(count_query, [(db, schema, d, txt_labels, ngram_l,
                                gen_full_doc_sql, gen_count_sql, data_dir,
                                log_file, gen_full_doc_sql_kwds)
                               for d in dates])

    # drop tmp tables
    pool.starmap(drop_temp_term_table, [(db, schema, ngram) for ngram in
                                        ngram_l])
