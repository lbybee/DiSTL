"""
these are the core methods for building a basic DTM from the databse
"""
from joblib import Parallel, delayed
from datetime import datetime
import psycopg2
import logging
import os


def term_query(term_part, db_kwds, schema, out_dir, term_columns,
               gen_term_sql, gen_term_sql_kwds, logger=None):
    """generates a table containing the term_label and term_id for the
    ngrams kept after applying any rules to constrain sql

    Note that we can't use a temporary table here because we need this table
    to exist across sessions so that the count building can happen in
    parallel

    Parameters
    ----------
    term_part : str
        label for current term partition
    db_kwds : dict-like
        key-words to pass to psycopg2 connection
    schema : str
        schema name
    out_dir : str
        location where output files are stored
    term_columns : list
        list of columns for term id files
    gen_term_sql : function
        method for generating term sql
    gen_term_sql_kwds : dict-like
        key-words to pass to term_sql
    logger : python logging instance or None
        if provided, we assume this is where the logs shall be written

    Returns
    -------
    None

    Writes
    ------
    term_id files
    """

    # establish connection
    t0 = datetime.now()
    conn = psycopg2.connect(**db_kwds)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO %s" % schema)
    conn.commit()

    term_sql = gen_term_sql(term_part, **gen_term_sql_kwds)
    sql = """CREATE TABLE tmp_term_%s AS (%s)""" % (term_part, term_sql)

    cursor.execute(sql)
    conn.commit()

    copy_sql = "(SELECT term_label, new_term_id FROM tmp_term_%s)" % term_part

    # now dump table to csv
    with open(os.path.join(out_dir, "term_id_%s.csv" % term_part),
              "w") as fd:
        fd.write(",".join(term_columns) + "\n")
        cursor.copy_to(fd, copy_sql, sep=",")

    if logger:
        t1 = datetime.now()
        logger.info("term,%s,%s\n" % (term_part, str(t1 - t0)))

    cursor.close()
    conn.close()


def drop_temp_term_table(term_part, db_kwds, schema):
    """drops all the temp term tables

    Parameters
    ----------
    term_part : str
        label of temporary term partition table to drop
    db_kwds : dict-like
        key-words to pass to pscopg2 connection
    schema : str
        schema name

    Returns
    -------
    None
    """

    # establish connection
    conn = psycopg2.connect(**db_kwds)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO %s" % schema)
    conn.commit()

    sql = "DROP TABLE IF EXISTS tmp_term_%s;" % term_part
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()



def doc_count_query(doc_part, db_kwds, schema, out_dir,
                    term_partitions, count_partitions, doc_columns,
                    count_columns, gen_full_doc_sql, gen_count_sql,
                    gen_full_doc_sql_kwds, logger=None):
    """selects the counts which meet the search criteria for terms and docs
    and writes them to a set of output files

    Parameters
    ----------
    doc_part : str
        label for current doc partition
    db_kwds : dict-like
        key-words to pass to pscopyg2 connection
    schema : str
        name of schema
    out_dir : str
        location where output files are stored
    term_partitions  : list
        list of term partitions (e.g. 1gram, 2gram,...)
    count_partitions : list
        list of count partitions (e.g. headline, body)
    doc_columns : list
        list of column names for doc_id file
    count_columns : list
        list of column names for count file
    gen_full_doc_sql : function
        method for generating doc sql
    gen_count_sql : function
        method for generating count sql
    gen_full_doc_sql_kwds : dict-like
        key words passed to gen_full_doc_sql
    logger : python logging instance or None
        if provided, we assume this is where the logs shall be written

    Returns
    -------
    None

    Writes
    ------
    1. doc_id files
    2. count files
    """

    t0 = datetime.now()
    conn = psycopg2.connect(**db_kwds)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO %s" % schema)
    conn.commit()

    # prep doc sql and doc_id_sql
    doc_sql, doc_id_sql = gen_full_doc_sql(doc_part, **gen_full_doc_sql_kwds)

    # write doc id
    with open(os.path.join(out_dir, "doc_id_%s.csv" % doc_part),
              "w") as fd:
        fd.write(",".join(doc_columns) + "\n")
        cursor.copy_to(fd, "(%s)" % doc_id_sql, sep=",")

    if logger:
        t1 = datetime.now()
        logger.info("doc,%s,%s\n" % (doc_part, str(t1 - t0)))

    # for each ngram and term label extract counts
    for term_part in term_partitions:
        for count_part in count_partitions:
            t0 = datetime.now()
            count_sql = gen_count_sql(doc_sql, doc_part, term_part,
                                      count_part)
            count_f = os.path.join(out_dir,
                                   "count_%s_%s_%s.csv" % (doc_part,
                                                           term_part,
                                                           count_part))
            with open(count_f, "w") as fd:
                fd.write(",".join(count_columns) + "\n")
                cursor.copy_to(fd, "(%s)" % count_sql, sep=",")

            if logger:
                t1 = datetime.now()
                logger.info("count,%s,%s,%s,%s\n" % (count_part, term_part,
                                                     doc_part, str(t1 - t0)))


def query_wrapper(doc_partitions, term_partitions, count_partitions, db_kwds,
                  schema, doc_columns, term_columns, count_columns, n_jobs,
                  out_dir, gen_full_doc_sql_kwds, gen_term_sql_kwds,
                  logger=None, **kwds):
    """runs a query on the text database to build a DTM

    Parameters
    ----------
    doc_partitions : list
        list of partition labels for doc axis (e.g. dates)
    term_partitions : list
        list of partition labels for term axis (e.g. 1gram, 2gram)
    count_partitions : list
        list of partition labels for different count types (e.g. headline,
        body)
    db_kwds : dict-like
        key-words passed to open psycopg2 conection
    schema : str
        label for DB schema
    doc_columns : list
        list of columns for resulting doc_id files
    term_columns : list
        list of columns for resulting term_id files
    count_columns : list
        list of columns for resulting count files
    n_jobs : int
        number of jobs for multiprocessing
    out_dir : str
        location where output files will be stored
    gen_full_doc_sql_kwds : dictionary
        dict to pass to gen_full_doc_sql method
    gen_term_sql_kwds : dictionary
        dict to pass to gen_term_sql
    logger : python logging instance or None
        if provided, we assume this is where the logs shall be written

    Returns
    -------
    None

    Writes
    ------
    DTM which includes

    1. doc_id files over doc_partitions
    2. term_id files over term_partitions
    3. count files over count_partitions, doc_partitions, and term_partitions
    """

    print(logger)

    # import schema type methods
    if schema == "DJN" or schema == "DJWSJ":
        from .DJ_methods import gen_count_sql, gen_full_doc_sql, gen_term_sql
    else:
        raise NotImplementedError("schema: %s not available for query" %
                                  schema)

    # wipe out temporary tables if they exist
    for term_part in term_partitions:
        drop_temp_term_table(term_part, db_kwds, schema)

    # create tmp term tables and write to the output dir
    Parallel(n_jobs=n_jobs)(
        delayed(term_query)(
            term_part, db_kwds, schema, out_dir, term_columns,
            gen_term_sql, gen_term_sql_kwds, logger)
        for term_part in term_partitions)

    # write doc_id and count files for each doc_part
    Parallel(n_jobs=n_jobs)(
        delayed(doc_count_query)(
            doc_part, db_kwds, schema, out_dir, term_partitions,
            count_partitions, doc_columns, count_columns,
            gen_full_doc_sql, gen_count_sql, gen_full_doc_sql_kwds,
            logger)
        for doc_part in doc_partitions)

    # drop tmp tables
    Parallel(n_jobs=n_jobs)(
        delayed(drop_temp_term_table)(
            term_part, db_kwds, schema)
        for term_part in term_partitions)
