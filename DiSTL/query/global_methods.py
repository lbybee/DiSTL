from datetime import datetime
import psycopg2


def create_temp_term_table(schema, db_kwds, output_file, term_part,
                           term_columns, threshold, stop_words,
                           regex_stop_words, log_file):
    """generates a table containing the term_label and term_id for the
    ngrams kept after applying threshold, stop_words and regex_stop_words
    filters

    Note that we can't use a temporary table here because we need this table
    to exist across sessions so that the count building can happen in
    parallel

    Parameters
    ----------
    schema : str
        schema name
    db_kwds : dict-like
        key-words to pass to pscopg2 connection
    output_file : luigi file object
        where we want to write the term id for the corresponding term_part
    term_part : str
        label for current term partition (e.g. 1gram, 2gram)
    term_columns : list
        labels for term_id file
    threshold : int
        count below which we drop terms
    stop_words : list
        list of stop words
    regex_stop_words : list
        list of regex stop words
    log_file : str
        location where log is written when done

    Returns
    -------
    None
    """

    # establish connection
    t0 = datetime.now()
    conn = psycopg2.connect(**db_kwds)
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
          """ % (term_part, term_part, threshold,
                 ",".join(["'%s'" % w for w in stop_words]),
                 "'.*(" + "|".join(regex_stop_words) + ").*'")

    cursor.execute(sql)
    conn.commit()

    copy_sql = "(SELECT term_label, new_term_id FROM tmp_term_%s)" % term_part

    # now dump table to csv
    with output_file.open("w") as fd:
        fd.write(",".join(term_columns) + "\n")
        cursor.copy_to(fd, copy_sql, sep=",")

    t1 = datetime.now()
    with open(log_file, "a") as fd:
        fd.write("term,%s,%s,%s\n" % (term_part, str(t1), str(t1 - t0)))

    cursor.close()
    conn.close()


def drop_temp_term_table(schema, db_kwds, term_part):
    """drops all the temp term tables

    Parameters
    ----------
    schema : str
        schema name
    db_kwds : dict-like
        key-words to pass to pscopg2 connection
    term_part : str
        label of temporary term partition table to drop

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


def count_query(schema, db_kwds, doc_output_file, count_output_file_dict,
                doc_part, term_partitions, count_partitions,
                doc_columns, count_columns, gen_full_doc_sql, gen_count_sql,
                gen_full_doc_sql_kwds, log_file):
    """selects the counts which meet the search criteria for terms and docs
    and writes them to a set of output files

    Parameters
    ----------
    schema : str
        name of schema
    db_kwds : dict-like
        key-words to pass to pscopyg2 connection
    doc_output_file : luigi file object
        where we want to write the doc_id for the corresponding doc_part
    count_output_file_dict : dictionary of luigi file objects
        dictionary where each element is a dictionary over term_part
        and then count_part so count_output_file_dict[term_part][count_part]
    doc_part : str
        label for current doc partition
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
    log_file : None or str
        if None location where log is written when done

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
    with doc_output_file.open("w") as fd:
        fd.write(",".join(doc_columns) + "\n")
        cursor.copy_to(fd, "(%s)" % doc_id_sql, sep=",")

    t1 = datetime.now()
    with open(log_file, "a") as fd:
        fd.write("doc,%s,%s,%s\n" % (doc_part, str(t1), str(t1 - t0)))

    # for each ngram and term label extract counts
    for term_part in term_partitions:
        for count_part in count_partitions:
            t0 = datetime.now()
            count_sql = gen_count_sql(doc_sql, doc_part, term_part,
                                      count_part)
            count_f = count_output_file_dict[term_part][count_part]
            with count_f.open("w") as fd:
                fd.write(",".join(count_columns) + "\n")
                cursor.copy_to(fd, "(%s)" % count_sql, sep=",")

            t1 = datetime.now()
            with open(log_file, "a") as fd:
                fd.write("count,%s,%s,%s,%s,%s\n" % (count_part,
                                                     term_part,
                                                     doc_part,
                                                     str(t1), str(t1 - t0)))
