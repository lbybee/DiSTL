from datetime import datetime
import .NYT_methods_database as NYTd
import .DJ_methods_database as DJd
import multiprocessing
import psycopg2
import os



def create_schema(db, schema):
    """creates the schema in the corresponding database

    Parameters
    ----------
    db : str
        name of database
    schema : str
        name of schema

    Returns
    -------
    None
    """

    conn = psycopg2.connect(host="localhost", database=db,
                            user="postgres", password="postgres")
    cursor = conn.cursor()
    cursor.execute("CREATE SCHEMA %s;" % schema)
    conn.commit()
    cursor.close()
    conn.close()


def populate_table(db, schema, pattern, sql_dir, table_dir, log_file):
    """creates a populates the table matching the pattern

    Parameters
    ----------
    db : str
        name of database
    schema : str
        name of schema
    pattern : str
        pattern for table
    sql_dir : str
        location where create table sql is stored
    table_dir : str
        location where table contents is stored
    log_file : str
        location of log

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

    # create table
    with open(os.path.join(sql_dir, "%s.sql" % pattern), "r") as fd:
        cursor.execute(fd.read())
        conn.commit()

    # populate table
    with open(os.path.join(table_dir, "%s.csv" % pattern), "r") as fd:
        columns=fd.readline()
        cursor.copy_expert(sql="""COPY %s(%s) FROM stdin WITH
                                    CSV
                               """ % (pattern, columns), file=fd)
        conn.commit()

    cursor.close()
    conn.close()

    t1 = datetime.now()
    with open(log_file, "a") as fd:
        fd.write("%s,%s,%s\n" % (pattern, str(t1), str(t1 - t0)))


def populate_partition(db, schema, link_labels, count_labels,
                       partition, table_dir, sql_dir, log_file):
    """populates all the tables for a given parition

    Parameters
    ----------
    db : str
        database name
    schema : str
        schema name
    processes : int
        number of proceses for multiprocesing Pool
    link_lables : list
        list of patterns for link tables
    count_lables : list
        list of patterns for count tables
    partitions : list
        list of partitions (generally dates)
    table_dir : str
        location where table files are stored
    sql_dir : str
        location where sql files are stored
    log_file : str
        location of log file

    Returns
    -------
    None
    """

    # populate doc table
    populate_table(db, schema, "doc_%s" % partition, sql_dir, table_dir,
                   log_file)

    # populate count tables
    for cl in count_labels:
        cl = "%s_%s" % (cl, partition)
        populate_table(db, schema, cl, sql_dir, table_dir, log_file)

    # populate link tables
    for ll in link_labels:
        ll = "%s_%s" % (ll, partition)
        populate_table(db, schema, ll, sql_dir, table_dir, log_file)


def database_wrapper(db, schema, processes, term_lables,
                     tag_labels, count_labels, link_lables, partitions,
                     table_dir, sql_dir, log_file, write_sql_kwds=None):
    """populates the database for the given input

    Parameters
    ----------
    db : str
        database name
    schema : str
        schema name
    processes : int
        number of proceses for multiprocesing Pool
    term_lables : list
        list of ngram table labels
    tag_labels : list
        list of tag table labels
    link_lables : list
        list of patterns for link tables
    count_lables : list
        list of patterns for count tables
    partitions : list
        list of partitions (generally dates)
    table_dir : str
        location where table files are stored
    sql_dir : str
        location where sql files are stored
    log_file : str
        location of log file

    Returns
    -------
    None
    """

    if data_type == "DJ":
        write_sql = DJd.write_sql
    elif data_type == "NYT":
        write_sql = NYTd.write_sql
    else:
        raise ValueError("Unsupported data_type: %s" % data_type)

    if write_sql_kwds is None:
        write_sql_kwds = {}

    write_sql(sql_dir, **write_sql_dir)

    create_schema(db, schema)

    pool = multiprocessing.Pool(processes)

    # populate id tables
    pool.starmap(populate_table, [(db, schema, pattern, sql_dir, table_dir,
                                   log_file) for pattern in
                                  (term_lables + tag_labels)])

    # populate partitions
    pool.starmap(populate_partition, [(db, schema, link_lables, count_labels,
                                       part, table_dir, sql_dir, log_file)
                                      for part in partitions])
