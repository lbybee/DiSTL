"""
methods for building DTM from text database
"""
from labbot.components import Coordinator
from .utilities import pg_conn
from jinja2 import Template
import os


##############################################################################
#                           Control/main functions                           #
##############################################################################

def query(out_dir, doc_sql_jtstr, term_sql_jtstr, count_sql_jtstr,
          doc_columns_map, term_columns_map, count_columns_map,
          doc_partitions=[None], term_partitions=[None],
          count_partitions=[None], doc_query_kwds={}, term_query_kwds={},
          count_query_kwds={}, db_kwds={}, **coordinator_kwds):
    """runs a query on the text database to build a DTM

    Parameters
    ----------
    out_dir : str
        location where output files will be stored
    doc_sql_jtstr : jinja template str
        template which takes a doc part as input to generate a doc query
    term_sql_jtstr : jinja template str
        template which takes a term part as input to generate a term query
    count_sql_jtstr : jinja template str
        template which takes a doc query, term part and count part as input
        to generate a count query
    doc_columns_map : dict
        mapping from db doc metadata column names to csv doc column names
    term_columns_map : dict
        mapping from db term metadata column names to csv term column names
    count_columns_map : dict
        mapping from db count column names to csv count column names
    doc_partitions : list
        list of partition labels for doc axis (e.g. dates)
    term_partitions : list
        list of partition labels for term axis (e.g. 1gram, 2gram)
    count_partitions : list
        list of partition labels for count axis (e.g. headline, body)
    doc_query_kwds : dict
        additional key-words for rendering doc query from template
    term_query_kwds : dict
        additional key-words for rendering term query from template
    count_query_kwds : dict
        additional key-words for rendering count query from template
    db_kwds : dict-like
        key-words to handle psycopg2 database connection
    coordinator_kwds : dict-like
        key-words to pass to init labbot Coordinator backend

    Notes
    -----
    - For naming conventions, in cases where there is only one partition,
      we represent this with None.  When the file/table names are generated
      we use this jinja template (same for doc/count as well):

        "{% if term_part %}_{{ term_part }}{% endif %}"

      So the resulting name will either be "term" or "term_{{ term_part }}"

    - When interacting with the DiSTL DTM class, note that the separate
      count_partitions here, really correspond to distinct DTMs with the
      same set of doc/term partitions
    """

    # decorate sub-functions
    dec_dttt = pg_comp(**db_kwds)(drop_temp_term_table)
    dec_dq = pg_conn(**db_kwds)(term_query)
    dec_dcq = pg_conn(**db_kwds)(doc_count_query)

    # init Coordinator backend to run jobs
    coord = Coordinator(gather=True, **coordinator_kwds)

    # drop any existing tmp tables
    coord.map(dec_dttt, term_partitions, cache=False)

    # create tmp term tables and write to the output dir
    coord.map(dec_tq, term_partitions, out_dir=out_dir,
              term_sql_jtstr=term_sql_jtstr,
              term_query_kwds=term_query_kwds,
              term_columns_map=term_columns_map)

    # write doc_id and count files for each doc_part
    coord.map(dec_dcq, doc_partitions, out_dir=out_dir,
              term_partitions=term_partitions,
              count_partitions=count_partitions,
              doc_sql_jtstr=doc_sql_jtstr,
              count_sql_jtstr=count_sql_jtstr,
              doc_query_kwds=doc_query_kwds,
              count_query_kwds=count_query_kwds,
              doc_columns_map=doc_columns_map,
              count_columns_map=count_columns_map)

    # drop tmp tables
    coord.map(dec_dttt, term_partitions, cache=False)



##############################################################################
#                           State/IO/gen functions                           #
##############################################################################

def term_query(term_part, term_sql_jtstr, term_query_kwds, term_columns_map,
               out_dir, **kwds):
    """create temporary term table from query and store corresponding csv


    Parameters
    ----------
    term_part : str
        label for current term partition
    term_sql_jtstr : jinja template str
        template which takes a term part as input to generate a term query
    term_query_kwds : dict or None
        additional key-words for rendering term query from template
    term_columns_map : dict
        mapping from db term metadata column names to csv term column names
    out_dir : str
        location where output files will be stored
    conn : psycopg2 connection or None
        connection instance for db
    cursor : psycopg2 connection or None
        cursor instance for db

    Notes
    -----
    - We can't use a temporary table here because we need this table
      to exist across sessions so that the count building can happen
      in parallel

    - Additionally, we need the tmp table in the first place st each
      count query doesn't need to rerun the term query (this improves
      performance for these large parallel queries)
    """

    conn = kwds.pop("conn", None)
    cursor = kwds.pop("cursor", None)

    # create temporary table containing output from term_sql query
    template = Template(term_sql_jtstr)
    query_sql = template.render(term_part=term_part, **term_query_kwds)
    tmp_sql = ("CREATE TABLE "
               "tmp_term"
               "{% if term_part %}_{{ term_part }}{% endif %} "
               "AS ({{ query_sql }})")
    template = Template(tmp_sql)
    tmp_sql = template.render(term_part=term_part, query_sql=query_sql)
    cursor.execute(tmp_sql)
    conn.commit()

    # dump temporary term table to csv
    select = ["%s AS %s" % (k, term_columns_map[k]) for k in term_columns_map]
    select = ",".join(select)
    header = ",".join(list(term_columns_map.values()))
    copy_sql = ("(SELECT {{ select }} FROM "
                "tmp_term"
                "{% if term_part %}_{{ term_part }}{% endif %})")
    template = Template(copy_sql)
    copy_sql = template.render(select=select, term_part=term_part)
    fname = "term{% if term_part %}_{{ term_part }}{% endif %}.csv"
    fname = Template(fname).render(term_part=term_part)
    with open(os.path.join(out_dir, fname), "w") as fd:
        fd.write(header + "\n")
        cursor.copy_to(fd, copy_sql, sep=",")


def drop_temp_term_table(term_part, **kwds):
    """drops the corresponding temp term tables

    Parameters
    ----------
    term_part : str
        label for current term partition
    conn : psycopg2 connection or None
        connection instance for db
    cursor : psycopg2 connection or None
        cursor instance for db
    """

    conn = kwds.pop("conn", None)
    cursor = kwds.pop("cursor", None)

    # drop temporary table
    drop_sql = ("DROP TABLE IF EXISTS "
                "tmp_term"
                "{% if term_part %}_{{ term_part }}{% endif %}")
    template = Template(drop_sql)
    drop_sql = template.render(term_part=term_part)
    cursor.execute(drop_sql)
    conn.commit()


def doc_count_query(doc_part, term_partitions, count_partitions,
                    doc_sql_jtstr, count_sql_jtstr, doc_query_kwds,
                    count_query_kwds, doc_columns_map, count_columns_map,
                    out_dir, **kwds):
    """query the docs and counts for a given doc part

    Parameters
    ----------
    doc_part : str
        label for current doc partition
    term_partitions : list or None
        list of partition labels for term axis (e.g. 1gram, 2gram)
    count_partitions : list or None
        list of partition labels for count axis (e.g. headline, body)
    doc_sql_jtstr : jinja template str
        template which takes a doc part as input to generate a doc query
    count_sql_jtstr : jinja template str
        template which takes a doc query, term_part and count part as input
        to generate a count query
    doc_query_kwds : dict or None
        additional key-words for rendering doc query from template
    count_query_kwds : dict or None
        additional key-words for rendering count query from template
    doc_columns_map : dict
        mapping from db doc metadata column names to csv doc column names
    count_columns_map : dict
        mapping from db count column names to csv count column names
    out_dir : str
        location where output files will be stored
    conn : psycopg2 connection or None
        connection instance for db
    cursor : psycopg2 connection or None
        cursor instance for db

    Notes
    -----
    - The count query works by using the current doc_part doc query as well
      as a query against the temporary term table
    """

    conn = kwds.pop("conn", None)
    cursor = kwds.pop("cursor", None)

    # format doc query
    template = Template(doc_sql_jtstr)
    doc_query_sql = template.render(doc_part=doc_part, **doc_query_kwds)

    # dump doc metadata
    select = ["%s AS %s" % (k, doc_columns_map[k]) for k in doc_columns_map]
    select = ",".join(select)
    header = ",".join(list(doc_columns_map.values()))
    copy_sql = "SELECT {{ select }} FROM ({{ doc_query_sql }}) AS tmp_tab"
    template = Template(copy_sql)
    copy_sql = template.render(select=select, doc_query_sql=doc_query_sql)
    fname = ("doc"
             "{% if doc_part %}_{{ doc_part }}{% endif %}"
             ".csv")
    fname = Template(fname).render(doc_part=doc_part)
    with open(os.path.join(out_dir, fname), "w") as fd:
        fd.write(header + "\n")
        cursor.copy_to(fd, "(%s)" % copy_sql, sep=",")

    # dump counts
    select = ["%s AS %s" % (k, count_columns_map[k])
              for k in count_columns_map]
    select = ",".join(select)
    header = ",".join(list(count_columns_map.values()))
    for term_part in term_partitions:
        for count_part in count_partitions:
            template = Template(count_sql_jtstr)
            count_query_sql = template.render(doc_query_sql=doc_query_sql,
                                              doc_part=doc_part,
                                              term_part=term_part,
                                              count_part=count_part,
                                              **count_query_kwds)
            copy_sql = ("SELECT {{ select }} FROM ({{ count_query_sql }}) "
                        "AS tmp_tab")
            template = Template(copy_sql)
            copy_sql = template.render(select=select,
                                      count_query_sql=count_query_sql)
            fname = ("count"
                     "{% if doc_part %}_{{ doc_part }}{% endif %}"
                     "{% if term_part %}_{{ term_part }}{% endif %}"
                     "{% if count_part %}_{{ count_part }}{% endif %}"
                     ".csv")
            fname = Template(fname).render(doc_part=doc_part,
                                           term_part=term_part,
                                           count_part=count_part)
            with open(os.path.join(out_dir, fname), "w") as fd:
                fd.write(header + "\n")
                cursor.copy_to(fd, "(%s)" % copy_sql, sep=",")
