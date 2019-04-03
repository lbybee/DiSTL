"""
methods for building document-tag-matrix from db
closely related to base tag query
"""
from grendel import Coordinator
from jinja2 import Template
import psycopg2
import os


##############################################################################
#                           Control/main functions                           #
##############################################################################

def query_tag(out_dir, doc_sql_jtstr, tag_sql_jtstr, link_sql_jtstr,
              doc_columns_map, tag_columns_map, link_columns_map,
              doc_partitions=[None], tag_partitions=[None],
              link_partitions=[None], doc_query_kwds={}, tag_query_kwds={},
              link_query_kwds={}, db_kwds={}, cache=False,
              **coordinator_kwds):
    """runs a query on the text database to build a DTM (of tags not terms)

    Parameters
    ----------
    out_dir : str
        location where output files will be stored
    doc_sql_jtstr : jinja template str
        template which takes a doc part as input to generate a doc query
    tag_sql_jtstr : jinja template str
        template which takes a tag part as input to generate a tag query
    link_sql_jtstr : jinja template str
        template which takes a doc query, tag part and link part as input
        to generate a link query
    doc_columns_map : dict
        mapping from db doc metadata column names to csv doc column names
    tag_columns_map : dict
        mapping from db tag metadata column names to csv tag column names
    link_columns_map : dict
        mapping from db link column names to csv link column names
    doc_partitions : list
        list of partition labels for doc axis (e.g. dates)
    tag_partitions : list
        list of partition labels for tag axis (e.g. 1gram, 2gram)
    link_partitions : list
        list of partition labels for link axis (e.g. headline, body)
    doc_query_kwds : dict
        additional key-words for rendering doc query from template
    tag_query_kwds : dict
        additional key-words for rendering tag query from template
    link_query_kwds : dict
        additional key-words for rendering link query from template
    db_kwds : dict-like
        key-words to handle psycopg2 database connection
    cache : bool
        whether to cache the coordinator func calls.  We default this to
        off here because query funcs don't return output values
    coordinator_kwds : dict-like
        key-words to pass to init labbot Coordinator backend

    Notes
    -----
    - We need the coordinator to return the references so that dask
      doesn't attempt to be too clever and apply certain operations before
      others have finished

    - For naming conventions, in cases where there is only one partition,
      we represent this with None.  When the file/table names are generated
      we use this jinja template (same for doc/link as well):

        "{% if tag_part %}_{{ tag_part }}{% endif %}"

      So the resulting name will either be "tag" or "tag_{{ tag_part }}"

    - When interacting with the DiSTL DTM class, note that the separate
      link_partitions here, really correspond to distinct DTMs with the
      same set of doc/tag partitions
    """

    # init Coordinator backend to run jobs
    coord = Coordinator(**coordinator_kwds)

    # drop any existing tmp tables
    coord.map(drop_temp_tag_table, tag_partitions,
              pure=False, gather=True, cache=cache, **db_kwds)

    # create tmp tag tables and write to the output dir
    coord.map(tag_query, tag_partitions, out_dir=out_dir,
              tag_sql_jtstr=tag_sql_jtstr,
              tag_query_kwds=tag_query_kwds,
              tag_columns_map=tag_columns_map,
              pure=False, gather=True, cache=cache, **db_kwds)

    # write doc_id and link files for each doc_part
    coord.map(doc_link_query, doc_partitions,
              tag_partitions=tag_partitions,
              link_partitions=link_partitions,
              doc_sql_jtstr=doc_sql_jtstr,
              link_sql_jtstr=link_sql_jtstr,
              doc_query_kwds=doc_query_kwds,
              link_query_kwds=link_query_kwds,
              doc_columns_map=doc_columns_map,
              link_columns_map=link_columns_map,
              out_dir=out_dir, pure=False, gather=True,
              cache=cache, **db_kwds)

    # drop tmp tables
    coord.map(drop_temp_tag_table, tag_partitions,
              pure=False, gather=True, cache=cache, **db_kwds)



##############################################################################
#                           State/IO/gen functions                           #
##############################################################################

def tag_query(tag_part, tag_sql_jtstr, tag_query_kwds, tag_columns_map,
               out_dir, schema, **conn_kwds):
    """create temporary tag table from query and store corresponding csv


    Parameters
    ----------
    tag_part : str
        label for current tag partition
    tag_sql_jtstr : jinja template str
        template which takes a tag part as input to generate a tag query
    tag_query_kwds : dict or None
        additional key-words for rendering tag query from template
    tag_columns_map : dict
        mapping from db tag metadata column names to csv tag column names
    out_dir : str
        location where output files will be stored
    schema : str
        schema which we'd like to interact with
    conn_kwds : dict
        additional key words to establish connection

    Notes
    -----
    - We can't use a temporary table here because we need this table
      to exist across sessions so that the link building can happen
      in parallel

    - Additionally, we need the tmp table in the first place st each
      link query doesn't need to rerun the tag query (this improves
      performance for these large parallel queries)
    """

    # establish postgres connection
    conn = psycopg2.connect(**conn_kwds)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO %s" % schema)
    conn.commit()

    # create temporary table containing output from tag_sql query
    template = Template(tag_sql_jtstr)
    query_sql = template.render(tag_part=tag_part, **tag_query_kwds)
    tmp_sql = ("CREATE TABLE "
               "tmp_tag"
               "{% if tag_part %}_{{ tag_part }}{% endif %} "
               "AS ({{ query_sql }})")
    template = Template(tmp_sql)
    tmp_sql = template.render(tag_part=tag_part, query_sql=query_sql)
    cursor.execute(tmp_sql)
    conn.commit()

    # dump temporary tag table to csv
    select = ["%s AS %s" % (k, tag_columns_map[k]) for k in tag_columns_map]
    select = ",".join(select)
    header = ",".join(list(tag_columns_map.values()))
    copy_sql = ("(SELECT {{ select }} FROM "
                "tmp_tag"
                "{% if tag_part %}_{{ tag_part }}{% endif %})")
    template = Template(copy_sql)
    copy_sql = template.render(select=select, tag_part=tag_part)
    fname = "tag{% if tag_part %}_{{ tag_part }}{% endif %}.csv"
    fname = Template(fname).render(tag_part=tag_part)
    with open(os.path.join(out_dir, fname), "w") as fd:
        fd.write(header + "\n")
        cursor.copy_to(fd, copy_sql, sep=",")

    # close connection and cursor
    cursor.close()
    conn.close()


def drop_temp_tag_table(tag_part, schema, **conn_kwds):
    """drops the corresponding temp tag tables

    Parameters
    ----------
    tag_part : str
        label for current tag partition
    schema : str
        schema which we'd like to interact with
    conn_kwds : dict
        additional key words to establish connection
    """

    # establish postgres connection
    conn = psycopg2.connect(**conn_kwds)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO %s" % schema)
    conn.commit()

    # drop temporary table
    drop_sql = ("DROP TABLE IF EXISTS "
                "tmp_tag"
                "{% if tag_part %}_{{ tag_part }}{% endif %}")
    template = Template(drop_sql)
    drop_sql = template.render(tag_part=tag_part)
    cursor.execute(drop_sql)
    conn.commit()

    # close connection and cursor
    cursor.close()
    conn.close()


def doc_link_query(doc_part, tag_partitions, link_partitions,
                    doc_sql_jtstr, link_sql_jtstr, doc_query_kwds,
                    link_query_kwds, doc_columns_map, link_columns_map,
                    out_dir, schema, **conn_kwds):
    """query the docs and links for a given doc part

    Parameters
    ----------
    doc_part : str
        label for current doc partition
    tag_partitions : list or None
        list of partition labels for tag axis (e.g. 1gram, 2gram)
    link_partitions : list or None
        list of partition labels for link axis (e.g. headline, body)
    doc_sql_jtstr : jinja template str
        template which takes a doc part as input to generate a doc query
    link_sql_jtstr : jinja template str
        template which takes a doc query, tag_part and link part as input
        to generate a link query
    doc_query_kwds : dict or None
        additional key-words for rendering doc query from template
    link_query_kwds : dict or None
        additional key-words for rendering link query from template
    doc_columns_map : dict
        mapping from db doc metadata column names to csv doc column names
    link_columns_map : dict
        mapping from db link column names to csv link column names
    out_dir : str
        location where output files will be stored
    schema : str
        schema which we'd like to interact with
    conn_kwds : dict
        additional key words to establish connection

    Notes
    -----
    - The link query works by using the current doc_part doc query as well
      as a query against the temporary tag table
    """

    # establish postgres connection
    conn = psycopg2.connect(**conn_kwds)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO %s" % schema)
    conn.commit()

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

    # dump links
    select = ["%s AS %s" % (k, link_columns_map[k])
              for k in link_columns_map]
    select = ",".join(select)
    header = ",".join(list(link_columns_map.values()))
    for tag_part in tag_partitions:
        for link_part in link_partitions:
            template = Template(link_sql_jtstr)
            link_query_sql = template.render(doc_query_sql=doc_query_sql,
                                              doc_part=doc_part,
                                              tag_part=tag_part,
                                              link_part=link_part,
                                              **link_query_kwds)
            copy_sql = ("SELECT {{ select }} FROM ({{ link_query_sql }}) "
                        "AS tmp_tab")
            template = Template(copy_sql)
            copy_sql = template.render(select=select,
                                      link_query_sql=link_query_sql)
            fname = ("tag_link"
                     "{% if tag_part %}_{{ tag_part }}{% endif %}"
                     "{% if doc_part %}_{{ doc_part }}{% endif %}"
                     "{% if link_part %}_{{ link_part }}{% endif %}"
                     ".csv")
            fname = Template(fname).render(doc_part=doc_part,
                                           tag_part=tag_part,
                                           link_part=link_part)
            with open(os.path.join(out_dir, fname), "w") as fd:
                fd.write(header + "\n")
                cursor.copy_to(fd, "(%s)" % copy_sql, sep=",")

    # close connection and cursor
    cursor.close()
    conn.close()
