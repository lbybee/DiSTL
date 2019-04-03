from grendel import Coordinator
from jinja2 import Template
import psycopg2
import os


##############################################################################
#                           Control/main functions                           #
##############################################################################

def populate(table_dir, sql_dir, doc_partitions=[None], term_partitions=[None],
             count_parittions=[None], tag_labels=[], tag_link_labels=[],
             db_kwds={}, **coordinator_kwds):
    """pouplates a schema in the text database

    Parameters
    ----------
    table_dir : str
        location where table files are stored
    sql_dir : str
        location where sql files are stored, each file should map to a
        file in table dir.  The sql files contain the sql code needed to
        populate the corresponding table
    doc_partitions : list
        list of partition labels for doc axis (e.g. dates)
    term_partitions : list
        list of partition labels for term axis (e.g. 1gram, 2gram)
    count_partitions : list
        list of partition labels for count axis (e.g. headline, body)
    tag_labels : list
        additional indexed tables which we want to link to docs
    tag_link_labels : list
        list of tags which have a corresponding link table.  This is
        separate from tag_labels because it is possible that mutliple
        index/metadata tables may share the same link table.  Consider
        the case of address info where we may have an index for state
        as well as city, but each address will only have one entry in
        the link table
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
    """

    # init Coordinator backend to run jobs
    coord = Coordinator(**coordinator_kwds)

    # create schema
    create_schema(**db_kwds)

    # populate the term tables
    term_pt = "term{% if term_part %}_{{ term_part }}{% endif %}"
    term_pt = Template(term_pt)
    term_labels = [term_pt.render(term_part=t) for t in term_partitions]
    coord.map(populate_table, term_labels, table_dir=table_dir,
              sql_dir=sql_dir, pure=False, gather=True, **db_kwds)

    # populate the tag/metdata index tables
    coord.map(populate_table, tag_labels, table_dir=table_dir,
              sql_dir=sql_dir, pure=False, gather=True, **db_kwds)

    # populate the partitions
    coord.map(populate_dpart, doc_partitions, table_dir=table_dir,
              sql_dir=sql_dir, term_partitions=term_partitions,
              count_partitions=count_partitions, dec_pop_tab=dec_pop_tab,
              tag_link_labels=tag_link_labels, pure=False, gather=True)


##############################################################################
#                           State/IO/gen functions                           #
##############################################################################

def create_schema(schema, **conn_kwds):
    """creates the schema in the corresponding database

    Parameters
    ----------
    schema : str
        name of schema
    conn_kwds : dict
        additional key words for the connection

    Notes
    -----
    Only creates the schema if it doesn't already exist
    """

    conn = psycopg2.connect(**conn_kwds)
    cursor = conn.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS %s;" % schema)
    conn.commit()
    cursor.close()
    conn.close()


def populate_dpart(dpart, table_dir, sql_dir, term_partitions,
                   count_partitions, tag_link_labels, **db_kwds):
    """populates all the tables for the corresponding doc part

    Parameters
    ----------
    dpart : str or None
        current document partition
    table_dir : str
        location where table files are stored
    sql_dir : str
        location where sql files are stored, each file should map to a
        file in table dir.  The sql files contain the sql code needed to
        populate the corresponding table
    term_partitions : list
        list of partition labels for term axis (e.g. 1gram, 2gram)
    count_partitions : list
        list of partition labels for count axis (e.g. headline, body)
    tag_link_labels : list
        additional indexed tables which we want to link to docs.  We
        assume that these contain an id table <label> and a link table
        <label>_link which link the tag entries to the docs
    db_kwds : dict
        additional key words to establish connection for populate_table
    """

    # populate doc table
    doc_pt = "doc{% if doc_part %}_{{ doc_part }}{% endif %}"
    doc_label = Template(doc_pt).render(doc_part=dpart)
    populate_table(doc_label, table_dir, sql_dir, **db_kwds)

    # populate count tables
    for tpart in term_partitions:
        for cpart in count_partitions:
            count_pt = ("count"
                        "{% if doc_part %}_{{ doc_part }}{% endif %}"
                        "{% if term_part %}_{{ term_part }}{% endif %}"
                        "{% if count_part %}_{{ count_part }}{% endif %}")
            count_label = Template(count_pt).render(doc_part=dpart,
                                                    term_part=tpart,
                                                    count_part=cpart)
            populate_table(count_label, table_dir, sql_dir, **db_kwds)

    # populate metadata tables
    for tlab in tag_link_labels:
        tag_pt = ("{{ tag_label }}"
                  "{% if doc_part %}_{{ doc_part }}{% endif %}_link")
        tag_label = Template(tag_pt).render(tag_label=tlab, doc_part=dpart)
        populate_table(tag_label, table_dir, sql_dir, **db_kwds)


def populate_table(tab_label, table_dir, sql_dir, schema, **conn_kwds):
    """populates the table matching the tab_label

    Parameters
    ----------
    tab_label : str
        pattern for table, should have a file in table dir labeled
        <tab_label>.csv, a file in the sql_dir labeled <tab_label>.sql
        and will end up with a table labeled <tab_label>
    table_dir : str
        location where table files are stored
    sql_dir : str
        location where sql files are stored, each file should map to a
        file in table dir.  The sql files contain the sql code needed to
        populate the corresponding table
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

    # create table
    with open(os.path.join(sql_dir, "%s.sql" % tab_label), "r") as fd:
        cursor.execute(fd.read())
        conn.commit()

    # populate table
    with open(os.path.join(table_dir, "%s.csv" % tab_label), "r") as fd:
        columns=fd.readline()
        cursor.copy_expert(sql="COPY %s(%s) FROM stdin WITH CSV" %
                           (tab_label, columns), file=fd)
        conn.commit()

    # close connection and cursor
    cursor.close()
    conn.close()
