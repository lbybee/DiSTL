import os


def write_sql(sql_dir, partitions, ngram_labels, txt_labels, tags):
    """writes the sql to a series of files for each table

    Parameters
    ----------
    sql_dir : str
        location where sql files are stored
    partitions : list
        list of dates (partitions)
    ngram_labels : list
        list of lables for ngrams
    txt_labels : list
        list of txt categories
    tags : list
        list of tag categories

    Returns
    -------
    None
    """

    # TODO the label naming is foobared

    # term id sql
    for nl in ngram_labels:
        sql = """CREATE TABLE term_%s (
                    term_id                     INTEGER PRIMARY KEY,
                    term_label                  VARCHAR(255) NOT NULL UNIQUE,
                    doc_count                   INTEGER DEFAULT 0,
                    term_count                  INTEGER DEFAULT 0,
                    lead_paragraph_doc_count    INTEGER DEFAULT 0,
                    lead_paragraph_term_count   INTEGER DEFAULT 0,
                    abstract_doc_count          INTEGER DEFAULT 0,
                    abstract_term_count         INTEGER DEFAULT 0,
                    snippet_doc_count           INTEGER DEFAULT 0,
                    snippet_term_count          INTEGER DEFAULT 0
                    )
              """ % nl
        with open(os.path.join(sql_dir, "term_%s.sql" % nl), "w") as fd:
            fd.write(sql)

    # tag id sql
    for tl in tags:
        sql = """CREATE TABLE tag_%s (
                     tag_id        INTEGER PRIMARY KEY,
                     tag_label     VARCHAR(255) NOT NULL UNIQUE,
                     doc_count     INTEGER DEFAULT 0
                     )
              """ % tl
        with open(os.path.join(sql_dir, "tag_%s.sql" % tl), "w") as fd:
            fd.write(sql)

    # partitions
    for dl in partitions:

        # docs
        sql = """
              CREATE TABLE doc_%s (
                doc_id                      INTEGER PRIMARY KEY,
                web_url                     VARCHAR(255),
                snippet                     TEXT,
                lead_paragraph              TEXT,
                abstract                    TEXT,
                print_page                  VARCHAR(255),
                source                      VARCHAR(255),
                pub_date                    timestamp,
                document_type               VARCHAR(255),
                news_desk                   VARCHAR(255),
                section_name                VARCHAR(255),
                word_count                  INTEGER,
                type_of_material            VARCHAR(255),
                nyt_id                      VARCHAR(255),
                headline                    TEXT,
                byline                      TEXT,
                lead_paragraph_term_count   INTEGER DEFAULT 0,
                abstract_term_count         INTEGER DEFAULT 0,
                snippet_term_count          INTEGER DEFAULT 0
              )
              """ % dl
        with open(os.path.join(sql_dir, "doc_%s.sql" % dl), "w") as fd:
            fd.write(sql)

        # counts
        for nl in ngram_labels:
            for tl in txt_labels:
                sql = """
                      CREATE TABLE count_%s_%s_%s (
                        count INTEGER NOT NULL,
                        term_id INTEGER NOT NULL,
                        doc_id INTEGER NOT NULL,
                            PRIMARY KEY (term_id , doc_id),
                            FOREIGN KEY (term_id)
                                REFERENCES term_%s (term_id)
                                ON UPDATE CASCADE ON DELETE CASCADE,
                            FOREIGN KEY (doc_id)
                                REFERENCES doc_%s (doc_id)
                                ON UPDATE CASCADE ON DELETE CASCADE
                          )
                      """ % (tl, nl, dl, nl, dl)
                with open(os.path.join(sql_dir,
                                       "count_%s_%s_%s.sql" % (tl, nl, dl)),
                          "w") as fd:
                    fd.write(sql)

        # tag links
        for tl in tags:
            sql = """
                  CREATE TABLE tag_link_%s_%s (
                    tag_id INTEGER NOT NULL,
                    doc_id INTEGER NOT NULL,
                        PRIMARY KEY (tag_id , doc_id),
                        FOREIGN KEY (tag_id)
                            REFERENCES tag_%s (tag_id)
                            ON UPDATE CASCADE ON DELETE CASCADE,
                        FOREIGN KEY (doc_id)
                            REFERENCES doc_%s (doc_id)
                            ON UPDATE CASCADE ON DELETE CASCADE
                      )
                  """ % (tl, dl, tl, dl)
            with open(os.path.join(sql_dir,
                                   "tag_link_%s_%s.sql" % (tl, dl)),
                      "w") as fd:
                fd.write(sql)
