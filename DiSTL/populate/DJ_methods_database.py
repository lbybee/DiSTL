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
                    term_id             INTEGER PRIMARY KEY,
                    term_label          VARCHAR(255) NOT NULL UNIQUE,
                    doc_count           INTEGER DEFAULT 0,
                    term_count          INTEGER DEFAULT 0,
                    headline_doc_count  INTEGER DEFAULT 0,
                    headline_term_count INTEGER DEFAULT 0,
                    body_doc_count      INTEGER DEFAULT 0,
                    body_term_count     INTEGER DEFAULT 0
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
                doc_id              INTEGER PRIMARY KEY,
                publisher           VARCHAR(255),
                product             VARCHAR(255),
                seq                 INT,
                docdate             date,
                news_source         VARCHAR(255),
                origin              VARCHAR(255),
                service_id          VARCHAR(255),
                brand               VARCHAR(255),
                temp_perm           VARCHAR(255),
                retention           VARCHAR(255),
                hot                 VARCHAR(255),
                original_source     VARCHAR(255),
                accession_number    BIGINT NOT NULL,
                page_citation       VARCHAR(255),
                display_date        timestamp NOT NULL,
                headline            TEXT,
                body_txt            TEXT NOT NULL,
                headline_term_count INTEGER DEFAULT 0,
                body_term_count     INTEGER DEFAULT 0
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
