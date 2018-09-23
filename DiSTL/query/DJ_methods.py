def gen_tag_sql(date, tag_drop_dict):
    """generates query to select documents based on tag drop lists

    Parameters
    ----------
    date : str
        date for corresponding tables
    tag_drop_dict : dict-like
        dictionary containing tag categories to drop (keys) as well as list
        of actual tags to drop (values)

    Returns
    -------
    tag sql query
    """

    # select doc_ids from tag link tables
    tag_link_sql = "SELECT doc_id FROM"
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
    tag_link_sql += " (%s) AS foo" % " UNION ".join(sql_l)

    # join with doc table
    tag_sql = """SELECT *
                    FROM doc_%s l
                    WHERE NOT EXISTS
                    (SELECT doc_id
                     FROM (%s) r
                     WHERE r.doc_id = l.doc_id
                    )
              """ % (date, tag_link_sql)

    return tag_sql


def gen_doc_sql(tag_sql, date, headline_l=[], author_l=[],
                article_threshold=0):
    """
    generates query to select documents based on doc specifics

    This does the following:

    1. remove weekends
    2. drop documents matching headline start patterns
    3. drop documents containing author patterns
    4. drop documents below term_count threshold

    Parameters
    ----------
    tag_sql : str
        query returned by gen_tag_sql
    date : str
        date label for current table
    headline_l : list
        list of headline patterns to drop
    author_l : list
        list of author patterns to drop
    article_threshold : int
        headline + body term count required to keep article

    Returns
    -------
    doc sql query
    """

    doc_sql = """SELECT *
                    FROM (%s) AS foo
                    WHERE EXTRACT(DOW FROM display_date) NOT IN (6, 0)
                    AND headline !~* %s
                    AND headline !~* %s
                    AND (headline_term_count + body_term_count) > %d
              """ % (tag_sql,
                     "'^(" + "|".join(headline_l) + ").*'",
                     "'.*(" + "|".join(author_l) + ").*'",
                     article_threshold)

    return doc_sql


def gen_const_company_sql(doc_sql, date, company_id_l):
    """generates a sql query returing doc metadata as well as tag
    label constrained to company_id_l

    Parameters
    ----------
    doc_sql : str
        query returned by gen_doc_sql
    date : str
        date label
    company_id_l : list of str
        list of tickers to extract

    Returns
    -------
    updated query
    """

    sql = """SELECT tag_label,
                    tmp_doc.*
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


def gen_full_company_sql(doc_sql, date):
    """generates a sql query returning doc metadata as well as tag
    label for all docs

    Parameters
    ----------
    doc_sql : str
        query returned by gen_doc_sql
    date : str
        date label

    Returns
    -------
    updated query
    """

    sql = """SELECT tag_label,
                    tmp_doc.*
             FROM (SELECT tag_label, tag_link_djn_company_%s.tag_id, doc_id
                   FROM
                   (SELECT tag_label, tag_id
                    FROM tag_djn_company
                   ) AS tmp_tag
                   INNER JOIN tag_link_djn_company_%s
                   ON tmp_tag.tag_id = tag_link_djn_company_%s.tag_id
                  ) AS tmp_tag_doc
             INNER JOIN (%s) AS tmp_doc
             ON tmp_tag_doc.doc_id = tmp_doc.doc_id
          """ % (date, date, date, doc_sql)

    return sql



def gen_article_doc_sql(doc_sql, threshold=0):
    """generates the query for extracting articles from the database

    Parameters
    ----------
    doc_sql : str
        current sql query
    threshold : int
        term count threhsold for dropping further articles

    Returns
    -------
    doc query
    """

    sql = """SELECT doc_id,
                    display_date,
                    accession_number,
                    dense_rank() OVER(ORDER BY doc_id) AS new_doc_id
                FROM (%s) AS tmp_doc
                WHERE (headline_term_count + body_term_count) > %d
            """ % (doc_sql, threshold)
    return sql


def gen_article_doc_id_sql(doc_sql):
    """generate the query needed to write the article doc id info to a csv

    Parameters
    ----------
    doc_sql : str
        current sql query

    Returns
    -------
    doc id query
    """

    sql = """SELECT display_date, accession_number, new_doc_id
                FROM (%s) as tmp_doc
          """ % doc_sql
    return sql


def gen_day_doc_sql(doc_sql, threshold=0):
    """generates the query for extracting days from the database
    (applying any daily threhsolding in the process)

    Parameters
    ----------
    doc_sql : str
        current sql query
    threshold : int
        term count threhsold for dropping days

    Returns
    -------
    doc query
    """

    sql = """SELECT *,
                    date(display_date) AS day_date,
                    dense_rank()
                    OVER(ORDER BY date(display_date))
                    AS new_doc_id
             FROM (%s) AS tmp_doc
             WHERE (headline_term_count + body_term_count) > %d
          """ % (doc_sql, threshold)
    return sql


def gen_day_doc_id_sql(doc_sql):
    """generates the query needed to write da doc id info to a csv

    Parameters
    ----------
    doc_sql : str
        current sql query

    Returns
    -------
    doc id query
    """

    sql = """SELECT DISTINCT ON (new_doc_id)
                day_date, new_doc_id
                FROM (%s) as tmp_doc
          """ % doc_sql
    return sql


def gen_ticker_day_doc_sql(doc_sql, threshold=0):
    """generates the query for extracting ticker-days from the database
    (applying any daily threhsolding in the process)

    Parameters
    ----------
    doc_sql : str
        current sql query
    threshold : int
        term count threhsold for dropping ticker-days

    Returns
    -------
    doc query
    """

    # generate the new id for thresholding
    # TODO this could likely be simplified
    sql = """SELECT *,
                    date(display_date) AS day_date,
                    (tmp_doc.headline_term_count
                     + tmp_doc.body_term_count) AS term_count,
                    dense_rank()
                    OVER(ORDER BY tag_label,
                        date(display_date))
                    AS new_doc_id
             FROM (%s) AS tmp_doc
          """ % (doc_sql)

    # select based on daily threshold
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
          """ % (sql, sql, threshold)

    return sql


def gen_ticker_day_doc_id_sql(doc_sql):
    """generate the query needed to write the ticker-day doc id info to a csv

    Parameters
    ----------
    doc_sql : str
        current sql query

    Returns
    -------
    doc id query
    """

    sql = """SELECT min(tag_label),
                    min(day_date),
                    new_doc_id,
                    COUNT(tag_label) AS article_count
                FROM (%s) AS tmp_doc
                GROUP BY new_doc_id""" % doc_sql
    return sql


def gen_ticker_month_doc_sql(doc_sql, threshold=0):
    """generates the query for extracting ticker-months from the database
    (applying any monthly threhsolding in the process)

    Parameters
    ----------
    doc_sql : str
        current sql query
    threshold : int
        term count threhsold for dropping ticker-months

    Returns
    -------
    doc query
    """

    # generate the new id for thresholding
    # TODO this could likely be simplified
    sql = """SELECT tag_label,
                    extract(year FROM display_date) AS year,
                    extract(month FROM display_date) AS month,
                    tmp_doc.doc_id,
                    (tmp_doc.headline_term_count
                     + tmp_doc.body_term_count) AS term_count,
                    dense_rank()
                    OVER(ORDER BY tag_label,
                        extract(year FROM display_date),
                        extract(month FROM display_date))
                    AS new_doc_id
             FROM (%s) AS tmp_doc
          """ % (doc_sql)

    # select based on monthly threshold
    sql = """SELECT tag_label,
                    year,
                    month,
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
          """ % (sql, sql, threshold)

    return sql


def gen_ticker_month_doc_id_sql(doc_sql):
    """generate the query needed to write the ticker-month doc id info to a
    csv

    Parameters
    ----------
    doc_sql : str
        current sql query

    Returns
    -------
    doc id query
    """

    sql = """SELECT min(tag_label),
                    min(year),
                    min(month),
                    new_doc_id,
                    COUNT(tag_label) AS article_count
                FROM (%s) AS tmp_doc
                GROUP BY new_doc_id""" % doc_sql
    return sql


def gen_count_sql(doc_sql, date, ngram, t_label):
    """joins the temporary term and doc tables with the counts and
    dumps the results to the provided file

    Parameters
    ----------
    sql : str
        current sql query
    date : str
        date label
    ngram : str
        ngram label
    t_label : str
        label for current txt category

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


def gen_full_doc_sql(date, tag_drop_dict=None, headline_l=[],
                     author_l=[], article_threshold=0,
                     company_id_l=[], agg_type="article",
                     agg_threshold=0):
    """generates the doc query and doc id query for the provided params

    Parameters
    ----------
    date : str
        date for corresponding tables
    tag_drop_dict : dict-like or None
        dictionary containing tag categories to drop (keys) as well as list
        of actual tags to drop (values)
    headline_l : list
        list of headline patterns to drop
    author_l : list
        list of author patterns to drop
    article_threshold : int
        headline + body term count required to keep article
    company_id_l : list
        list of tickers to extract
    agg_type : str
        lvl of aggregation for final document
    agg_threshold : int
        any additional thresholding done at final aggregate lvl

    Returns
    -------
    tuple containing doc threshold and doc id threhsold
    """

    if tag_drop_dict:
        sql = gen_tag_sql(date, tag_drop_dict)
    else:
        sql = "SELECT * FROM doc_%s" % date

    if len(headline_l) > 0 or len(author_l) > 0 or article_threshold > 0:
        sql = gen_doc_sql(sql, date, headline_l, author_l, article_threshold)

    if len(company_id_l) > 0:
        sql = gen_const_company_sql(sql, date, company_id_l)
    elif "ticker" in agg_type:
        sql = gen_full_company_sql(sql, date)

    if agg_type == "article":
        doc_sql = gen_article_doc_sql(sql, agg_threshold)
        doc_id_sql = gen_article_doc_id_sql(doc_sql)

    elif agg_type == "day":
        doc_sql = gen_day_doc_sql(sql, agg_threshold)
        doc_id_sql = gen_day_doc_id_sql(doc_sql)

    elif agg_type == "ticker_day":
        doc_sql = gen_ticker_day_doc_sql(sql, agg_threshold)
        doc_id_sql = gen_ticker_day_doc_id_sql(doc_sql)

    elif agg_type == "ticker_month":
        doc_sql = gen_ticker_month_doc_sql(sql, agg_threshold)
        doc_id_sql = gen_ticker_month_doc_id_sql(doc_sql)

    else:
        raise ValueError("unsupported agg_type: %s" % agg_type)

    return doc_sql, doc_id_sql
