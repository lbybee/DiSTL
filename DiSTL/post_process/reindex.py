"""
This code combines the various count and id files that are generated by
the database query and cleans up the indexes and aggregates where necessary

Currently the following types of documents are supported

    1. articles
    2. days
    3. months
    4. ticker-days
    5. ticker-months
"""
import dask.dataframe as dd
import pandas as pd
import multiprocessing
import sys
import os


def load_term_id(source_data_dir, out_data_dir, ngrams):
    """loads the term ids, writes the updated term_ids and returns the offset
    dict

    Parameters
    ----------
    source_data_dir : str
        location of source data files
    out_data_dir : str
        location where results are stored
    ngrams : int
        number of ngrams

    Returns
    -------
    term_id_offset dictionary

    Writes
    ------
    new term ids
    """

    term_id_offset = 0
    term_id_offset_dict = {}
    for n in ngrams:
        term_id = pd.read_csv(os.path.join(source_data_dir,
                                           "term_id_%s.csv" % n),
                              names=["term", "term_id"])
        term_id["term_id"] += term_id_offset
        term_id_offset_dict[n] = term_id_offset
        term_id.to_csv(os.path.join(out_data_dir,
                                    "term_id_%s.csv" % n), index=False)
        term_id_offset += len(term_id)

    return term_id_offset_dict


def load_permno(permno_f):
    """loads the permno file and preps to merge with doc_id

    Parameters
    ----------
    permno_f : str
        permno file name

    Returns
    -------
    pandas dataframe
    """

    permno_df = pd.read_csv(permno_f, sep="\t")
    permno_df.columns = ["permno", "date", "ticker", "permco", "ret"]
    permno_df["date"] = pd.to_datetime(permno_df["date"].astype(str))
    permno_df["year"] = permno_df["date"].dt.year
    permno_df["month"] = permno_df["date"].dt.month
    permno_df = permno_df[~permno_df.duplicated(["ticker", "date"])]
    permno_df = permno_df.drop(["date"], axis=1)

    return permno_df


def load_article_doc_id(source_data_dir, date):

    raise NotImplementedError()


def load_month_doc_id(source_data_dir, date):
    """loads the daily doc ids

    Parameters
    ----------
    source_data_dir : str
        location of input files
    date : str
        current date for doc_id

    Returns
    -------
    doc_id
    """

    doc_id = pd.read_csv(os.path.join(source_data_dir,
                                      "doc_id_%s.csv" % date),
                         names=["year", "month", "doc_id"])
    return doc_id


def load_day_doc_id(source_data_dir, date):
    """loads the daily doc ids

    Parameters
    ----------
    source_data_dir : str
        location of input files
    date : str
        current date for doc_id

    Returns
    -------
    doc_id
    """

    doc_id = pd.read_csv(os.path.join(source_data_dir,
                                      "doc_id_%s.csv" % date),
                         names=["date", "doc_id"])
    return doc_id


def load_ticker_day_doc_id(source_data_dir, date, permno_df):
    """loads the doc ids which are ticker-daily (so just have date
    variable)

    Parameters
    ----------
    source_data_dir : str
        location of input files
    date : str
        current date for doc_id
    permno_df : pandas dataframe
        dataframe containing permnos

    Returns
    -------
    doc_id
    """

    doc_id = pd.read_csv(os.path.join(source_data_dir,
                                      "doc_id_%s.csv" % date),
                         names=["ticker", "date", "doc_id"])
    doc_id["date"] = pd.to_datetime(doc_id["date"])
    doc_id["year"] = doc_id["date"].dt.year
    doc_id["month"] = doc_id["date"].dt.month
    doc_id = doc_id.merge(permno_df, on=("ticker", "year", "month"))
    doc_id = doc_id[["permno", "date", "doc_id"]]

    return doc_id


def load_ticker_month_doc_id(source_data_dir, date, permno_df):
    """loads the doc ids which are ticker-monthly (so have a year and
    month variable)

    Parameters
    ----------
    source_data_dir : str
        location of input files
    date : str
        current date for doc_id
    permno_df : pandas dataframe
        dataframe containing permnos

    Returns
    -------
    doc_id
    """

    doc_id = pd.read_csv(os.path.join(source_data_dir,
                                      "doc_id_%s.csv" % date),
                         names=["ticker", "year", "month", "doc_id"])
    doc_id = doc_id.merge(permno_df, on=("ticker", "year", "month"))
    doc_id = doc_id[["permno", "year", "month", "doc_id"]]

    return doc_id


def load_doc_id(source_data_dir, date, doc_id_type="ticker_day",
                permno_df=None):
    """loads the doc_id according to the doc_id_type

    Parameters
    ----------
    source_data_dir : str
        location of input files
    date : str
        current date for doc_id
    doc_id_type : str
        label for doc id
    permno_df : pandas dataframe
        dataframe containing permnos

    Returns
    -------
    doc_id
    """

    if doc_id_type == "article":
        doc_id = load_article_doc_id(source_data_dir, date)
    elif doc_id_type == "day":
        doc_id = load_day_doc_id(source_data_dir, date)
    elif doc_id_type == "month":
        doc_id = load_month_doc_id(source_data_dir, date)
    elif doc_id_type == "ticker_day":
        doc_id = load_day_doc_id(source_data_dir, date, permno_df)
    elif doc_id_type == "ticker_month":
        doc_id = load_month_doc_id(source_data_dir, date, permno_df)
    else:
        raise ValueError("Unsupported doc_id_type: %s" % doc_id_type)

    return doc_id


def counter(source_data_dir, tmp_dir, date, doc_id_type="ticker_day",
            permno_df=None):
    """counts the number of documents for each doc_id file, this is done so
    that we can reindex the counts in parallel

    Parameters
    ----------
    source_data_dir : str
        location of input files
    tmp_dir : str
        location where count files will be stored
    date : str
        current date for doc_id
    doc_id_type : str
        type of doc id
    permno_df : None or pandas dataframe
        dataframe containing permnos

    Returns
    -------
    None

    Writes
    ------
    counts
    """

    doc_id = load_doc_id(source_data_dir, date, doc_id_type, permno_df)

    count = len(doc_id)
    with open(os.path.join(tmp_dir, "counts.csv"), "a") as fd:
        fd.write("%s,%d\n" % (date, count))


def reindexer(source_data_dir, tmp_dir, out_data_dir, date, ngrams,
              term_id_offset_dict, doc_id_type="ticker_day", permno_df=None):
    """reindexes the counts, this updates the term_id and doc_id and writes
    the updated doc_ids and counts

    Parameters
    ----------
    source_data_dir : str
        location of input files
    tmp_dir : str
        location where count files will be stored
    out_data_dir : str
        location where results are stored
    date : str
        current date for doc_id
    ngrams : int
        number of ngrams
    term_id_offset_dict : dict-like
        dictionary mapping ngrams to offset
    doc_id_type : str
        type of doc id
    permno_df : pandas dataframe
        dataframe containing permnos

    Returns
    -------
    None

    Writes
    ------
    - new doc ids
    - new counts
    """

    # get doc_id offset
    agg_counts = pd.read_csv(os.path.join(tmp_dir, "counts.csv"),
                             names=["date", "count"])
    agg_counts = agg_counts.sort_values("date")
    agg_counts = agg_counts[agg_counts["date"] < date]
    doc_id_offset = agg_counts["count"].sum()

    # load doc id
    doc_id = load_doc_id(source_data_dir, date, doc_id_type, permno_df)

    # generate new doc_id
    doc_id_map = doc_id[["doc_id"]]
    doc_id_map["new_doc_id"] = doc_id_map.index + doc_id_offset
    doc_id_map.index = doc_id_map["doc_id"]
    doc_id_map = doc_id_map["new_doc_id"]
    doc_id["doc_id"] = doc_id["doc_id"].map(doc_id_map)
    doc_id.to_csv(os.path.join(out_data_dir, "doc_id_%s.csv" % date),
                  index=False)

    # apply new doc_id to counts
    for ngram in ngrams:
        term_id_offset = term_id_offset_dict[ngram]
        body_f = os.path.join(source_data_dir,
                              "count_body_%s_%s.csv" % (ngram, date))
        headline_f = os.path.join(source_data_dir,
                                  "count_headline_%s_%s.csv" % (ngram, date))
        body_count = pd.read_csv(body_f, names=["doc_id", "term_id", "count"])
        body_count = body_count.set_index(["doc_id", "term_id"])
        body_count = body_count["count"]
        headline_count = pd.read_csv(headline_f,
                                     names=["doc_id", "term_id", "count"])
        headline_count = headline_count.set_index(["doc_id", "term_id"])
        headline_count = headline_count["count"]

        count = body_count.add(headline_count, fill_value=0)
        count = count.reset_index()

        count["term_id"] += term_id_offset
        count["doc_id"] = count["doc_id"].map(doc_id_map)
        count = count[~pd.isnull(count["doc_id"])]
        count["doc_id"] = count["doc_id"].astype(int)
        count["term_id"] = count["term_id"].astype(int)
        count["count"] = count["count"].astype(int)
        count.to_csv(os.path.join(out_data_dir, "count_%s_%s.csv" % (ngram,
                                                                     date)),
                     index=False)


def reindex_wrapper(source_data_dir, tmp_dir, out_data_dir, processes,
                    permno_f=None, doc_id_type="ticker_day"):
    """runs the reindexing code

    Parameters
    ----------
    source_data_dir : str
        location of input files
    tmp_dir : str
        location where count files will be stored
    out_data_dir : str
        location where results are stored
    processes : int
        number of processes for multiprocessing
    permno_f : None or str
        location of permno file if exists
    term_id_offset_dict : dict-like
        dictionary mapping ngrams to offset
    doc_id_type : str
        type of doc id

    Returns
    -------
    None

    Writes
    ------
    - new term ids
    - new doc ids
    - new counts
    """

    # initialize multiprocessing pool
    pool = multiprocessing.Pool(processes)

    # get dates and ngrams
    dates = []
    ngrams = []
    for f in os.listdir(source_data_dir):
        if "doc_id" in f:
            dates.append("_".join(f.split(".")[0].split("_")[2:]))
        if "term_id" in f:
            ngrams.append(f.split(".")[0].split("_")[2])
    dates.sort()
    ngrams.sort()

    # load permno ticker map
    if permno_f:
        permno_df = load_permno(permno_f)
    else:
        permno_df = None

    # handle term ids/indices
    term_id_offset_dict = load_term_id(source_data_dir, out_data_dir, ngrams)

    # generate agg counts
    count_f = os.path.join(tmp_dir, "counts.csv")
    if os.path.exists(count_f):
        os.remove(count_f)
    pool.starmap(counter, [(source_data_dir, tmp_dir, d, doc_id_type,
                            permno_df)
                           for d in dates])

    # generate reindexed docs and counts
    pool.starmap(reindexer, [(source_data_dir, tmp_dir, out_data_dir, d,
                              ngrams, term_id_offset_dict, doc_id_type,
                              permno_df)
                             for d in dates])
