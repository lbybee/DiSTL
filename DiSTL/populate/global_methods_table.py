from .global_methods_general import tokenizer, vocab_cleaner, \
                                    default_lemmatizer
from datetime import datetime
import .NYT_methods_table as NYTt
import .DJ_methods_table as DJt
import dask.dataframe as dd
import pandas as pd
import numpy as np
import multiprocessing
import os


def token_binner(tokens, ngrams, term_id_map_dict):
    """generates binned counts for the tokens provided

    Parameters
    ----------
    tokens : list
        list of words
    ngrams : int
        number of ngrams
    term_id_map_dict : dict-like
        dictionary containing map to term_id for each n-gram

    Returns
    -------
    count_dict mapping term_id to count (for current doc)
    """

    counts = {}
    for n in range(1, ngrams + 1):
        term_ngram = "%dgram" % n
        counts[term_ngram] = {}
        for j in zip(*[tokens[i:] for i in range(n)]):
            try:
                term_id = term_id_map_dict[term_ngram][" ".join(j)]
                if term_id not in counts[term_ngram]:
                    counts[term_ngram][term_id] = 1
                else:
                    counts[term_ngram][term_id] += 1
            except:
                continue

    return counts


def gen_term_counts(txt_labels, n, count_dir, stop_word_files=None,
                    regex_stop_word_files=None, doc_lthresh=None,
                    stem=False):
    """aggregates counts for various txt_labels and cleans terms

    Parameters
    ----------
    txt_labels : list
        list of txt cateogries (e.g. headline, body)
    count_dir : str
        location where count files are stored
    n : int
        n in n-gram
    stop_word_files : list or None
        list of files containing stop words
    regex_stop_word_files : list or None
        list of files containing regex stop words
    doc_lthresh : int or None
        threshold below which to drop terms
    stem : bool
        whether to apply stemming/lemmatization

    Returns
    -------
    table containing all counts
    """

    if stem:
        stemmer = default_lemmatizer
    else:
        stemmer = None

    if stop_word_files:
        stop_words = []
        for swf in stop_word_files:
            stop_words.extend([t for t in open(swf, "r").read().split("\n")
                               if t != ""])
    else:
        stop_words = None

    if regex_stop_word_files:
        regex_stop_words = []
        for rswf in regex_stop_word_files:
            regex_stop_words.extend([t for t in
                                     open(rswf, "r").read().split("\n")
                                     if t != ""])
    else:
        regex_stop_words = None

    term_counts = pd.DataFrame([], columns=["term_label"])
    for txt_label in txt_labels:
        counts = pd.read_csv(os.path.join(count_dir,
                                          "%s_%dgram.csv" % (txt_label, n)))
        counts["%s_doc_count" % txt_label] = counts["doc_count"]
        counts["%s_term_count" % txt_label] = counts["term_count"]
        counts = counts[["term_label", "%s_doc_count" % txt_label,
                         "%s_term_count" % txt_label]]
        term_counts = term_counts.merge(counts.set_index("term_label"),
                                        on="term_label", how="outer")
        term_counts = term_counts.fillna(0)

    term_counts["doc_count"] = 0
    term_counts["term_count"] = 0
    for txt_label in txt_labels:
        term_counts["doc_count"] += term_counts["%s_doc_count" %
                                                txt_label]
        term_counts["term_count"] += term_counts["%s_term_count" %
                                                 txt_label]

    term_counts["raw_term"] = term_counts["term_label"]
    term_counts["count"] = term_counts["doc_count"]
    term_counts = vocab_cleaner(term_counts, stop_words=stop_words,
                                regex_stop_words=regex_stop_words,
                                doc_lthresh=doc_lthresh,
                                stemmer=stemmer)
    term_counts["term_label"] = term_counts["term"]
    term_counts["term_id"] = term_counts["term_label"].rank(method="dense")

    columns = ["raw_term", "term_label", "term_id", "doc_count", "term_count"]
    for l in txt_labels:
        columns.extend(["%s_doc_count" % l, "%s_term_count" % l])

    term_counts = term_counts[columns]

    return term_counts


def write_term_id_table(term_counts, n, txt_labels, table_dir):
    """writes the specified term_counts to a term id table

    Parameters
    ----------
    term_counts : pd DataFrame
        dataframe containing term counts
    n : int
        n for n-gram
    txt_labels : list
        list of txt categories
    table_dir : str
        location where tables are stored

    Returns
    -------
    None
    """

    term_id_table = term_counts.groupby(["term_label", "term_id"],
                                         as_index=False).sum()
    for col in term_id_table.columns:
        if term_id_table[col].dtype == np.float64:
            term_id_table[col] = term_id_table[col].astype(int)
    term_id_table.to_csv(os.path.join(table_dir, "term_%dgram.csv" % n),
                         index=False)


def gen_term_id_map(term_counts):
    """generates term id map from aggregate term counts

    Parameters
    ----------
    term_counts : pd DataFrame
        dataframe containing term counts

    Returns
    -------
    dictionary mapping raw terms to term id
    """

    term_id_map = term_counts[["raw_term", "term_id"]]
    term_id_map.index = term_id_map["raw_term"]
    term_id_map = term_id_map["term_id"].to_dict()

    return term_id_map


def gen_tag_counts(tag_label, count_dir):
    """generates a table containing the tag counts

    Parameters
    ----------
    tag_label : str
        label for tag
    count_dir : str
        location of count files

    Returns
    -------
    table containing counts
    """

    counts = pd.read_csv(os.path.join(count_dir, "%s.csv" % tag_label))
    counts = counts[["tag_label", "tag_id", "doc_count"]]
    return counts


def write_tag_id_table(tag_counts, tag_label, table_dir):
    """writes the provided tag_counts to a table file

    Parameters
    ----------
    tag_counts : pd DataFrame
        dataframe containing tag counts
    tag_label : str
        label for current tag category
    table_dir : str
        location where tables are stored

    Returns
    -------
    None
    """

    tag_counts.to_csv(os.path.join(table_dir, "tag_%s.csv" % tag_label),
                      index=False)


def gen_tag_id_map(tag_id_map):
    """generates a id map from the tag counts table

    Parameters
    ----------
    tag_id_map : pd Dataframe
        dataframe containing tag counts

    Returns
    -------
    dictionary mapping tags to tag ids
    """

    tag_id_map.index = tag_id_map["tag_label"]
    tag_id_map = tag_id_map["tag_id"].to_dict()

    return tag_id_map


def table_wrapper(ngrams, processes, txt_labels, raw_dir, count_dir,
                  table_dir, raw_files, log_file, term_count_kwds_dict,
                  data_type):
    """generates tables for specified data set

    Parameters
    ----------
    ngrams : int
        n for ngrams
    processes : int
        number of processes for pool
    txt_labels : list
        list of txt labels
    raw_dir : str
        location where raw files are stored
    count_dir : str
        location where count files are stored
    table_dir ; str
        location where tables are stored
    raw_files : str
        files to be processed
    log_file : str
        location where log is written
    term_count_kwds_dict : dict-like
        dictionary containing key words for each ngram
    data_type : str
        label for data type of current run

    Returns
    -------
    None
    """

    if data_type == "DJ":
        table_file_processor = DJt.table_file_processor
        table_post_cleaner = DJt.table_post_cleaner
    elif data_type == "NYT":
        table_file_processor = NYTt.table_file_processor
        table_post_cleaner = NYTt.table_post_cleaner
    else:
        raise ValueError("Unsupported data_type: %s" % data_type)

    t0 = datetime.now()

    # generate id maps (and write tables)

    # terms
    term_id_map_dict = {}
    for n in range(1, ngrams + 1):
        term_counts = gen_term_counts(txt_labels, n, count_dir,
                                      **term_count_kwds_dict["%dgram" % n])
        write_term_id_table(term_counts, n, txt_labels, table_dir)
        term_id_map = gen_term_id_map(term_counts)
        term_id_map_dict["%dgram" % n] = term_id_map

    # tags
    t_diff = ["%s_%dgram" % (l, n) for l in txt_labels
              for n in range(1, ngrams + 1)]
    t_files = [t.replace(".csv", "") for t in os.listdir(count_dir)]
    tags = [t for t in t_files if t not in t_diff]

    tag_id_map_dict = {}
    for t in tags:
        tag_counts = gen_tag_counts(t, count_dir)
        write_tag_id_table(tag_counts, t, table_dir)
        tag_id_map = gen_tag_id_map(tag_counts)
        tag_id_map_dict[t] = tag_id_map

    t1 = datetime.now()
    with open(log_file, "a") as fd:
        fd.write("id_tables,%s,%s\n" % (str(t1), str(t1 - t0)))

    # produce all other tables
    pool = multiprocessing.Pool(processes)

    pool.starmap(table_file_processor, [(f, raw_dir, table_dir, ngrams,
                                         tag_id_map_dict, term_id_map_dict,
                                         log_file) for f in raw_files])

    # apply any post cleaning methods
    if table_post_cleaner:
        pool.starmap(table_post_cleaner, [(f, table_dir, log_file)
                                          for f in raw_files])
