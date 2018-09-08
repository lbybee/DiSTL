import pandas as pd
import numpy as np
import multiprocessing
import sys
import os


def load_term_id(source_data_dir, ngrams):
    """loads the term ids and generates a map from term ids to terms

    Parameters
    ----------
    source_data_dir : str
        location of source data files
    ngrams : int
        number of ngrams

    Returns
    -------
    term_id_offs

    """
    term_id_map_dict = {}
    for n in ngrams:
        term_id = pd.read_csv(os.path.join(source_data_dir,
                                           "term_id_%s.csv" % n))
        term_id_map = term_id.copy()
        term_id_map.index = term_id_map["term_id"]
        term_id_map = term_id_map["term"]
        term_id_map_dict[n] = term_id_map

    return term_id_map_dict


def gen_doc_id_map(doc_id, col_labels):
    """generates the maps from doc ids to doc labels given the
    provided doc id file

    Parameters
    ----------
    doc_id : pandas df
        input data frame

    Returns
    -------
    dictionary containing map for each label/content column
    """

    doc_id_map = doc_id.copy()
    doc_id_map.index = doc_id_map["doc_id"]

    col_labels = ["permno", "date"]

    doc_id_map_dict = {}
    for l in col_labels:
        doc_id_map_dict[l] = doc_id_map[l]

    return doc_id_map_dict


def date_builder(date, source_data_dir, out_data_dir, ngrams,
                 term_id_map_dict, doc_id_type="ticker_day"):
    """loads the doc ids and counts and writes to new file which is
    denormalized (so mapping through intermediate indices)

    Parameters
    ----------
    date : str
        label for current date file
    source_data_dir : str
        location where input files are located
    out_data_dir : str
        location where output will be stored
    ngrams : list
        list of ngrams
    term_id_map_dict : dict-like
        dictionary mapping term ids to terms
    doc_id_type : str
        label for type of content variables in doc_id

    Returns
    -------
    None

    Writes
    ------
    DTM file containing doc metadata, term and count
    """

    doc_id = pd.read_csv(os.path.join(source_data_dir,
                                      "doc_id_%s.csv" % date))

    if doc_id_type == "ticker_day":
        doc_id_map_dict = gen_doc_id_map(doc_id, ["permno", "date"])
    elif doc_id_type == "ticker_month":
        doc_id_map_dict = gen_doc_id_map(doc_id, ["permno", "year", "month"])
    else:
        raise ValueError("unsupported doc_id_type: %s" % doc_id_type)
    keys = [k for k in doc_id_map_dict.keys()]

    # apply new doc_id to counts

    total_count = pd.DataFrame([], columns=keys + ["term", "count"])

    for ngram in ngrams:
        term_id_map = term_id_map_dict[ngram]
        count = pd.read_csv(os.path.join(source_data_dir,
                                         "count_%s_%s.csv" % (ngram, date)))
        count["term"] = count["term_id"].map(term_id_map)
        for k in doc_id_map_dict:
            count[k] = count["doc_id"].map(doc_id_map_dict[k])
        count = count[keys + ["term", "count"]]
        count = count.dropna()
        total_count = pd.concat([total_count, count])

    total_count = total_count.sort_values(keys + ["term"])
    total_count.to_csv(os.path.join(out_data_dir, "DTM_%s.csv" % date),
                       index=False)


def denormalize_wrapper(source_data_dir, out_data_dir, processes,
                        doc_id_type="ticker_day"):
    """runs the denormalizing code

    Parameters
    ----------
    source_data_dir : str
        location of input files
    out_data_dir : str
        location where output will be stored
    processes : int
        number of processes for multiprocessing
    doc_id_type : str
        label for type of ocntent variables in doc_id

    Returns
    -------
    None

    Writes
    ------
    DTM files containing doc metadata, terms and counts
    """

    dates = []
    ngrams = []
    for f in os.listdir(source_data_dir):
        if "doc_id" in f:
            dates.append("_".join(f.split(".")[0].split("_")[2:]))
        if "term_id" in f:
            ngrams.append(f.split(".")[0].split("_")[2])
    dates.sort()
    ngrams.sort()

    term_id_map_dict = load_term_id(source_data_dir, ngrams)

    pool = multiprocessing.Pool(processes)

    pool.starmap(date_builder, [(d, source_data_dir, out_data_dir,
                                 ngrams, term_id_map_dict, doc_id_type)
                                for d in dates])
