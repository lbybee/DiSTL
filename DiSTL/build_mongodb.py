from pymongo import MongoClient
from datetime import datetime
from dask import delayed
import dask.dataframe as dd
import pandas as pd
import numpy as np
import logging
import shutil
import dask
import imp
import csv
import sys
import os
import re


"""
Build code optimized to handle input from a mongo database instance.

At a high level the way this works:

    1. The config file contains several aggregation pipelines, as well
    as key cleaning params.

        1.a search_pipeline

        returns a subset of documents matching the search criteria

        1.b vocab_pipeline

        returns a set of terms and any correpsonding counts, applied
        after the search_pipeline

        1.c dtm_pipeline

        returns document term pairs with their corresponding counts
        to build sparse DTM representation

        1.d vocab_cleaning_params

        A dictionary which includes all the arguments passed to
        the vocab cleaning method

    2. The run process is then structured as follows:

        2.a build_vocab

        This calls many instances of chunk_build_vocab in parallel.
        Each of these opens a different connection to the database to
        build its portion of the vocab.

        The results are then combined into one large dataframe

        2.b clean_vocab

        Takes the results from build_vocab, and applies all the desired
        cleaning rules.  The result of this is two maps

            3.b.i raw_term -> term_id
            3.b.ii clean_term -> term_id

        These maps can then be applied by build_DTM to introduce any
        cleaning desired.

        2.c build_DTM

        Calls many instances of chunk_build_DTM in parallel.  Each of
        these go to the database, pulls out documents, applies the
        vocab map and returns the doc_ids and triplet representation
        for its portion of the DTM

        2.d

    3. All of this is wrapped by TBA

"""


# TODO list
# 1. generalize dict keys


def chunk_build_vocab(db, collection, pipeline, data_dir, log):
    """
    build vocab based on collection from database

    Parameters
    ----------
    db : str
        name of database
    collection : str
        name of collection
    pipeline : list of dictionaries
        aggregation pipeline
    data_dir : str
        location where data files are stored
    log : bool
        indicator for whether to record a log entry when done

    Returns
    -------
    None
    """

    t0 = datetime.now()
    fname = "tmp_vocab_%s.csv" % collection

    client = MongoClient()
    client.admin.command({"setParameter": 1, "cursorTimeoutMillis": 60000000})
    iterator = client[db][collection].aggregate(pipeline, allowDiskUse=True)
    df = pd.DataFrame([r for r in iterator])
    df.to_csv(os.path.join(data_dir, fname), index=False)
    client.close()

    if log:
        t1 = datetime.now()
        with open(os.path.join(data_dir, "DTM.log"), "a") as f:
            f.write("v %s %s\n" % (collection, str(t1 - t0)))


def build_vocab(db, collections, search_pipeline, vocab_pipeline,
                data_dir, log=True):
    """
    assembles the temporary vocab files

    Parameters
    ----------
    db : str
        name of database
    collection : iterable
        list of collection names
    search_pipeline : list of dictionaries
        aggregation pipeline for searching database
    vocab_pipeline : list of dictionaries
        aggregation pipeline for vocab database
    data_dir : str
        location where data files are stored
    log : bool
        indicator for whether to record a log entry when done
    """

    pipeline = search_pipeline + vocab_pipeline

    del_l = [delayed(chunk_build_vocab)(db, c, pipeline, data_dir, log)
             for c in collections]
    dask.compute(*del_l)


def clean_vocab(data_dir, stop_words=None, regex_stop_words=None,
                pre_doc_lthresh=None, pre_doc_uthresh=None,
                pre_tfidf_thresh=None, stemmer=None,
                pre_term_length=None, log=True):
    """
    loads the temporay vocab files, returned from each process,
    and combines these into one vocab by summing the counts.
    Any desired cleaning rules and then a term id is generated
    for each remaining term.

    The resulting maps from

    raw_term -> term_id
    clean_term -> term_id

    are then written to the data directory.

    Parameters
    ----------
    data_dir : str
        location where data files are stored
    stop_words : list or None
        list of words which should be removed from vocab
    regex_stop_words : list or None
        list of regex patterns which should be removed from vocab
    stemmer : function or None
        function applied to pandas Series to generated stemmed
        version of series
    pre_doc_<lthresh|uthresh> : scalar
        <lower|upper> threshold for pre-stemming/stop word doc
        count thresholding should be between 0 and 1, is multiplied by
        D to determine count
    pre_tfidf_thresh : scalar
        tfidf threshold below for pre-stemming/stop words thresholding.
    pre_term_length : scalar
        minimum length required for term
    log : bool
        indicator for whether to record a log entry when done

    Returns
    -------
    None
    """

    t0 = datetime.now()

    # load and aggregate vocab
    tmp_vocab_files = os.path.join(data_dir, "tmp_vocab_*.csv")
    vocab = dd.read_csv(tmp_vocab_files)
    vocab = vocab.groupby("_id").sum().compute()
    vocab["raw_term"] = vocab.index
    vocab = vocab.reset_index(drop=True)


    # now apply cleaning rules

    # removing stop words
    if stop_words:
        vocab = vocab[~vocab["raw_term"].isin(stop_words)]

    # remove regex stop words
    if regex_stop_words:
        for term in list(regex_stop_words):
            vocab = vocab[~vocab["raw_term"].str.contains(term, na=False)]

    # apply count thresholding
    if pre_doc_lthresh:
        thresh = D * pre_doc_lthresh
        vocab = vocab[vocab["count"] >= thresh]
    if pre_doc_uthresh:
        thresh = D * pre_doc_uthresh
        vocab = vocab[vocab["count"] <= thresh]

    # apply tfidf thresholding
    if pre_tfidf_thresh:
        raise ValueError("Tf-Idf thresholding is currently not supported")

    # apply stemming
    if stemmer:
        vocab["term"] = stemmer(vocab[["raw_term"]])
    else:
        vocab["term"] = vocab["raw_term"]

    # remove terms below term length
    if pre_term_length:
        fn = lambda x: len(x) >= pre_term_length
        vocab = vocab[vocab["term"].apply(fn)]

    # now generate term_id maps
    vocab = vocab.reset_index(drop=True)
    vocab["term_id"] = vocab.index


    # write the results
    vocab.to_csv(os.path.join(data_dir, "vocab.csv"), index=False)

    # remove temporary files
    for f in glob.glob(tmp_vocab_files):
        os.remove(os.path.join(data_dir, f))

    if log:
        t1 = datetime.now()
        with open(os.path.join(data_dir, "DTM.log"), "a") as f:
            f.write("vc %s\n" % str(t1 - t0))


def chunk_build_DTM(db, collection, pipeline, data_dir, term_id_map, log):
    """
    build DTM based on collection from database

    Parameters
    ----------
    db : str
        name of database
    collection : str
        name of collection
    pipeline : list of dictionaries
        aggregation pipeline
    data_dir : str
        location where data files are to be stored
    term_id_map : dict-like
        map from raw_term -> term_id.  If a term doesn't end up in the vocab
        it shouldn't be in this dict
    log : bool
        indicator for whether to record a log entry when done

    Returns
    -------
    None
    """

    t0 = datetime.now()
    doc_id_fname = "tmp_doc_id_%s.csv" % collection
    DTM_fname = "tmp_DTM_%s.csv" % collection


    client = MongoClient()
    client.admin.command({"setParameter": 1, "cursorTimeoutMillis": 60000000})
    iterator = client[db][collection].aggregate(pipeline, allowDiskUse=True)

    doc_id_l = []
    DTM_l = []

    for doc_id, doc in enumerate(iterator):

        metadata = doc["_id"]
        metadata["doc_id"] = doc_id
        doc_id_l.append(metadata)

        term_dict = doc["txt"]
        for term in term_dict:
            # We do try except here because it seems to be quicker
            # than checking for the existence of a term in term_id_map
            # first.  There may be a better way to do this...
            try:
                term_id =
                term_id = term_id_map[term]
                count = term_dict[term]
                triplet = {"term_id": term_id, "doc_id": doc_id,
                           "count": count}
                DTM_l.append(triplet)
            except:
                continue
    doc_id = pd.DataFrame(doc_id_l)
    doc_id.to_csv(os.path.join(data_dir, doc_id_fname), index=False)
    DTM = pd.DataFrame(DTM_l)
    DTM.to_csv(os.path.join(data_dir, DTM_fname), index=False)
    client.close()

    if log:
        t1 = datetime.now()
        with open(os.path.join(data_dir, "DTM.log"), "a") as f:
            f.write("d %s %s\n" % (collection, str(t1 - t0)))


def build_DTM(db, collections, search_pipeline, dtm_pipeline, term_id_map,
              log=True):
    """
    assembles the temporary DTM files

    Parameters
    ----------
    db : str
        name of database
    collection : iterable
        list of collection names
    search_pipeline : list of dictionaries
        aggregation pipeline for searching database
    dtm_pipeline : list of dictionaries
        aggregation pipeline for DTM database
    term_id_map : dict-like
        map from raw_term -> term_id.  If a term doesn't end up in the vocab
        it shouldn't be in this dict
    data_dir : str
        location where data files are stored
    log : bool
        indicator for whether to record a log entry when done

    Returns
    -------
    None
    """

    pipeline = search_pipeline + dtm_pipeline

    del_l = [delayed(chunk_build_DTM)(db, c, pipeline, data_dir,
                                      term_id_map, log)
             for c in collections]
    dask.compute(*del_l)


def clean_DTM(data_dir, log=True):
    """
    loads the temporary DTM files generated by build_DTM, the doc_ids are
    combined into one large doc_id index.  This index is mapped to the DTM
    and the resulting files are written to the data directory

    Parameters
    ----------
    data_dir : str
        location where data files are stored
    log : bool
        indicator for whether to record a log entry when done

    Returns
    -------
    None
    """

    t0 = datetime.now()

    # load tmp files
    tmp_doc_files = os.path.join(data_dir, "tmp_doc_id_*.csv")
    doc_id_dd = dd.read_csv(tmp_doc_files, blocksize=None)

    tmp_dtm_files = os.path.join(data_dir, "tmp_DTM_*.csv")
    dtm_dd = dd.read_csv(tmp_dtm_files, blocksize=None)

    # generate correct doc_id
    doc_id_dd["new_doc_id"] = 1
    doc_id_dd["new_doc_id"] = doc_id_dd["new_doc_id"].cumsum()

    # now map new id to old id
    delayed_dtm = dtm_dd.to_delayed()
    delayed_doc_id = doc_id_dd.to_delayed()
    dtm_l = len(delayed_dtm)
    doc_l = len(delayed_doc_id)
    if dtm_l != doc_l:
        raise ValueError("doc_id and DTM chunks aren't same length")

    def zip_mapper(dtm_i, doc_id_i):

        doc_id_i.index = doc_id_i["doc_id"]
        id_dict = doc_id_i["new_doc_id"]
        dtm_i["doc_id"] = dtm_i["doc_id"].map(id_dict)
        return dtm_i

    del_l = [delayed(zip_mapper)(dtm_i, doc_i)
             for dtm_i, doc_i in zip(delayed_dtm, delayed_doc_id)]
    dtm_dd = dd.from_delayed(del_l)
    doc_id_dd["doc_id"] = doc_id_dd["new_doc_id"]
    doc_id_dd = doc_id_dd.drop("new_doc_id", axis=1)

    # write results to data file
    dtm_dd.to_csv(os.path.join(data_dir, "DTM_*.csv"), index=False)
    doc_id_dd.to_csv(os.path.join(data_dir, "doc_id_*.csv"), index=False)

    # remove tmp files
    tmp_doc_files = glob.glob(tmp_doc_files)
    for f in tmp_doc_files:
        os.remove(f)
    tmp_dtm_files = glob.glob(tmp_dtm_files)
    for f in tmp_dtm_files:
        os.remove(f)

    if log:
        t1 = datetime.now()
        with open(os.path.join(data_dir, "DTM.log"), "a") as f:
            f.write("dc %s\n" % str(t1 - t0))
