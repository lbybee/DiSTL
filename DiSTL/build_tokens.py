from DiSTL.cleaning_util import vocab_cleaner
from pymongo import MongoClient
from datetime import datetime
from dask import delayed
import dask.dataframe as dd
import pandas as pd
import numpy as np
import logging
import shutil
import glob
import dask
import imp
import csv
import sys
import os
import re


regex = re.compile(r"(?u)\b\w\w\w+\b")


def build_vocab_map(vocab_file, doc_thresh=10, stop_words=None,
                    regex_stop_words=None):
    """generates vocab map removing base stop words, terms appearing
    in very few documents, and performs stemming (lemmatizing)
    """

    vocab = pd.read_csv(vocab_file)
    vocab = vocab.dropna()

    # drop rare terms
    vocab = vocab[vocab["count"] >= doc_thresh]

    # remove stop words
    if stop_words is not None:
        vocab = vocab[~vocab["term"].isin(stop_words)]

    # remove regex stop words
    if regex_stop_words is not None:
        for term in list(regex_stop_words):
            vocab = vocab[~vocab["term"].str.contains(term, na=False)]

    vocab["stem"] = _default_lemmatizer(vocab[["term"]].copy())
    vocab.index = vocab["term"]
    vocab_map = vocab["stem"].to_dict()
    return vocab_map


def tokenize_txt(text_string, vocab_map=None, ngrams=1):
    """remove unwanted characters and convert string
    to list"""
    text_string = re.sub("[^ a-zA-Z]", "", text_string)
    text_string = text_string.lower()
    text_list = regex.findall(text_string)


    n_text_list = []
    for t in text_list:
        try:
            t = vocab_map[t]
            n_text_list.append(t)
        except:
            continue

    ngram_lists = [n_text_list]
    for n in range(2, ngrams + 1):
        ngram_lists.append([" ".join(j) for j in
                            zip(*[n_text_list[i:] for i in range(n)])])

    return ngram_lists


def _mongodb_backend(doc_generator, doc_process, doc_processor_kwds,
                     db, col, rebuild=False, log_file=None,
                     log_info=None):
    """
    backend for adding tokens to mongodb database instance

    Parameters
    ----------
    doc_generator : generator
        iterable that yields documents, these documents are then processed
        by the doc_processor and added to the DB
    doc_processor : function
        method for processing each document yielded by the doc_generator
    doc_processor_kwds : dict-like
        key-words passed to doc_processor
    db : str
        database label to store tokens
    col : str
        collection label to store tokens
    rebuild : bool
        indicator for whether to rebuild the collection or append
    log_file : str or None
        location of log file if used (logs how long this takes)
    log_info : str or None
        label info to include in log output

    Returns
    -------
    None
    """

    t0 = datetime.now()

    client = MongoClient()
    db = client[db]
    if rebuild:
        db.drop_collection(collection_name)
    col = db[collection_name]

    docs = []
    for d in doc_generator:
        d = doc_processor(d, **doc_processor_kwds)
        if d:
            docs.append(d)
    if len(docs) > 0:
        col.insert_many(docs)

    if log_file:
        t1 = datetime.now()
        with open(log_file, "a") as f:
            f.write("%s %s %s\n" % (log_info, partition, str(t1 - t0)))



def inserter(files, db_name, collection_name, data_processor, doc_builder,
             finished_file=None, data_processor_kwds=None,
             doc_builder_kwds=None, rebuild=False):
    """iterates through the file applying the data processor to
    each file, prepping the documents for insertion and inserting
    the documents for each file

    Parameters
    ----------
    files : list
        list of files for processing
    db_name : string
        name of database
    collection_name : string
        name of collection
    data_processor : generator
        takes a string corresponding to a file, or a list of files
        and generates a list of documents
    doc_builder : function
        converts a list of documents returned by data_processor into
        a list of dictionaries that can be inserted into the database
    data_processor_kwds : dict-like or None
        key-words for data_processor
    doc_builder_kwds : dict-like or None
        key-words for doc_builder
    rebuild : bool
        if true, this will assume that the collection needs to be overwritten
        Useful when a collection corresponds to file and something failed
        in the middle of a write

    Returns
    -------
    None
    """

    client = MongoClient()
    db = client[db_name]
    if rebuild:
        db.drop_collection(collection_name)
    col = db[collection_name]

    if data_processor_kwds is None:
        data_processor_kwds = {}
    if doc_builder_kwds is None:
        doc_builder_kwds = {}

    t0 = datetime.now()
    for f in files:
        t1 = datetime.now()
        docs_iter = data_processor(f, **data_processor_kwds)
        docs = []
        for d in docs_iter:
            d = doc_builder(d, **doc_builder_kwds)
            if d:
                docs.append(d)
        if len(docs) > 0:
            col.insert_many(docs)
        else:
            print("No documents inserted for file", f)

        if finished_file is not None:
            with open(finished_file, "a") as ofile:
                writer = csv.writer(ofile)
                writer.writerow([f, str(datetime.now() - t1),
                                 str(datetime.now() - t0)])

    client.close()
    return None
