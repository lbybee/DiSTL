from DiSTL.cleaning_util import vocab_cleaner
from pymongo import MongoClient
from datetime import datetime
from dask import delayed, compute
import dask.dataframe as dd
import pandas as pd
import numpy as np
import dask.multiprocessing
import logging
import shutil
import glob
import imp
import csv
import sys
import os
import re


"""
Code for building a token database from raw txt.  Currently, this supports
Mongodb for the token database.

At a high level the way this works:

    1. The config file contains several methods which will be used here

    1.a doc_processor

    Takes individual documents as well as a tokenizer (and any use case
    specific params) and applies any necessary cleaning to the document
    before insertion

    1.b doc_generator

    Yields documents for each partition

    1.c part_kwds_generator

    Yields partition specific key-words needed by methods here.  These
    are

    1.c.i backend_kwds
    1.c.ii doc_generator_kwds

    2. In addition to these methods the config file contains  a number of
    additional parameters

    2.a partitions
    2.b vocab_file
    2.c vocab_map_kwds
    2.d tokenizer_kwds
    2.e doc_processor_kwds
    2.f part_kwds_generator_kwds
    2.g log_file
    2.h log_info

    3. The key build steps are then as follows:

    3.a build_vocab_map

    If any clenaing is being done as part of the tokenization a dictionary
    is generated which maps the possible raw terms to clean versions.

    Note that in order for this to work, you need to provide a complete
    vocab for the data which may be difficult if this is the first time
    loading it.

    How best to bootstrap this is in the works.

    3.b build_wrapper

    build_wrapper calls partition_handler in parallel over each of the
    provided partitions using dask.

    Additionally, it assembles the doc_generator by combining all the
    necessary tokenization and cleaning components.  This is done here because
    we assume that the tokenization and cleaning is homogenous across
    partitions

    3.c partition_handler

    This handles each partition independently, it wraps whatever backend
    is used to actually insert the documents

"""


def build_vocab_map(vocab_file, stop_words=None, regex_stop_words=None,
                    D=None, doc_lthresh=None, doc_uthresh=None,
                    tfidf_thresh=None, stemmer=None,
                    term_length=None, log=True):
    """
    If a pre-build vocab is build we can use that to apply cleaning rules
    to the text as we tokenize.  This function loads that vocab and
    generates a map from raw_term -> clean term (term)

    Parameters
    ----------
    vocab_file : str
        location where vocab is stored
    stop_words : list or None
        list of words which should be removed from vocab
    regex_stop_words : list or None
        list of regex patterns which should be removed from vocab
    stemmer : function or None
        function applied to pandas Series to generated stemmed
        version of series
    doc_<lthresh|uthresh> : scalar
        <lower|upper> threshold for stemming/stop word doc count thresholding
    D : int or None
        D is the number of documents in the corpus if D is > 0 we assume
        the doc_<X> thresh is between 0 and 1 so we multiply by D, otherwise
        we use the threshold as-is.
    tfidf_thresh : scalar
        tfidf threshold below for stemming/stop words thresholding.
    term_length : scalar
        minimum length required for term

    Returns
    -------
    dictionary mapping raw_term to clean term (term)
    """

    vocab = pd.read_csv(vocab_file)
    vocab = vocab.dropna()

    vocab = vocab_cleaner(vocab, stop_words=stop_words,
                          regex_stop_words=regex_stop_words,
                          D=D, doc_lthresh=doc_lthresh,
                          doc_uthresh=doc_uthresh,
                          tfidf_thresh=tfidf_thresh,
                          stemmer=stemmer, term_length=term_length)

    vocab = vocab[["raw_term", "term"]]
    vocab.index = vocab["raw_term"]
    vocab_map = vocab["term"].to_dict()
    return vocab_map


def _default_tokenizer(text_string, regex_chars="[^ a-zA-Z]", lower=True,
                       regex_token=re.compile(r"(?u)\b\w\w\w+\b"),
                       vocab_map=None, ngrams=1):
    """tokenizes a txt string using the provided parameters

    Parameters
    ----------
    text_string : str
        text string to be processed
    regex_chars : str
        regex pattern which characters must match to keep, the default
        is all alphabetical characters
    lower : str
        whether to set all chars to lower case
    regex_token : regex pattern
        pattern for the actual tokenzing, the default breaks up by space
        defining words as 3+ characters
    vocab_map : dict-like
        optional cleaning dictionary mapping raw terms to clean terms
    ngrams : int
        n in n-grams (one is unigrams, 2 is unigrams+bigrams, etc.)

    Returns
    -------
    list of lists, where each nested list contains the corresponding n-grams
    (so if ngrams=1 this is just a list containing a list of unigrams
     if ngrams=2 this is a list containing a list of unigrams and a list
     of bigrams)
    """

    # TODO should return something cleaner here

    text_string = re.sub(regex_chars, "", text_string)
    if lower:
        text_string = text_string.lower()
    text_list = regex_token.findall(text_string)

    if vocab_map:
        n_text_list = []
        for t in text_list:
            try:
                t = vocab_map[t]
                n_text_list.append(t)
            except:
                continue
    else:
        n_text_list = text_list

    ngram_lists = [n_text_list]
    for n in range(2, ngrams + 1):
        ngram_lists.append([" ".join(j) for j in
                            zip(*[n_text_list[i:] for i in range(n)])])

    return ngram_lists


def _mongodb_backend(doc_generator, doc_processor, db, col, rebuild=False):
    """
    backend for adding tokens to mongodb database instance

    Parameters
    ----------
    doc_generator : generator
        iterable that yields documents, these documents are then processed
        by the doc_processor and added to the DB
    doc_processor : function
        method for processing each document yielded by the doc_generator
    db : str
        database label to store tokens
    col : str
        collection label to store tokens
    rebuild : bool
        indicator for whether to rebuild the collection or append

    Returns
    -------
    None
    """

    client = MongoClient()
    db = client[db]
    if rebuild:
        db.drop_collection(collection_name)
    col = db[col]

    docs = []
    for d in doc_generator:
        d = doc_processor(d)
        if d:
            docs.append(d)
    if len(docs) > 0:
        col.insert_many(docs)

    client.close()


def partition_handler(partition, backend, doc_generator, doc_processor,
                      backend_kwds=None, doc_generator_kwds=None,
                      log_file=None, log_info=None):
    """
    takes a document generator and inserts the yielded documents into the
    specified database backend

    Parameters
    ----------
    partition : str
        label for current partition
    backend : str
        label corresponding to backend
    doc_processor : function
        method for processing individual documents
    backend_kwds : dict-like or None
        key-words passed to backend
    doc_generator_kwds : dict-like or None
        key-words passed to doc_generator
    log_file : str or None
        location of log file if used (logs how long this takes)
    log_info : str or None
        label info to include in log output

    Returns
    -------
    None
    """

    t0 = datetime.now()

    if backend_kwds is None:
        backend_kwds = {}
    if doc_generator_kwds is None:
        doc_generator_kwds = {}

    doc_generator_init = doc_generator(**doc_generator_kwds)

    if backend == "mongodb":
        _mongodb_backend(doc_generator_init, doc_processor, **backend_kwds)
    else:
        raise ValueError("backend: %s, is currently not supported" % backend)

    if log_file:
        t1 = datetime.now()
        with open(log_file, "a") as f:
            f.write("%s %s %s\n" % (log_info, partition, str(t1 - t0)))


def build_wrapper(partitions, backend, doc_processor, doc_generator,
                  vocab_file=None, tokenizer_base=_default_tokenizer,
                  part_kwds_generator=None, vocab_map_kwds=None,
                  tokenizer_kwds=None, doc_processor_kwds=None,
                  part_kwds_generator_kwds=None, log_file=None,
                  log_info=None):
    """
    runs the partition_handle in parallel over the different partitions

    Parameters
    ----------
    partitions : iterable
        list of partition labels
    backend : str
        label corresponding to backend
    doc_processor : function
        method for processing documents
    doc_generator : function
        method for generating documents
    backend_kwds : dict-like or None
        key-words passed to backend
    vocab_file : str or None
        location of vocab file for vocab_map if provided
    tokenizer_base : function
        tokenizer method for parsing strings into tokens
    part_kwds_generator : generator or None
        generator for producing kwds passed to backend and doc_generator
        that may vary by partition
    vocab_map_kwds : dict-like or None
        key-words passed to build_vocab_map
    tokenizer_kwds : dict-like or None
        key-words passed to tokenizer
    doc_processor_kwds : dict-like or None
        key-words passed to doc_processor
    part_kwds_generator_kwds : dict-like or None
        key-words passed to part_kwds_generator_kwds
    log_file : str or None
        location of log file if used (logs how long this takes)
    log_info : str or None
        label info to include in log output

    Returns
    -------
    None
    """

    t0 = datetime.now()

    if vocab_map_kwds is None:
        vocab_map_kwds = {}
    if tokenizer_kwds is None:
        tokenizer_kwds = {}
    if doc_processor_kwds is None:
        doc_processor_kwds = {}

    if part_kwds_generator is None:

        def part_kwds_generator(partitions):
            for part in partitions:
                yield part, {}, {}

    # prep a temporary tokenizer function using the tokenizer_kwds and
    # vocab map which can passed into doc_processor_init to pass cleanly
    # to the lower lvls
    # TODO this could maybe be rethought

    if vocab_file:
        tokenizer_kwds["vocab_map"] = build_vocab_map(vocab_file,
                                                      **vocab_map_kwds)

    tokenizer = lambda x: tokenizer_base(x, **tokenizer_kwds)
    doc_processor_init = lambda x: doc_processor(x, tokenizer,
                                                 **doc_processor_kwds)

    for part, backend_kwds, doc_generator_kwds in part_kwds_generator(partitions, **part_kwds_generator_kwds):
        partition_handler(part, backend, doc_generator, doc_processor_init,
                          backend_kwds, doc_generator_kwds, log_file,
                          log_info)
#    del_l = [delayed(partition_handler)(part, backend, doc_generator,
#                                        doc_processor_init, backend_kwds,
#                                        doc_generator_kwds, log_file,
#                                        log_info)
#            for part, backend_kwds, doc_generator_kwds
#            in part_kwds_generator(partitions, **part_kwds_generator_kwds)]
#
#    compute(*del_l, scheduler="processes")

    if log_file:
        t1 = datetime.now()
        with open(log_file, "a") as f:
            f.write("%s all %s\n" % (log_info, str(t1 - t0)))
