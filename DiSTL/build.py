"""
Includes the code necessary to build a document term data-frame (DTDF).
There are two primary components here:

1. make_DTDF

make_DTDF is a function which takes some document source, as well
as an instance of DTDFBuilder to assemble the necessary files for
a DTDF.  Currently it supports the following sources:

    1.a Mongodb aggregation generator
    1.b csv files

2. DTDFBuilder

DTDFBuilder is a class containing all the information required to
clean a DTDF from source files, as well as the methods needed to
build/clean the DTDF.  These methods will be called within make_DTDF.
"""
import dask.dataframe as dd
import dask.bag as db
import pandas as pd
import numpy as np
import glob
import json
import os
import re


# ----------------- #
# 1. make_DTDF code #
# ----------------- #


def _gen_docs_csv(file_pattern, text_column="text"):
    """
    a generator for producing doc_groups from a series (or single)
    raw CSV file

    Parameters
    ----------
    file_pattern : str
        a globable file pattern where each file name can be passed
        to pandas.read_csv
    text_column : str
        the column in the resulting pandas DataFrame which corresponds
        to the text, all other columns are considered index

    Yields
    ------
    DataFrame.iter_rows()
    """

    files = glob.glob(file_pattern)
    for f in files:
        df = pd.read_csv(f)
        text = df[text_column].tolist()
        df = df.drop(text_column, axis=1)
        yield df, text


def _gen_docs_mongodb(config):

    if os.path.isfile(config):
        with open(config, "r") as ifile:
            config = json.load(ifile)
    elif type(config) is dict:
        config = config
    else:
        raise ValueError("Unsupported config type")

    db = config["db"]
    collections = config["collections"]
    pipeline = config["pipeline"]

    client = MongoClient()
    client.admin.command({"setParameter": 1,
                          "cursorTimeoutMillis": 60000000})
    for col in collections:
        yield client[db][col].aggregate(pipeline, allowDiskUse=True)


def make_DTDF(source, DTDF_dir, inp_DTDFBuilder=None, source_type="csv",
              doc_type=None, gen_kwds=None, vocab_kwds=None,
              DTDF_kwds=None):
    """
    wrapper for making DTDF.

    Parameters
    ----------
    source : multiple
        argument passed to _gen_docs_<source_type>, dependings on
        source_type.
        location where source files are stored
    DTDF_dir : str
        location where DTDF files should be stored
    inp_DTDFBuilder : multiple
        Supported types are:

            1. DTDTBuilder instance
            2. dict-like
                In this case DTDFBuilder instance is created
            3. None
                In this case generic DTDFBuilder instance is created

    source_type : str
        type of source used, currently supports:

            a. json config file containing mongodb aggregation pipeline
               as well database and collection names
            b. directory containing csv files

    doc_type : str or None
        what type is each element in the iterable?
        Supported types:

            - str
            - list
            - dict

    gen_kwds : dict-like or None
        additional kwds to pass to _gen_DTDF_<source_type>
    vocab_kwds : dict-like or None
        kwds passed to build_vocab
    DTDF_kwds : dict-like or None
        kwds passed to build_DTDF

    Returns
    -------
    None, stores files in DTDF_dir
    """

    # TODO support missing vocab_dict
    # TODO support json inp_DTDFBuilder

    # update any kwds
    if gen_kwds is None:
        gen_kwds = {}
    if vocab_kwds is None:
        vocab_kwds = {}
    if DTDF_kwds is None:
        DTDF_kwds = {}
    if doc_type is not None:
        vocab_kwds["doc_type"] = doc_type
        DTDF_kwds["doc_type"] = doc_type

    # initialize DTDFBuilder
    if inp_DTDFBuilder is None:
        inp_DTDFBuilder = {}
    if type(inp_DTDFBuilder) is dict:
        inst_DTDFBuilder = DTDFBuilder(**inp_DTDFBuilder)
    elif type(inp_DTDFBuilder) is DTDFBuilder:
        inst_DTDFBuilder = inp_DTDFBuilder
    else:
        raise ValueError("Unsupported type for inp_DTDFBuilder")

    # get document generator
    if source_type == "csv":
        doc_group_gen = _gen_docs_csv(source, **gen_kwds)
    elif source_type == "mongodb":
        doc_group_gen = _gen_docs_mongodb(source, **gen_kwds)
    else:
        raise ValueError("Unsupported source_type %s" % source_type)

    # build vocab
    inst_DTDFBuilder.build_vocab(doc_group_gen, DTDF_dir, **vocab_kwds)
    inst_DTDFBuilder.clean_vocab(DTDF_dir)

    # build DTDF
    inst_DTDFBuilder.build_DTDF(doc_group_gen, DTDF_dir, **DTDF_kwds)
    inst_DTDFBuilder.clean_DTDF(DTDF_dir)


# ------------------- #
# 2. DTDFBuilder code #
# ------------------- #


regex = re.compile(r"(?u)\b\w\w\w+\b")

def _default_parser(doc):
    """parses the string

    Parameters
    ----------
    doc : str
        unclean string (corresponding to doc)

    Returns
    -------
    cleaned list of terms in doc
    """

    doc = re.sub("[^ a-zA-Z]", "", doc)
    doc = doc.lower()
    return regex.findall(text_string)


def _pre_tokenizer(doc):
    """special case of tokenizer when there is
    no vocab_dict (generally used for generating
    vocab_dict).

    Parameters
    ----------
    doc : list
        list of terms

    Returns
    -------
    dictionary of token counts
    """

    n_doc = {}

    for term in doc:
        if term not in n_doc:
            n_doc[term] = 1
        else:
            n_doc[term] += 1

    return n_doc


def _unigram_tokenizer(doc, vocab_dict):
    """special case of tokenizer for unigrams

    Parameters
    ----------
    doc : list
        list of terms
    vocab_dict : dict-like
        map from unique term to clean unique term

    Returns
    -------
    dictionary of token counts
    """

    n_doc = {}

    for term in doc:
        try:
            term = vocab_dict[term]
            if term not in n_doc:
                n_doc[term] = 1
            else:
                n_doc[term] += 1
        except
            continue

    return n_doc


def _tokenizer(doc, vocab_dict, n_grams, mult_grams):
    """converts list of terms into dict of token counts.

    Parameters
    ----------
    doc : list
        list of terms
    vocab_dict : dict-like
        map from unique term to clean unique term
    n_grams : scalar
        token word count, e.g. n_grams == 1: unigrams
        n_grams == 2: bigrams
    mult_grams : scalar or bool
        indicator for whether n_grams should be inclusive.
        If True, for n_grams > 1 also generate all lower
        order n_grams, e.g. n_grams == 2: unigrams + bigrams

    Returns
    -------
    dictionary of token counts
    """

    # handle faster special case
    if n_grams == 1:
        return _unigram_tokenizer(doc, vocab_dict)

    n_doc = {}

    for d_ind in range(len(doc) + 1 - n_grams):

        # generate cleaned terms conditional on vocab_dict
        t_doc = doc[d_ind:d_ind+n_grams]
        nt_doc = []
        for term in t_doc:
            try:
                term = vocab_dict[term]
                nt_doc.append(term)
            except:
                continue

        # generate the token and add to dict
        if len(nt_doc) == n_grams:
            term = " ".join(nt_doc)
            if term not in n_doc:
                n_doc[term] = 1
            else:
                n_doc[term] += 1

        # add multigrams if requested
        if mult_grams and n_grams > 1:
            for k_grams in range(n_grams - 1):
                for i in range(len(nt_doc) + 1 - k_grams):
                    term = " ".join(nt_doc[i:i+k_grams])
                    if term not in n_doc:
                        n_doc[term] = 1
                    else:
                        n_doc[term] += 1

    return n_doc


def _token_vocab_map(doc, vocab_dict):
    """in cases where the documents or elements of initial iterable
    are already tokens (dicts) apply the vocab_map to each element
    to clean.

    Parameters
    ----------
    doc : dict-like
        dictionary of token counts
    vocab_dict : dict-like
        map from unique token to clean unique token

    Returns
    -------
    cleaned dictionary of tokens (cleaned doc)
    """

    n_doc = {}
    for k in doc:
        try:
            k = vocab_dict[k]
            if k not in n_doc:
                n_doc[k] = doc[k]
            else:
                n_doc[k] += doc[k]
        except:
            continue

    return n_doc


class DTDFBuilder(object):
    """
    class for building DTDF contains all the cleaning related
    information.  An instance of DTDFBuilder can be used
    to clean multiple different document streams.

    Parameters
    ----------
    str_parser : function/method
        method to apply to string to convert to list of terms
    n_grams : scalar
        token word count, e.g. n_grams == 1: unigrams
        n_grams == 2: bigrams
    mult_grams : scalar or bool
        indicator for whether n_grams should be inclusive.
        If True, for n_grams > 1 also generate all lower
        order n_grams, e.g. n_grams == 2: unigrams + bigrams
    stop_words : list or None
        list of words which should be removed from vocab
    regex_stop_words : list or None
        list of regex patterns which should be removed from vocab
    stemmer : function or None
        function applied to pandas Series to generated stemmed
        version of series
    <pre|post>_doc_<lthresh|uthresh> : scalar
        <lower|upper> threshold for <pre|post>-stemming/stop word doc
        count thresholding should be between 0 and 1, is multiplied by
        D to determine count
    <pre|post>_tfidf_thresh : scalar
        tfidf threshold below for <pre|post>-stemming/stop words
        thresholding.
    agg_group : TBA
    """

    def __init__(self, str_parser=_default_parser, n_grams=1, mult_grams=0,
                 stop_words=None, regex_stop_words=None, stemmer=None,
                 pre_doc_lthresh=0., pre_doc_uthresh=1.,
                 pre_tfidf_thresh=0., post_doc_lthresh=0.,
                 post_doc_lthresh=0., post_tfidf_thresh=0.,
                 agg_group=None):

        # TODO Logging
        # TODO Missing vocab_dict
        # TODO Compression for data files
        # TODO post DTDF cleaning (thresholding/agg_group)

        self.str_parser = str_parser
        self.n_grams = n_grams
        self.mult_grams = mult_grams
        self.stop_words = stop_words
        self.regex_stop_words = regex_stop_words
        self.stemmer = stemmer
        self.pre_doc_lthresh=pre_doc_lthresh
        self.pre_doc_uthresh=pre_doc_uthresh
        self.pre_tfidf_thresh=pre_tfidf_thresh
        self.post_doc_lthresh=post_doc_lthresh
        self.post_doc_uthresh=post_doc_uthresh
        self.post_tfidf_thresh=post_tfidf_thresh


    def build_base_DTDF(self, doc_group, data_dir, doc_type="str", mnum=0):
        """
        builds sparse document term matrix from documents.
        Is wrapped by build_DTDF to handle possible parallel
        computing.

        Parameters
        ----------
        doc_group : iterable or tuple
            doc_group either corresponds to an iterable of tuples
            or a tuple of iterables.  The two possibilities
            correspond to different data sources

            1. iterable of tuples
                The index must be assembled along with the documents
                e.g. pulling elements from a database
            2. tuple of iterables
                the index is already assembled
                e.g. load data out of a csv

            In either case, the first element of the tuple corresponds
            to the index and the second the documents
        data_dir : str
            location where DTDF files should be stored
        doc_type : str
            what type is each element in the iterable (doc_group)?
            Supported types:

                - str
                - list
                - dict

        mnum : scalar
            number assigned to current process.  Needed for distributed
            computing.

        Returns
        -------
        None, resulting pandas DataFrame is stored to data_dir
        """

        # load the vocab dict to map clean terms
        vocab_dict_file = os.path.join(data_dir, "vocab_dict.csv"))
        if not os.path.isfile(vocab_dict_file):
            raise FileNotFoundError("vocab_dict.csv")
        vocab_dict = pd.read_csv(vocab_dict_file)
        vocab_dict.index = vocab_dict["term"]
        vocab_dict = vocab_dict["stem"]
        vocab_dict = vocab_dict.to_dict()

        # set up doc_builder based on doc_type
        if doc_type == "str":

            def doc_builder(doc, vocab_dict):
                doc = self.str_parser(doc)
                return _tokenizer(doc, vocab_dict, self.n_grams,
                                  self.mult_grams)

        elif doc_type == "list":

            def doc_builder(doc, vocab_dict):
                return _tokenizer(doc, vocab_dict, self.n_grams,
                                  self.mult_grams)

        elif doc_type == "dict":

            def doc_builder(doc, vocab_dict):
                return _token_vocab_map(doc, vocab_dict)

        else:

            raise ValueError("Unsupported doc_type %s" % doc_type)

        # account for different possible doc_groups
        if type(doc_group) is tuple:
            i = 0
            doc_id = []
            term = []
            count = []
            for doc in doc_group[1]:
                doc = doc_builder(doc, vocab_dict)
                t_term = doc.keys()
                doc_id.extend([i] * len(t_term))
                term.extend(t_term)
                count.extend(doc.values())
                i += 1

            index_df = doc_group[0]
            index_df["doc_id"] = range(i)

        else:
            i = 0
            doc_id = []
            term = []
            count = []
            index_l = []
            for index, doc in doc_group:
                doc = doc_builder(doc, vocab_dict)
                t_term = doc.keys()
                doc_id.extend([i] * len(t_term))
                term.extend(t_term)
                count.extend(doc.values())
                index["doc_id"] = i
                index_l.append(index)
                i += 1

            # build index
            index_df = pd.DataFrame(index_l)

        f_name = os.path.join(data_dir, "tmp_doc_id_%s.csv" % mnum)
        index_df.to_csv(f_name, index=False)

        # build sparse DTDF
        dtm_df = pd.DataFrame({"doc_id": doc_id, "term": term, "count": count})
        f_name = os.path.join(data_dir, "tmp_DTDF_%d.csv" % mnum)
        dtm_df.to_csv(f_name, index=False)


    def build_DTDF(self, doc_group_gen, data_dir, doc_type="str"):
        """
        builds a temporary version of the DTDF for cleaning and
        prior thresholding purposes.

        Parameters
        ----------
        doc_group_gen : generator
            A generate where each element corresponds to a doc_group. This
            is used to handle the distributed computing: in cases where
            doc_group_gen yields more than one element, the computation
            of each element is done in a distributed fashion.
        data_dir : str
            location where DTDF files should be stored
        doc_type : str
            what type is each text document in doc_group?
            Supported types:

                - str
                - list
                - dict

        Returns
        -------
        None, resulting data is stored in data_dir
        """

        def wrapper(tup):
           i, doc_group = tup
           self.build_base_DTDF(doc_group, data_dir, doc_type, i)

        bag = db.from_sequence(doc_group_gen).map(wrapper)
        bag.compute()


    def clean_DTDF(self, data_dir):
        """
        cleans DTDF and produces final files that can be used
        by DTDF class

        Parameters
        ----------
        data_dir : str
            location where DTDF files should be stored.  In order
            for this script to do anything, build_DTDF should
            already have been run on data_dir.

        Returns
        -------
        None, stores a sparse representation of the DTDF
        (triplet counts, doc ids and term ids)
        """

        tmp_doc_files = os.path.join(data_dir, "tmp_doc_id_*.csv")
        doc_df = dd.read_csv(tmp_doc_files)

        tmp_dtm_files = os.path.join(data_dir, "tmp_DTDF_*.csv")
        dtm_df = dd.read_csv(tmp_dtm_files)

        # build term id
        term_id = pd.DataFrame(tmp_dtm_df["term"].unique().compute())
        term_id["term_id"] = term_id.index

        # map term -> term_id in sparse DTDF
        term_id_map = term_id.copy()
        term_id_map.index = term_id["term"]
        term_id_map = term_id_map["term_id"]
        dtm_df["term_id"] = dtm_df["term"].map(term_id_map)
        dtm_df = dtm_df[["doc_id", "term_id", "count"]]

        # get correct doc id
        doc_id_map = doc_df.copy()
        doc_id_map["t_doc_id"] = 1
        doc_id_map["t_doc_id"] = doc_id_map["t_doc_id"].cumsum() - 1
        doc_id_map.index = doc_id_map["doc_id"]
        doc_id_map = doc_id_map["t_doc_id"]
        dtm_df["doc_id"] = dtm_df["doc_id"].map(doc_id_map)
        doc_df["doc_id"] = doc_df["doc_id"].map(doc_id_map)

        # write results
        doc_id = doc_df.compute()
        doc_id.to_csv(os.path.join(data_dir, "doc_id.csv"), index=False)

        dtm_df.to_csv(os.path.join(data_dir, "DTDF_*.csv"), index=False)

        term_id.to_csv(os.path.join(data_dir, "term_id.csv"), index=False)

        # remove tmp files
        tmp_doc_files = glob.glob(tmp_doc_files)
        for f in tmp_doc_files:
            os.remove(f)
        tmp_dtm_files = glob.glob(tmp_dtm_files)
        for f in tmp_dtm_files:
            os.remove(f)


    def build_base_vocab(self, doc_group, data_dir, doc_type="str", mnum=0):
        """
        builds an aggregate version of the vocab for cleaning
        and prior thresholding purposes.  Is wrapped by
        vocab_builder to handle possible parallel computing.

        Parameters
        ----------
        doc_group : iterable or tuple
            doc_group either corresponds to an iterable of tuples
            or a tuple of iterables.  The two possibilities
            correspond to different data sources

            1. iterable of tuples
                The index must be assembled along with the documents
                e.g. pulling elements from a database
            2. tuple of iterables
                the index is already assembled
                e.g. load data out of a csv

            In either case, the first element of the tuple corresponds
            to the index and the second the documents
        data_dir : str
            location where DTDF files should be stored
        doc_type : str
            what type is each element in the iterable (doc_group)?
            Supported types:

                - str
                - list
                - dict

        mnum : scalar
            number assigned to current process.  Needed for distributed
            computing.

        Returns
        -------
        None, resulting pandas DataFrame is stored to data_dir
        """

        # set up doc_builder based on doc_type
        if doc_type == "str":

            def doc_builder(doc, vocab_dict):
                doc = self.str_parser(doc)
                return _pre_tokenizer(doc)

        elif doc_type == "list":

            def doc_builder(doc, vocab_dict):
                return _pre_tokenizer(doc)

        elif doc_type == "dict":

            def doc_builder(doc, vocab_dict):
                return doc

        else:

            raise ValueError("Unsupported doc_type %s" % doc_type)

        # build vocab dictionary from documents, each key
        # corresponds to a term and each value corresponds
        # to a list where the first element in the doc count
        # and the second is the term count
        vocab = {}
        doc_count = 0

        # account for different possible doc_groups
        if type(doc_group) is tuple:
            for doc in doc_group[1]:
                doc = doc_builder(doc, vocab_dict)
                for k in doc:
                    if k not in vocab:
                        vocab[k] = [1, 1]
                    else:
                        vocab[k][0] += 1
                        vocab[k][1] += doc[k]
                doc_count += 1

        else:
            for index, doc in doc_group:
                doc = doc_builder(doc, vocab_dict)
                for k in doc:
                    if k not in vocab:
                        vocab[k] = [1, 1]
                    else:
                        vocab[k][0] += 1
                        vocab[k][1] += doc[k]
                doc_count += 1

        vocab = pd.DataFrame(vocab).T
        vocab.columns = ["doc_term_count", "term_count"]
        vocab["doc_count"] = doc_count
        vocab["term"] = vocab.index
        vocab = vocab[["term", "term_count", "doc_term_count", "doc_count"]]
        f_name = os.path.join(data_dir, "tmp_vocab_%d.csv" % mnum)
        vocab.to_csv(f_name, index=False)


    def build_vocab(self, doc_group_gen, data_dir, doc_type="str"):
        """
        builds an aggregate version of the vocab for cleaning and
        prior thresholding purposes.

        Parameters
        ----------
        doc_group_gen : generator
            A generate where each element corresponds to a doc_group. This
            is used to handle the distributed computing: in cases where
            doc_group_gen yields more than one element, the computation
            of each element is done in a distributed fashion.
        data_dir : str
            location where DTDF files should be stored
        doc_type : str
            what type is each text document in doc_group?
            Supported types:

                - str
                - list
                - dict

        Returns
        -------
        None, resulting data is stored in data_dir
        """

        def wrapper(tup):
           i, doc_group = tup
           self.build_base_vocab(doc_group, data_dir, doc_type, i)

        bag = db.from_sequence(doc_group_gen).map(wrapper)
        bag.compute()


    def clean_vocab(self, data_dir):
        """
        cleans vocab and produces vocab_dict, which maps raw terms to
        clean versions

        Parameters
        ----------
        data_dir : str
            location where DTDF files should be stored.  In order
            for this script to do anything, build_vocab should
            already have been run on data_dir.

        Returns
        -------
        None, stores a cleaned vocab, and vocab_map to data_dir
        """

        # load vocab and gen doc count
        tmp_vocab_files = glob.glob(os.path.join(data_dir, "tmp_vocab_*.csv"))
        vocab_df_l = []
        D = 0
        for f in tmp_vocab_files:
            df = pd.read_csv(f)
            D += df["doc_count"].unique()[0]
            vocab_df_l.append(df)
        vocab = pd.concat(vocab_df_l, ignore_index=True)

        vocab = vocab[["term", "doc_term_count", "term_count"]]
        vocab = vocab.groupby("term", as_index=False).sum()

        # doc thresholding
        if self.pre_doc_lthresh > 0:
            thresh = D * self.pre_doc_lthresh
            vocab = vocab[vocab["doc_term_count"] >= thresh]
        if self.pre_doc_uthresh < 1:
            thresh = D * self.pre_doc_uthresh
            vocab = vocab[vocab["doc_term_count"] < thresh]

        # tfidf thresholding
        if self.pre_tfidf_thresh > 0:
            thresh = self.pre_tfidf_thresh
            vocab["tfidf"] = vocab["term_count"] / vocab["term_count"].sum()
            vocab["tfidf"] *= np.log(D / (vocab["doc_term_count"] + 1.))
            vocab = vocab[vocab["tfidf"] > thresh]

        # removing stop words
        if self.stop_words is not None:
            vocab = vocab[~vocab["term"].sin(self.stop_words)]

        # remove regex stop words
        if self.regex_stop_words is not None:
            for term in list(self.regex_stop_words):
                vocab = vocab[~vocab["term"].str.contains(term, na=False)]

        # apply stemming
        if self.stemmer is not None:
            vocab["stem"] = self.stemmer(vocab["term"])
        else:
            vocab["stem"] = vocab["term"]

        # add doc count to vocab
        vocab["doc_count"] = D

        vocab.to_csv(os.path.join(data_dir, "vocab.csv"), index=False)
        vocab = vocab["stem"]
        vocab.to_csv(os.path.join(data_dir, "vocab_dict.csv"))

        # remove tmp files
        for f in tmp_vocab_files:
            os.remove(f)
