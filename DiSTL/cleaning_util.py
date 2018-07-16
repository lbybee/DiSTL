import re


def vocab_cleaner(vocab, stop_words=None, regex_stop_words=None,
                  D=None, doc_lthresh=None, doc_uthresh=None,
                  tfidf_thresh=None, stemmer=None,
                  term_length=None):
    """
    function for applying cleaning filters to a vocab at an arbitrary step
    in the cleaning process.

    This method is imported by both build_tokens and build_DTM for their
    respective cleaning steps.

    Parameters
    ----------
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
    cleaned vocab
    """

    # removing stop words
    if stop_words:
        vocab = vocab[~vocab["raw_term"].isin(stop_words)]

    # remove regex stop words
    if regex_stop_words:
        for term in list(regex_stop_words):
            vocab = vocab[~vocab["raw_term"].str.contains(term, na=False)]

    # apply count thresholding
    if doc_lthresh:
        if D:
            thresh = D * doc_lthresh
        else:
            thresh = doc_lthresh
        vocab = vocab[vocab["count"] >= thresh]
    if doc_uthresh:
        if D:
            thresh = D * doc_uthresh
        else:
            thresh = doc_uthresh
        vocab = vocab[vocab["count"] <= thresh]


    # apply tfidf thresholding
    if tfidf_thresh:
        raise ValueError("Tf-Idf thresholding is currently not supported")

    # apply stemming
    if stemmer:
        vocab["term"] = stemmer(vocab[["raw_term"]])
    else:
        vocab["term"] = vocab["raw_term"]

    # remove terms below term length
    if term_length:
        fn = lambda x: len(x) >= term_length
        vocab = vocab[vocab["term"].apply(fn)]

    return vocab


def _gen_stem_map(vocab, stem):
    """helper function used by default_lemmatizer"""

    stem_map = vocab.copy()
    stem_map = stem_map.apply(stem)
    ind = stem_map.duplicated(keep=False)
    stem_map.loc[~ind] = vocab.loc[~ind]
    stem_map.index = vocab
    stem_map = stem_map.to_dict()

    return stem_map


def default_lemmatizer(unstemmed):
    """takes a pandas data frame with just a term var and generates
    stems (lemmas).  Note this is a sequential lemmatisation procedure

    Params
    ------
    unstemmed : Pandas Data Frame

    Returns
    -------
    Pandas series
    """

    def es0_stemmer(x):
        if x[-4:] == "sses":
            x = x[:-2]
        return x

    def es1_stemmer(x):
        if x[-3:] == "ies":
            x = x[:-3] + "y"
        return x

    def s_stemmer(x):

        if x[-1] == "s" and x[-2:] != "ss":
            x = x[:-1]
        return x

    def ly_stemmer(x):

        if x[-2:] == "ly":
            x = x[:-2]
        return x

    def ed0_stemmer(x):

        if x[-2:] == "ed":
            x = x[:-2]
        return x

    def ed1_stemmer(x):

        if x[-2:] == "ed":
            x = x[:-1]
        return x

    def ing0_stemmer(x):

        if x[-3:] == "ing":
            x = x[:-3] + "e"
        return x

    def ing1_stemmer(x):

        if re.search("([^aeiouy])\\1ing$", x):
            x = x[:-4]
        return x

    def ing2_stemmer(x):

        if x[-3:] == "ing":
            x = x[:-3]
        return x

    vocab = unstemmed.copy()
    for stem in [es0_stemmer, es1_stemmer, s_stemmer, ly_stemmer, ed0_stemmer,
                 ed1_stemmer, ing0_stemmer, ing1_stemmer, ing2_stemmer]:
        stem_map = _gen_stem_map(vocab["raw_term"].drop_duplicates(), stem)
        vocab["raw_term"] = vocab["raw_term"].map(stem_map)
    return vocab["raw_term"]
