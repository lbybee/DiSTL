def vocab_cleaner(vocab, stop_words=None, regex_stop_words=None,
                  pre_doc_lthresh=None, pre_doc_uthresh=None,
                  pre_tfidf_thresh=None, stemmer=None,
                  pre_term_length=None, log=True):
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
        <lower|upper> threshold for stemming/stop word doc
        count thresholding should be between 0 and 1, is multiplied by
        D to determine count
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

    return vocab
