"""
test columns :
    1. unigrams
    2. bigrams
    3. multigrams
    4. pre-threshold bottom art 0.1
    5. pre-threshold top art 0.5
    6. stop-words (the, a)
    7. regex stop words (he)
    8. parallel
    9. list input
    10. dict input

    : 1 : 2 : 3 : 4 : 5 : 6 : 7 : 8 : 9 : 10 :
T1  : X :   :   :   :   :   :   :   :   : X  :
T2  : X :   :   :   :   :   :   :   : X :    :
T3  : X :   :   : X :   :   :   :   :   : X  :
T4  : X :   :   :   : X :   :   :   :   : X  :
T5  : X :   :   :   :   : X :   :   :   : X  :
T6  : X :   :   :   :   :   : X :   :   : X  :
T7  : X :   :   :   :   :   :   : X :   : X  :
T8  :   : X :   :   :   :   :   :   : X :    :
T9  :   : X :   : X :   :   :   :   : X :    :
T10 :   : X :   :   :   :   :   : X : X :    :
"""
# add path to code
import sys
sys.path.append("../")

# imports
from numpy.testing import assert_equal, assert_array_equal
from build import make_DTDF
import dask.dataframe as dd
import dask.bag as db
import pandas as pd
import numpy as np
import json
import os


def load_data(data_dir):
    """loads the corresponding test DTM for testing

    Parameters
    ----------
    data_dir : str
        location of DTM files

    Returns
    -------
    tuple of term_id, doc_id and sparse corpus
    """

    term_id = pd.read_csv(os.path.join(data_dir, "term_id.csv"))
    doc_id = pd.read_csv(os.path.join(data_dir, "doc_id.csv"))
    files = os.listdir(data_dir)
    c_files = [f for f in files if "corpus" in f]
    d_files = [f for f in files if "DTDF" in f]
    if len(c_files) > 0:
        corpus = dd.read_csv(os.path.join(data_dir, "corpus_*.csv"),
                             header=None,
                             names=["doc_id", "term_id", "count"]).compute()
        term_id.columns = ["term", "term_id"]
    else:
        corpus = dd.read_csv(os.path.join(data_dir, "DTDF_*.csv")).compute()

    return term_id, doc_id, corpus


def test_df_comp(tup_old, tup_new):
    """compares the doc_id, term_id and sparse counts for each
    method of DTM generation, tup_old : archive/old method
    tup_new : new/DiSTL method

    Parameters
    ----------
    tup_old : tuple
        contains doc_id, term_id, and corpus, corresponds to the
        old/oldive method
    tup_new : tuple
        contains doc_id, term_id, and corpus, corresponds to the
        new/DiSTL method

    Returns
    -------
    None, runs tests
    """

    # unload values
    term_id_old, doc_id_old, corpus_old = tup_old
    term_id_new, doc_id_new, corpus_new = tup_new


    # compare term_ids

    # confirm same columns
    assert_array_equal(term_id_new.columns, term_id_old.columns)

    # confirm same terms
    term_id_new = term_id_new.sort_values("term")
    term_id_old = term_id_old.sort_values("term")
    assert_array_equal(term_id_new["term"].values, term_id_old["term"].values)


    # compare doc_ids

    # confirm same columns
    assert_array_equal(doc_id_new.columns, doc_id_old.columns)

    # confirm same index
    doc_id_new = doc_id_new.sort_index()
    doc_id_old = doc_id_old.sort_index()
    assert_array_equal(doc_id_new.index.values, doc_id_old.index.values)


    # compare counts

    # compare doc counts
    # TODO generalize to multiple indices
    doc_id_new_map = doc_id_new.copy()
    doc_id_new_map.index = doc_id_new_map["doc_id"]
    doc_id_new_map = doc_id_new_map["display-date"]
    corpus_new["date"] = corpus_new["doc_id"].map(doc_id_new_map)
    doc_id_old_map = doc_id_old.copy()
    doc_id_old_map.index = doc_id_old_map["doc_id"]
    doc_id_old_map = doc_id_old_map["display-date"]
    corpus_old["date"] = corpus_old["doc_id"].map(doc_id_old_map)
    doc_count_new = corpus_new.groupby("date")["count"].sum()
    doc_count_new = doc_count_new.sort_index()
    doc_count_old = corpus_old.groupby("date")["count"].sum()
    doc_count_old = doc_count_old.sort_index()
    assert_array_equal(doc_count_new.values, doc_count_old.values)

    # compare term counts
    term_id_new_map = term_id_new.copy()
    term_id_new_map.index = term_id_new_map["term_id"]
    term_id_new_map = term_id_new_map["term"]
    corpus_new["term"] = corpus_new["term_id"].map(term_id_new_map)
    term_id_old_map = term_id_old.copy()
    term_id_old_map.index = term_id_old_map["term_id"]
    term_id_old_map = term_id_old_map["term"]
    corpus_old["term"] = corpus_old["term_id"].map(term_id_old_map)
    term_count_new = corpus_new.groupby("term")["count"].sum()
    term_count_new = term_count_new.sort_index()
    term_count_old = corpus_old.groupby("term")["count"].sum()
    term_count_old = term_count_old.sort_index()
    assert_array_equal(term_count_new.values, term_count_old.values)


def build_test_wrapper(test_dir, **kwds):
    """wraps all the code needed to build a new DTM, compare with the
    old DTM and clean up after it is done

    Parameters
    ----------
    test_dir : str
        location where old DTM and new DTM config are stored

    Returns
    -------
    None
    """

    new_dir = os.path.join(test_dir, "DTDF")
    old_dir = os.path.join(test_dir, "arch")

    # load config
    with open(os.path.join(test_dir, "DTDF_config.json"), "r") as ifile:
        DTDF_config = json.load(ifile)
    config = DTDF_config["config"]
    DTDFBuilder_kwds = DTDF_config["DTDFBuilder_kwds"]
    doc_type = DTDF_config["doc_type"]

    make_DTDF(config, new_dir, DTDFBuilder_kwds, source_type="mongodb",
              doc_type=doc_type)

    # load tuples
    tup_new = load_data(new_dir)
    tup_old = load_data(old_dir)

    test_df_comp(tup_old, tup_new)

    files = os.listdir(new_dir)
    for f in files:
        if f != "config.json":
            os.remove(os.path.join(new_dir, f))
