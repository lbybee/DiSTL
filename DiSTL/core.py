"""
Includes the core code used by DiSTL, this includes:

1. DTDF class

Document Term Data-Frame (DTDF), the basic class for working with text
data in DiSTL.  Build on top of a pandas sparse Data-Frame.

2. DDTDF class

Distributed Document Term Data-Frame (DDTDF), compaarable to DTDF
but built on top of dask dataframe.

3. loadDTDF

Function for building instance of DTDF or DDTDF from source files.
"""

from dask import delayed
import dask.dataframe as dd
import pandas as pd
import numpy as np
import dask
import os


# ------------- #
# 1. DTDF Class #
# ------------- #

class DTDF(pd.SparseDataFrame):
    """
    class for document term data-frame

    Parameters
    ----------
    kwds : dict-like
        key words for building SparseDataFrame
    """

    def __init__(self, **kwds):
        super(DTDF, self).__init__(**kwds)


    def idf(self):

        return None


    def tfidf(self):

        return None


    def find_freq_terms(self, count):

        return None


    def find_assoc(self, threshold):

        return None


    def word_cloud(self):

        return None


    def similarity(self, freq):

        return None


    def least_sq(self, freq):

        return None


    def to_csv(self, data_dir):

        return None


# -------------- #
# 2. DDTDF Class #
# -------------- #

class DDTDF(dd.DataFrame):
    """
    class for distributed document term data-frame

    Parameters
    ----------
    df : dask DataFrame
        converts df to DDTDF
    """

    def __init__(self, df):
        super(DDTDF, self).__init__(df.dask, df._name, df._meta, df.divisions)


    def idf(self):

        return None


    def tfidf(self):

        return None


    def find_freq_terms(self, count):

        return None


    def find_assoc(self, threshold):

        return None


    def word_cloud(self):

        return None


    def similarity(self, freq):

        return None


    def least_sq(self, freq):

        return None


    def to_csv(self, data_dir):

        return None


# ---------------- #
# 3. loadDTDF Code #
# ---------------- #

def loadDTDF(data_dir, distributed=0):
    """builds an instance of a DTDF or DDTDF depending on
    distributed

    Parameters
    ----------
    data_dir : str
        location where files are stored
    distributed : scalar or bool
        indicator for whether to gen DTDF instance or
        DDTDF instance.

    Returns
    -------
    instance of DTDF or DDTDF depending on distributed
    choice
    """

    # load sparse representation
    dtm_df = dd.read_csv(os.path.join(data_dir, "DTDF_*.csv"))
    dtm_delayed = dtm_df.to_delayed()
    doc_id = dd.read_csv(os.path.join(data_dir, "doc_id_*.csv"))
    doc_delayed = doc_id.to_delayed()
    term_id = pd.read_csv(os.path.join(data_dir, "term_id.csv"))
    P = term_id.shape[0]

    def dtm_maker(dtm_i, doc_i):

        D = doc_i.shape[0]
        doc_i = doc_id.drop("doc_id", axis=1)
        sparse_mat = ss.csr_matrix((dtm_i["count"],
                                    (dtm_i["doc_id"],
                                     dtm_i["term_id"])), (D, P))
        sparse_df = DTDF(sparse_mat)
        sparse_df.columns = term_id["term"]
        sparse_df.index = doc_i
        return sparse_df

    del_l = [delayed(dtm_maker)(dtm_i, doc_i)
             for dtm_i, doc_i in zip(dtm_delayed, doc_delayed)]
    dtm = dd.from_delayed(del_l)
    if distributed:
        return DDTDF(dtm)
    else:
        return dtm.compute()
