"""
Includes the core code used by DiSTL, this includes:

1. DTDF class

Document Term Data-Frame (DTDF), the basic class for working with text
data in DiSTL.  Build on top of an ndarray-like.

2. loadDTDF

Function for building instance of DTDF from source files.
"""

from dask import delayed
from dataframe import BDF
import dask.dataframe as dd
import dask.array as da
import pandas as pd
import numpy as np
import sparse
import dask
import os


# ------------- #
# 1. DTDF Class #
# ------------- #

class DTDF(BDF):
    """
    class for document term data-frame

    Parameters
    ----------
    See BDF

    Attributes
    ----------
    See BDF
    """

    def __init__(self, *args, **kwds):

        BDF.__init__(self, *args, **kwds)


    def idf(self):
        """generates the inverse document-frequency for the DTDF

        Returns
        -------
        numpy array of idf, log(D / (D_v + 1))
        """

        D, V = self.dtm.shape
        D_v = (self.dtm != 0).sum(axis=0)
        idf = np.log(D / (D_v + 1.))
        idf = np.squeeze(np.asarray(idf))
        return idf


    def tfidf(self):
        """returns a term-frequency, inverse document-frequency (TF-IDF)
        version of the DTDF

        Returns
        -------
        TF-IDF version of DTDF
        """

        idf = self.idf()
        return self.div(self.data.sum(axis=1), axis=0).mul(idf)


    def find_freq_terms(self, count):
        """returns a list of the <count> most frequent terms

        Parameters
        ----------
        count : scalar
            number of terms to return

        Returns
        -------
        array of terms
        """

        group = self.sum()
        group = group.sort_values()
        return group[:count]


    def find_assoc(self, term, threshold):
        """finds the terms most associated with the specified
        term, those terms whose correlation is above the given
        threshold

        term : str
            term to find association with
        threshold : scalar
            level above which to keep terms

        Returns
        -------
        series of terms and associations
        """

        if term in self.columns:
            corr = self.corr().compute()
            t_corr = corr[term]
            return t_corr[t_corr > threshold]
        else:
            raise ValueError("%s not in columns")


    def word_cloud(self):
        """saves a word cloud to the corresponding f_name

        Parameters
        ----------
        f_name : str
            location of file

        Returns
        -------
        None
        """

        from wordcloud import WordCloud, STOPWORDS
        import matplotlib.pyplot as plt

        group = self.sum()
        group = group.to_dict()
        wordcloud = WordCloud(stopwords=STOPWORDS, background_color="white",
                              width=800, height=500).fit_words(group)
        wordcloud.to_file(f_name)


    def similarity(self, freq, method="cosine"):
        """This method generates a vector of similarities for each document
        using the desired method

        Paramters
        ---------
        freq : array-like
            array of values for comparison vocab.  Can be multidimensional
            in the leastsq case
        method : str
            similarity method
            1. cosine
            2. leastsq

        Returns
        -------
        array of similarities
        """

        # define method
        if method == "cosine":

            freq = freq / np.linalg.norm(freq)

            def sim_method(row):
                row /= np.linalg.norm(row)
                row *= freq
                row[row.isnull()] = 0
                return row.sum()

        elif method == "leastsq":

            import statsmodels.api as sm
            freq = pd.DataFrame(freq)
            cols = self.columns
            freq = freq.reindex(cols)

            def sim_method(row):
                mod = sm.OLS(row, freq, missing=drop)
                return mod.fit().params

        else:
            raise ValueError("Unknown method %s" % method)

        return self.apply(sim_method)


    def to_csv(self, data_dir):
        """stores the DTDF into the corresponding data_dir

        Parameters
        ----------
        data_dir : str
            location where results are stored

        Returns
        -------
        None
        """

        # term id
        term_id = self.metadata[1]
        term_id.to_csv(os.path.join(data_dir, "term_id.csv"), index=False)

        # doc id
        doc_id = self.metadata[0]

        # DTM
        dtm = self.data["DTM"]

        # write doc id and dtm, if the data is a dask array we write
        # one doc_id and DTDF file for each chunk/partition
        if len(self.chunks[0]) > 1:

            delayed_doc = doc_id.to_delayed()
            delayed_dtm = dtm.to_delayed().flatten()

            def writer(dtm_i, doc_i, i):
                doc_i.to_csv(os.path.join(data_dir, "doc_id_%d.csv" % i),
                             index=False)
                dtm_df = pd.DataFrame({"doc_id": dtm_i.coords[0],
                                       "term_id": dtm_i.coords[1],
                                       "count": dtm_i.data})
                dtm_df.to_csv(os.path.join(data_dir, "DTDF_%d.csv" % i),
                              index=False)

            del_l = [delayed(writer)(dtm_i, doc_i, i) for dtm_i, doc_i, i
                     in zip(delayed_dtm, delayed_doc,
                            range(len(delayed_dtm)))]
            dask.compute(*del_l)

        else:
            doc_id.to_csv(os.path.join(data_dir, "doc_id_0.csv"), index=False)
            dtm_df = pd.DataFrame({"doc_id": dtm.coords[0],
                                   "term_id": dtm.coords[1],
                                   "count": dtm.data})
            dtm_df.to_csv(os.path.join(data_dir, "DTDF_0.csv"), index=False)


# ---------------- #
# 3. loadDTDF Code #
# ---------------- #

def loadDTDF(data_dir, distributed=0):
    """builds an instance of a DTDF

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
    doc_id = dd.read_csv(os.path.join(data_dir, "doc_id_*.csv"))
    term_id = pd.read_csv(os.path.join(data_dir, "term_id.csv"))
#    D = len(doc_id)
    P = term_id.shape[0]

    delayed_dtm_df = dtm_df.to_delayed()
    delayed_doc_id = doc_id.to_delayed()

    def dtm_maker(dtm_i, doc_i):

        D = doc_i.shape[0]
        coords = (dtm_i["doc_id"], dtm_i["term_id"])
        data = dtm_i["count"]
        return(coords, data, (D, P))

    del_l = [da.from_delayed(delayed(sparse.COO)(*dtm_maker(dtm_i, doc_i)),
                             (doc_i.shape[0].compute(), P), int)
             for dtm_i, doc_i in zip(delayed_dtm_df, delayed_doc_id)]
    dtm = da.concatenate(del_l, axis=0)

    # if not distributed we drop out of dask
    if not distributed:
        dtm = dtm.compute()
        doc_id = doc_id.compute()
        chunks = tuple((s,) for s in dtm.shape)
    else:
        chunks = dtm.chunks
    data = {"DTM": dtm}
    axes = {"DTM": (0, 1)}
    metadata = [doc_id, term_id]
    return DTDF(data, axes, metadata=metadata, chunks=chunks)
