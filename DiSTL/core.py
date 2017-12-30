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

    def __init__(self, *args, **kwds):
        pd.SparseDataFrame.__init__(self, *args, **kwds)


    def idf(self):
        """generates the inverse document-frequency for the DTDF

        Returns
        -------
        numpy array of idf, log(D / (D_v + 1))
        """

        D, V = self.shape
        D_v = (self != 0).sum(axis=0)
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
        return self.div(self.sum(axis=1), axis=0).mul(idf)


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

            def sim_method(row):
                mod = sm.OLS(row, freq)
                return mod.fit().params

        else:
            raise ValueError("Unknown method %s" % method)

        return self.apply(sim_method)


    def to_csv(self, data_dir):
        """stores the DTDF into the corresponding data_dir, since
        this is not a parallel instance (a DDTDF), we store one
        file for the doc_id and dtm

        Parameters
        ----------
        data_dir : str
            location where results are stored

        Returns
        -------
        None
        """

        # term id
        terms = self.columns
        term_id = pd.DataFrame({"term": terms, "term_id": range(len(terms))})
        term_id.to_csv(os.path.join(data_dir, "term_id.csv"))

        # doc id
        docs = self.index
        doc_id = pd.DataFrame(docs)
        doc_id["doc_id"] = range(len(doc_id))
        doc_id.to_csv(os.path.join(data_dir, "doc_id_0.csv"))

        # DTDF sparse counts
        mat = self.to_coo()
        dtm_df = pd.DataFrame({"doc_id": mat.row, "term_id": mat.col,
                               "count": mat.data})
        dtm_df.to_csv(os.path.join(data_dir, "DTDF_0.csv"))


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
        """generates the inverse document-frequency for the DDTDF

        Returns
        -------
        numpy array of idf, log(D / (D_v + 1))
        """

        D = len(self)
        V = len(self.columns)
        D_v = (self != 0).sum(axis=0)
        idf = np.log(D / (D_v + 1.))
        idf = np.squeeze(np.asarray(idf))
        return idf


    def tfidf(self):
        """returns a term-frequency inverse document-frequency (TF-IDF)
        version of the DDTDF

        Returns
        -------
        TF-IDF version of DDTDF
        """

        idf = self.idf()
        return self.div(self.sum(axis=1), axis=0).mul(idf)


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


    def word_cloud(self, f_name):
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

        # prep freq

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

            def sim_method(row):
                mod = sm.OLS(row, freq)
                return mod.fit().params

        else:
            raise ValueError("Unknown method %s" % method)

        return self.apply(sim_method)


    def to_csv(self, data_dir):
        """stores the DDTDF into the corresponding data_dir, since
        this is a parallel instance (a DDTDF not DTDF), we store
        multiple files for the doc_id and dtm, one for each partition

        Parameters
        ----------
        data_dir : str
            location where results are stored

        Returns
        -------
        None
        """

        # TODO fix doc_id

        # term id
        terms = self.columns
        term_id = pd.DataFrame({"term": terms, "term_id": range(len(terms))})
        term_id.to_csv(os.path.join(data_dir, "term_id.csv"))

        # prep doc id
        docs = self.index
        docs_delayed = docs.to_delayed()

        def index_to_df(doc_i):
            doc_i = pd.DataFrame(doc_i)
            doc_i["doc_id"] = 1
            return doc_i

        del_l = [delayed(index_to_df)(doc_i) for doc_i in docs_delayed]
        doc_id = dd.from_delayed(del_l)
        doc_ind = doc_id["doc_id"].cumsum() - 1
        doc_id = doc_id.drop("doc_id", axis=1)

        # write doc id and dtm
        dtm_delayed = self.to_delayed()
        doc_id_delayed = doc_id.to_delayed()
        doc_ind_delayed = doc_ind.to_delayed()

        def writer(dtm_i, doc_i, doc_ind_i, i):

            # write doc id
            doc_i["doc_id"] = doc_ind_i
            doc_i.to_csv(os.path.join(data_dir, "doc_id_%d.csv" % i))

            # write dtm
            mat = self.to_coo()
            dtm_df = pd.DataFrame({"doc_id": mat.row, "term_id": mat.col,
                                   "count": mat.data})
            dtm_df.to_csv(os.path.join(data_dir, "DTDF_%d.csv" % i))

        # compute writers
        del_l = [delayed(writer)(dtm_i, doc_i, doc_ind_i, i)
                 for dtm_i, doc_i, doc_ind_i, i in
                 zip(dtm_delayed, doc_id_delayed, doc_ind_delayed,
                     range(len(dtm_delayed)))]
        dask.compute(*del_l)


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
#    D = len(doc_id)
    P = term_id.shape[0]

    def dtm_maker(dtm_i, doc_i):

        D = doc_i.shape[0]
        doc_i = doc_id.drop("doc_id", axis=1)
        sparse_mat = ss.coo_matrix((dtm_i["count"],
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
