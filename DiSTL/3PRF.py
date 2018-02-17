from dask import delayed
import dask.dataframe as dd
import dask.array as da
import pandas as pd
import numpy as np
import sparse
import os


def load_dtm(data_dir):
    """
    loads builds dask array corresponding to DTM, returns the array along with
    the term_id and doc_id as a tuple

    Parameters
    ----------
    data_dir : str
        location where DTM files are stored

    Returns
    -------
    tuple of DTM array, doc_id, term_id
    """

    dtm_df = dd.read_csv(os.path.join(data_dir, "DTDF_*.csv"), blocksize=None)
    doc_id = dd.read_csv(os.path.join(data_dir, "doc_id_*.csv"))
    term_id = pd.read_csv(os.path.join(data_dir, "term_id.csv"))

    P = term_id.shape[0]
    D = len(doc_id)

    def _dtm_maker(dtm_i):
        """function for converting each DataFrame of coordinates
        to sparse array to load into dask array
        """

        tD = dtm_i["doc_id"].max() - dtm_i["doc_id"].min()
        coords = (dtm_i["doc_id"], dtm_i["term_id"])
        data = dtm_i["count"]
        return(coords, data, (tD, P))

    delayed_dtm_df = dtm_df.to_delayed()
    delayed_doc_id = doc_id.to_delayed()
    del_l = [da.from_delayed(delayed(sparse.COO)(*_dtm_maker(dtm_i)),
                                    (dtm_i["doc_id"].max().compute()
                                     - dtm_i["doc_id"].min().compute() + 1, P),
                             float)
             for dtm_i in delayed_dtm_df]
    dtm = da.concatenate(del_l, axis=0)
    return dtm, doc_id, term_id


def load_vol(rv_file):
    """
    loads volatility data from corresponding rv_file

    Parameters
    ----------
    rv_file : str
        location where vol stored

    Returns
    -------
    data frame corresponding to volatility
    """

    rv_df = pd.read_csv(rv_file)
    rv_df = rv_df[["date", "SPX2.rv"]]
    rv_df = rv_df.dropna()
    rv_df["date"] = pd.to_datetime(rv_df["date"], format="%Y-%m-%d")
    return rv_df


def intersect_data(dtm, doc_id, rv_df):
    """takes the intersection of observations based on date

    Parameters
    ----------
    dtm : dask-array
        sparse dask array corresponding to DTM
    doc_id : dask-dataframe
        data frame linking doc_id to date
    rv_df : pandas DataFrame
        DataFrame containing date and volatility

    Returns
    -------
    tuple containing updated version of each param
    """

    # the conversion to datetime is done this way to only
    # keep the date component (while still being a datetime)
    dtm_date = pd.to_datetime(doc_id["display-date"].compute(), format="%Y-%m-%d")
    dtm_date = pd.to_datetime(dtm_date.dt.date)
    dtm_ind = dtm_date.isin(rv_df["date"])
    rv_ind = rv_df["date"].isin(dtm_date)

    dtm = dtm[np.where(dtm_ind)[0],:]
    rv_df = rv_df[rv_ind]

    # we have to do the doc_id seperate because dask doesn't have
    # everything we need yet
    delayed_doc_id = doc_id.to_delayed()

    def _doc_id_maker(doc_id_i):

        doc_id_i["display-date"] = pd.to_datetime(doc_id_i["display-date"],
                                                  format="%Y-%m-%d")
        doc_id_i["display-date"] = doc_id_i["display-date"].dt.date
        doc_id_i["display-date"] = pd.to_datetime(doc_id_i["display-date"])
        doc_id_i = doc_id_i[doc_id_i["display-date"].isin(rv_df["date"])]
        return doc_id_i

    del_l = [delayed(_doc_id_maker)(doc_id_i) for doc_id_i in delayed_doc_id]
    doc_id = dd.from_delayed(del_l)
    doc_id_map = doc_id[["doc_id"]].compute()
    doc_id_map["new_doc_id"] = 1
    doc_id_map["new_doc_id"] = doc_id_map["new_doc_id"].cumsum() - 1
    doc_id_map.index = doc_id_map["doc_id"]
    doc_id_map = doc_id_map["new_doc_id"]
    doc_id["doc_id"] = doc_id["doc_id"].map(doc_id_map)

    return dtm, doc_id, rv_df


def gen_vol_df(doc_id, rv_df):
    """generates a dask dataframe which contains the volatility and
    aligns with the DTM

    Parameters
    ----------
    doc_id : dask dataframe
        corresponds to date doc_id link
    rv_df : pandas dataframe
        contains data and volatility

    Returns
    -------
    dask dataframe containing date and vol
    """

    rv_map = rv_df.copy()
    rv_map.index = rv_map["date"]
    rv_map = rv_map["SPX2.rv"]
    vol_ddf = doc_id.copy()
    vol_ddf["vol"] = vol_ddf["display-date"].map(rv_map)
    return vol_ddf


def column_3PRF(dtm, proxy):
    """runs the logit 3PRF on each column

    Parameters
    ----------
    dtm : dask array
        corresponds to document term matrix

    """

    return None

project_dir = "/mnt/dropbox/Dropbox/BK_LB_Projects"
dtm_dir = os.path.join(project_dir, "topic_model_experiments/DJWSJ_multigrams_articles_filterAll_v9/DTM")
rv_file = os.path.join(project_dir, "Forecasting/intermediate/OxfordRV/RV.csv")

dtm, doc_id, term_id = load_dtm(dtm_dir)
rv = load_vol(rv_file)
dtm, doc_id, rv = intersect_data(dtm, doc_id, rv)
vol_ddf = gen_vol_df(doc_id, rv)
