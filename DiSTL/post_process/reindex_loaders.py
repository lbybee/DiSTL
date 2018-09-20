"""
loader methods for various standard data-types that will be used by
the reindexers
"""
from datetime import datetime
import pandas as pd
import os


# --------------- #
# term_id Loaders #
# --------------- #


# -------------- #
# doc_id Loaders #
# -------------- #

def load_article_doc_id(source_data_dir, doc_part):

    doc_id = pd.read_csv(os.path.join(source_data_dir,
                                      "doc_id_%s.csv" % doc_part),
                         names=["display_date", "accession_number", "doc_id"])
    doc_id["display_date"] = pd.to_datetime(doc_id["display_date"])

    return doc_id

def load_month_doc_id(source_data_dir, doc_part):

    doc_id = pd.read_csv(os.path.join(source_data_dir,
                                      "doc_id_%s.csv" % doc_part),
                         names=["year", "month", "doc_id"])
    return doc_id

def load_day_doc_id(source_data_dir, doc_part):

    doc_id = pd.read_csv(os.path.join(source_data_dir,
                                      "doc_id_%s.csv" % doc_part),
                         names=["date", "doc_id"])
    doc_id["date"] = pd.to_datetime(doc_id["date"])

    return doc_id

def load_ticker_month_doc_id(source_data_dir, doc_part):

    doc_id = pd.read_csv(os.path.join(source_data_dir,
                                      "doc_id_%s.csv" % doc_part),
                         names=["ticker", "year", "month", "doc_id"])
    return doc_id

def load_ticker_day_doc_id(source_data_dir, doc_part):

    doc_id = pd.read_csv(os.path.join(source_data_dir,
                                      "doc_id_%s.csv" % doc_part),
                         names=["ticker", "date", "doc_id"])
    doc_id["date"] = pd.to_datetime(doc_id["date"])

    return doc_id


# --------------------- #
# term metadata Loaders #
# --------------------- #


# ------------------------- #
# document metadata Loaders #
# ------------------------- #

def load_permno_metadata(metadata_f, doc_partitions):
    """loads metadata that really just contains permno-ticker maps, this
    is used for the basic case where we don't want any additional variables
    but want to map the tickers from the DB to permnos
    """

    metadata_df = pd.read_csv(metadata_f, sep="\t")
    metadata_df.columns = ["permno", "date", "ticker", "permco", "ret"]
    metadata_df["date"] = pd.to_datetime(metadata_df["date"].astype(str))
    metadata_df["year"] = metadata_df["date"].dt.year
    metadata_df["month"] = metadata_df["date"].dt.month
    metadata_df = metadata_df[~metadata_df.duplicated(["ticker", "date"])]
    metadata_df = metadata_df.drop(["date"], axis=1)

    dt_dates = [datetime.strptime(d, "%Y_%m") for d in doc_partitions]
    metadata_df_partitions = [metadata_df[((metadata_df["year"] == d.year) &
                                           (metadata_df["month"] == d.month))]
                              for d in dt_dates]

    return metadata_df_partitions

def load_crsp_ret_metadata(metadata_f, doc_partitions, ticker_l=None,
                           permno_l=None):
    """loads return data from crsp as well as any other additional variables
    which we might want to link with documents

    Additionally, can constrain the resulting metadata based on an
    externally provided list of tickers or permnos
    """

    metadata_df = pd.read_csv(metadata_f)
    metadata_df = metadata_df.rename(columns={"TICKER": "ticker"})

    if ticker_l:
        metadata_df = metadata_df[metadata_df["ticker"].isin(ticker_l)]
    if permno_l:
        metadata_df = metadata_df[metadata_df["PERMNO"].isin(permno_l)]

    # NaN rows which contain string returns/SIC codes (these correspond
    # to bad data from WRDS
    metadata_df["RET"] = pd.to_numeric(metadata_df["RET"], errors="coerce")
    metadata_df["RETX"] = pd.to_numeric(metadata_df["RETX"], errors="coerce")
    metadata_df["SICCD"] = pd.to_numeric(metadata_df["SICCD"],
                                         errors="coerce")

    # handle dates
    metadata_df["date"] = pd.to_datetime(metadata_df["date"].astype(str))
    metadata_df["year"] = metadata_df["date"].dt.year
    metadata_df["month"] = metadata_df["date"].dt.month
    metadata_df = metadata_df[~metadata_df.duplicated(["ticker", "date"])]

    # drop missing data
    metadata_df = metadata_df.dropna(subset=["RET", "SICCD"])

    # partition metadata by year-months
    dt_dates = [datetime.strptime(d, "%Y_%m") for d in doc_partitions]
    metadata_df_partitions = [metadata_df[((metadata_df["year"] == d.year) &
                                           (metadata_df["month"] == d.month))]
                              for d in dt_dates]

    return metadata_df_partitions
