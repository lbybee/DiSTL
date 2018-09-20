"""
methods for generating updates for doc_ids
"""
import dask.dataframe as dd
import pandas as pd
import numpy as np
import multiprocessing
import os


def doc_cosine_updater(doc_id, counts, merge_columns_l, base_idf,
                       agg_idf_dict):
    """generates the cosine similarity for the current doc_part using
    the provided idf values

    Parameters
    ----------
    doc_id : pandas df
        doc_id dataframe for current doc_part
    counts : pandas df
        counts df for current doc_part
    merge_columns_l : list
        list of columns which we merge to get cosine sim
    base_idf : pandas Series
        map from term_id to idf for base counts
    agg_idf_dict : dict-like
        dictionary mapping each merge_columns set to a idf Series
        (which in turn maps each term_id to an idf for the corresponding
        merge_columns set)

    Returns
    -------
    updated doc_id
    """

    def sim(x):

        den = np.linalg.norm(x["base_tfidf"]) * np.linalg.norm(x["agg_tfidf"])
        return x["base_tfidf"].dot(x["agg_tfidf"]) / den

    for merge_columns in merge_columns_l:
        merge_label = "_".join(merge_columns)
        agg_idf = agg_idf_dict[merge_label]
        id_merge = doc_id[merge_columns + ["doc_id"]]
        t_counts = counts.merge(id_merge, on="doc_id")
        t_counts["agg_idf"] = t_counts["term_id"].map(agg_idf)
        t_counts["base_idf"] = t_counts["term_id"].map(base_idf)
        group = t_counts.groupby("doc_id")
        t_counts["base_dc"] = group["count"].transform("count")
        group = t_counts.groupby(merge_columns)
        t_counts["agg_dc"] = group["count"].transform("count")
        group = t_counts.groupby(merge_columns + ["term_id"])
        t_counts["agg_count"] = group["count"].transform(sum)
        t_counts["base_tfidf"] = ((t_counts["count"] / t_counts["base_dc"])
                                  * t_counts["base_idf"])
        t_counts["agg_tfidf"] = ((t_counts["agg_count"] / t_counts["agg_dc"])
                                 * t_counts["agg_idf"])
        t_counts = t_counts.groupby(merge_columns).apply(sim)
        t_counts = t_counts.reset_index()
        t_counts = t_counts.rename(columns={0: "%s_sim" % merge_label})
        t_counts = t_counts[merge_columns + ["%s_sim" % merge_label]]
        doc_id = doc_id.merge(t_counts, on=merge_columns)

    doc_id = doc_id.sort_values("doc_id")

    return doc_id


def doc_count_updater(doc_id, counts, merge_columns_l):
    """takes the current doc_id and counts dfs and updates the doc_id with
    term counts for different agg lvls.  The lvls are:

    1. permno-day
    2. industry-day
    3. total-day

    Parameters
    ----------
    doc_id : pandas df
        doc_id dataframe for current doc_part
    counts : pandas df
        counts df for current doc_part
    merge_columns_l : list
        list of columns which we merge to get counts

    Returns
    -------
    updated doc_id df
    """

    for merge_columns in merge_columns_l:
        id_merge = doc_id[merge_columns + ["doc_id"]]
        t_counts = counts.merge(id_merge, on="doc_id")
        t_counts = t_counts.groupby(merge_columns)["count"].sum()
        t_counts = t_counts.reset_index()
        t_counts = t_counts.rename(columns={"count": "%s_count" %
                                                     "_".join(merge_columns)})
        doc_id = doc_id.merge(t_counts, on=merge_columns)

    doc_id = doc_id.sort_values("doc_id")

    return doc_id


def updater(data_dir, part, f_pattern, load_id_method, update_methods,
            counts=None, id_kwds=None, update_methods_kwds=None):
    """updates <doc|term>_id entries based on update methods

    Parameters
    ----------
    data_dir : str
        location where data files are stored
    part : str
        label for <doc|term> partition
    f_pattern : str
        file pattern for resulting id file, if this is applied to
        <doc|term>_id should be of form <update_label>_<doc|term>_id_%s.csv
    load_id_method : function
        method for loading data frame containing <doc|term> metadata
    update_methods : iterable
        list of methods to apply to <doc|term>_id
    counts : pandas DataFrame or None
        if coutns are needed for the <doc|term>_id update they are passed in
    id_kwds : dict-like or None
        key-words passed to load_id_method
    update_methods_kwds : iterable or None
        list of kwds to pass to update methods

    Returns
    -------
    None

    Writes
    ------
    upodated <doc|term>_id
    """

    # initialize kwds
    if id_kwds is None:
        id_kwds = {}
    if update_methods_kwds is None:
        update_methods_kwds = [{} for method in update_methods]

    # add counts to kwds if they exists
    if counts:
        n_update_methods_kwds = []
        for m in update_methods_kwds:
            m["counts"] = counts
            n_update_methods_kwds.append(m)
        update_methods_kwds = n_update_methods_kwds

    # load doc_id
    data_id = load_id_method(data_dir, part, **id_kwds)

    # apply updates
    for method, method_kwds in zip(update_methods, update_methods_kwds):
        data_id = method(data_id, **method_kwds)

    # write updated doc_id
    data_id.to_csv(os.path.join(data_dir, f_pattern % part), index=False)


def update_wrapper(data_dir, processes, doc_partitions,
                   term_partitions, load_doc_id_method=None,
                   load_term_id_method=None, load_count_method=None,
                   doc_update_methods=None, term_update_methods=None,
                   doc_id_kwds=None, term_id_kwds=None, count_kwds=None,
                   doc_update_kwds=None, term_update_kwds=None):
    """handles the multiprocessing for the doc_updates

    Parameters
    ----------
    data_dir : str
        location where data files are stored
    processes : int
        number of processes for multiprocessing pool
    <doc|term>_partitions : iterable
        list of partitions which <doc|term>_ids are split over
        (e.g. <dates|ngrams>), these should correspond to the elements in
        <doc|term>_id files (e.g. <doc|term>_<part_label>.csv)
    load_<doc|term>_id_method : function or None
        method for loading <doc|term>_ids, takes data_dir, and
        <doc|term>_part
    load_count_method : function or None
        method for loading counts, should return a list of lists
        where the inner list is over term partitions and the outer
        over doc_partitions
    <X>_kwds : dict-like or None
        key-words to pass to <X> related method

    Returns
    -------
    None

    Writes
    ------
    ids updated to include any additional variables
    """

    # init empty kwds
    if doc_update_kwds is None:
        doc_update_kwds = [{} for method in doc_update_methods]
    if term_update_kwds is None:
        term_update_kwds = [{} for method in term_update_methods]

    if load_counts_method:
        if counts_kwds is None:
            counts_kwds = {}
        counts = load_counts_method(data_dir, **counts_kwds)

    pool = multiprocessing.Pool(processes)

    pool.starmap(doc_update_wrapper, [(data_dir, d, term_partitions,
                                       update_methods, update_methods_kwds)
                                      for d in doc_partitions])
