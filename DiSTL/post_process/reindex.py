"""
This code is a general set of methods for reindexing a DTM pulled from the
database to include metadata and performs any necessary reindexing of
terms and docs in the process to unify into one clean matrix of counts
"""
import pandas as pd
import multiprocessing
import os


def load_term_id(source_data_dir, out_data_dir, term_partitions):
    """loads the term ids, writes the updated term_ids and returns the offset
    dict

    Parameters
    ----------
    source_data_dir : str
        location of source data files
    out_data_dir : str
        location where results are stored
    term_partitions : None or list
        list of partitions for terms (e.g. ngrams)
        these should correspond to elements in term_id files
        (e.g. term_id_<agg_part_label>_<term_part_label>.csv)

    Returns
    -------
    term_id_offset dictionary

    Writes
    ------
    new term ids
    """

    term_id_offset = 0
    term_id_offset_dict = {}
    for term_part in term_partitions:
        term_id = pd.read_csv(os.path.join(source_data_dir,
                                           "term_id_%s.csv" % term_part),
                              names=["term", "term_id"])
        term_id["term_id"] += term_id_offset
        term_id_offset_dict[term_part] = term_id_offset
        term_id.to_csv(os.path.join(out_data_dir,
                                    "term_id_%s.csv" % term_part),
                       index=False)
        term_id_offset += len(term_id)

    return term_id_offset_dict


def counter(source_data_dir, agg_count_dir, doc_part, load_doc_id_method,
            doc_id_method_kwds, metadata_df=None,
            metadata_merge_columns=None):
    """counts the number of documents for each doc_id file, this is done so
    that we can reindex the counts in parallel

    Parameters
    ----------
    source_data_dir : str
        location of input files
    agg_count_dir : str
        location where temporary aggregate count files will be stored
    doc_part : str
        current document partition label
    load_doc_id_method : function
        method used for loading doc id
    doc_id_method_kwds : dict-like
        key-words to pass to load_doc_id_method
    metadata_df : None or pandas dataframe
        dataframe containing metadata
    metadata_merge_columns : list or None
        if list this contains the columns which the doc id is merged to
        the metadata on

    Returns
    -------
    None

    Writes
    ------
    temporary agg counts
    """

    doc_id = load_doc_id_method(source_data_dir, doc_part,
                                **doc_id_method_kwds)
    if metadata_merge_columns:
        doc_id = doc_id.merge(metadata_df, on=metadata_merge_columns)

    count = len(doc_id)
    with open(os.path.join(agg_count_dir, "counts.csv"), "a") as fd:
        fd.write("%s,%d\n" % (doc_part, count))


def reindexer(source_data_dir, agg_count_dir, out_data_dir,
              load_doc_id_method, doc_id_method_kwds,
              doc_part, term_partitions, term_id_offset_dict,
              term_agg_partitions=None, metadata_df=None,
              metadata_merge_columns=None, sort_columns=None):
    """reindexes the counts, this updates the term_id and doc_id and writes
    the updated doc_ids and counts

    Parameters
    ----------
    source_data_dir : str
        location of input files
    agg_count_dir : str
        location where temporary aggregate count files are stored
    out_data_dir : str
        location where results are stored
    load_doc_id_method : function
        method used for loading doc id
    doc_id_method_kwds : dict-like
        key-words to pass to load_doc_id_method
    doc_part : str
        label for current doc id
    term_partitions : None or list
        list of partitions for terms (e.g. ngrams)
        these should correspond to elements in term_id files
        (e.g. term_id_<agg_part_label>_<term_part_label>.csv)
    term_id_offset_dict : dict-like
        dictionary mapping term partition to corresponding offset
    term_agg_partitions : None or list
        if None this contains a list of additional partitions
        who's counts should be aggreagated during the reindexing
        process (e.g. headlines and body) the count files should be
        of the form:
        count_<agg_part_label>_<term_part_label>_<doc_part_label>.csv
    metadata_df : None or pandas dataframe
        dataframe containing metadata
    metadata_merge_columns : list or None
        if list this contains the columns which the doc id is merged to
        the metadata on
    sort_columns : list or None
        list of columns which we'd like to sort the resulting doc_ids on

    Returns
    -------
    None

    Writes
    ------
    - new doc ids
    - new counts
    """

    # TODO currently this is a hacky way to work for dates but will
    # potentially fail is the partition sorting doesn't make sense
    # get doc_id offset
    agg_counts = pd.read_csv(os.path.join(agg_count_dir, "counts.csv"),
                             names=["partition", "count"])
    agg_counts = agg_counts.sort_values("partition")
    agg_counts = agg_counts[agg_counts["partition"] < doc_part]
    doc_id_offset = agg_counts["count"].sum()

    # load doc id and merge with metadata
    doc_id = load_doc_id_method(source_data_dir, doc_part,
                                **doc_id_method_kwds)
    if metadata_merge_columns:
        doc_id = doc_id.merge(metadata_df, on=metadata_merge_columns)

    if sort_columns:
        doc_id = doc_id.sort_values(sort_columns)

    # generate new doc_id
    doc_id_map = doc_id[["doc_id"]]
    doc_id_map = doc_id_map.reset_index(drop=True)
    doc_id_map["new_doc_id"] = doc_id_map.index + doc_id_offset
    doc_id_map.index = doc_id_map["doc_id"]
    doc_id_map = doc_id_map["new_doc_id"]
    doc_id["doc_id"] = doc_id["doc_id"].map(doc_id_map)
    doc_id.to_csv(os.path.join(out_data_dir, "doc_id_%s.csv" % doc_part),
                  index=False)

    # apply new doc_id to counts
    for term_part in term_partitions:

        term_id_offset = term_id_offset_dict[term_part]
        if term_agg_partitions:
            count = pd.DataFrame([], columns=["doc_id", "term_id", "count"])
            count = count.set_index(["doc_id", "term_id"])
            count = count["count"]
            for agg_part in term_agg_partitions:
                tmp_f = os.path.join(source_data_dir,
                                     "count_%s_%s_%s.csv" % (agg_part,
                                                             term_part,
                                                             doc_part))
                tmp_count = pd.read_csv(tmp_f, names=["doc_id", "term_id",
                                                      "count"])
                tmp_count = tmp_count.set_index(["doc_id", "term_id"])
                tmp_count = tmp_count["count"]
                count = count.add(tmp_count, fill_value=0)
            count = count.reset_index()

        else:
            count_f = os.path.join(source_data_dir,
                                   "count_%s_%s.csv" % (term_part, doc_part))
            count = pd.read_csv(count_f, names=["doc_id", "term_id", "count"])

        # update term_id and doc_id based on term_id and doc_id partitions
        count["term_id"] += term_id_offset
        count["doc_id"] = count["doc_id"].map(doc_id_map)
        count = count[~pd.isnull(count["doc_id"])]
        count["doc_id"] = count["doc_id"].astype(int)
        count["term_id"] = count["term_id"].astype(int)
        count["count"] = count["count"].astype(int)
        count.to_csv(os.path.join(out_data_dir,
                                  "count_%s_%s.csv" % (term_part, doc_part)),
                     index=False)


def reindex_wrapper(source_data_dir, agg_count_dir, out_data_dir, processes,
                    load_doc_id_method, doc_partitions,
                    term_partitions, term_agg_partitions=None,
                    doc_id_method_kwds=None, metadata_f=None,
                    load_metadata_method=None,
                    metadata_method_kwds=None,
                    metadata_merge_columns=None,
                    sort_columns=None):
    """runs the reindexing code

    Parameters
    ----------
    source_data_dir : str
        location of input files
    agg_count_dir : str
        location where temporary aggregate count files will be stored
    out_data_dir : str
        location where results are stored
    processes : int
        number of processes for multiprocessing
    load_doc_id_method : function
        method for generating doc ids
    doc_partitions : list
        list of partitions which doc_ids are split over (e.g. dates)
        these should correspond to elements in doc_id files
        (e.g. doc_id_<doc_part_label>.csv)
    term_partitions : list
        list of partitions for terms (e.g. ngrams)
        these should correspond to elements in term_id files
        (e.g. term_id_<agg_part_label>_<term_part_label>.csv)
    term_agg_partitions : None or list
        if None this contains a list of additional partitions
        who's counts should be aggreagated during the reindexing
        process (e.g. headlines and body) the count files should be
        of the form:
        count_<agg_part_label>_<term_part_label>_<doc_part_label>.csv
    doc_id_method_kwds : dict-like or None
        key-words to pass to load_doc_id_method
    metadata_f : None or str
        location of metadata file if exists
    load_metadata_method : function or None
        method which takes the metadata_f and doc_partitions
        list and returns a partitioned dataframe split along
        the doc_partitions corresponding to the metadata
    metadata_method_kwds : dict-like or None
        key-words to pass to load_metadata_method
    metadata_merge_columns : None or list
        list of columns to merge metadata to doc_id
    sort_columns : list or None
        list of columns which we'd like to sort the resulting doc_ids on

    Returns
    -------
    None

    Writes
    ------
    - new term ids
    - new doc ids (including metadata)
    - new counts
    """

    # initialize multiprocessing pool
    pool = multiprocessing.Pool(processes)

    # initialize kwds
    if doc_id_method_kwds is None:
        doc_id_method_kwds = {}
    if metadata_method_kwds is None:
        metadata_method_kwds = {}

    # load permno ticker map
    if metadata_f:
        metadata_df_partitions = load_metadata_method(metadata_f,
                                                      doc_partitions,
                                                      **metadata_method_kwds)
    else:
        metadata_df_partitions = [None for part in doc_partitions]

    # handle term ids/indices
    term_id_offset_dict = load_term_id(source_data_dir, out_data_dir,
                                       term_partitions)

    # generate agg counts
    count_f = os.path.join(agg_count_dir, "counts.csv")
    if os.path.exists(count_f):
        os.remove(count_f)
    pool.starmap(counter, [(source_data_dir, agg_count_dir, part,
                            load_doc_id_method, doc_id_method_kwds,
                            metadata_df_partitions[i],
                            metadata_merge_columns)
                            for i, part in enumerate(doc_partitions)])

    # generate reindexed docs and counts
    pool.starmap(reindexer, [(source_data_dir, agg_count_dir, out_data_dir,
                              load_doc_id_method, doc_id_method_kwds,
                              part, term_partitions, term_id_offset_dict,
                              term_agg_partitions, metadata_df_partitions[i],
                              metadata_merge_columns, sort_columns)
                             for i, part in enumerate(doc_partitions)])
